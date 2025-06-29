"""Scraper module for Upstart job postings.

This module fetches job listings from Upstart's public Greenhouse API
and retrieves full detail for each job. The primary entry-point is the
`scrape()` coroutine or the synchronous `scrape_sync()` helper which
returns a list of JSON dictionaries (one per job posting).

The implementation purposefully avoids scraping HTML; it instead relies
on Greenhouse's JSON API which powers the career site. As such it is
faster, less brittle and friendly to the website.

Usage (command line):
    python -m upst.scraper  # stores raw JSON in data/raw/<timestamp>.json

Inside Python code:
    from upst.scraper import scrape_sync
    jobs = scrape_sync(concurrency=10)
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List

import aiohttp

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

GREENHOUSE_BOARD = "upstart"
LIST_ENDPOINT = (
    f"https://boards-api.greenhouse.io/v1/boards/{GREENHOUSE_BOARD}/jobs?content=false"
)
DETAIL_ENDPOINT = (
    f"https://boards-api.greenhouse.io/v1/boards/{GREENHOUSE_BOARD}/jobs/{{job_id}}?content=true"
)

DEFAULT_CONCURRENCY = 5  # ~5 requests/second keeps us well below typical limits.
MAX_RETRIES = 3
BACKOFF_INITIAL = 0.5  # Seconds – doubled after each retry
TIMEOUT = aiohttp.ClientTimeout(total=30)

# A small rotation of realistic desktop user-agent strings.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _random_user_agent() -> str:  # pragma: no cover
    """Return a random User-Agent header value."""

    return random.choice(USER_AGENTS)


async def _fetch_json(url: str, session: aiohttp.ClientSession, *, semaphore: asyncio.Semaphore) -> Any | None:
    """Fetch *url* and decode JSON, applying retries & exponential back-off.

    Returns None if the request ultimately fails.
    """

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, headers={"User-Agent": _random_user_agent()}) as resp:
                    # Respect rate-limit errors explicitly so we can back-off more.
                    if resp.status == 429:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=429, message="Rate limited"
                        )
                    resp.raise_for_status()
                    return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES:
                print(f"[error] Giving up on {url} after {MAX_RETRIES} tries: {exc}", file=sys.stderr)
                return None
            # Exponential back-off with jitter.
            backoff = BACKOFF_INITIAL * 2 ** (attempt - 1) + random.uniform(0, 0.5)
            await asyncio.sleep(backoff)


# ---------------------------------------------------------------------------
# Public scraping functions
# ---------------------------------------------------------------------------


async def _fetch_job_list(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> List[dict[str, Any]]:
    """Return list of job dicts from the list endpoint (content=false)."""

    payload = await _fetch_json(LIST_ENDPOINT, session, semaphore=semaphore)
    if not payload or "jobs" not in payload:
        return []
    return payload["jobs"]


async def _fetch_job_detail(job_id: int | str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> dict[str, Any] | None:
    """Return detailed job payload for *job_id* or None if it fails."""

    url = DETAIL_ENDPOINT.format(job_id=job_id)
    return await _fetch_json(url, session, semaphore=semaphore)


async def scrape(*, concurrency: int = DEFAULT_CONCURRENCY) -> List[dict[str, Any]]:  # noqa: D401
    """Scrape Upstart job postings.

    Parameters
    ----------
    concurrency
        Maximum concurrent network requests.

    Returns
    -------
    list of dict
        Detailed job payloads in the same shape returned by Greenhouse.
    """

    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        jobs_basic = await _fetch_job_list(session, semaphore)
        if not jobs_basic:
            return []

        # Schedule detail fetches concurrently.
        tasks = [
            _fetch_job_detail(job["id"], session, semaphore) for job in jobs_basic if "id" in job
        ]
        detailed_jobs = await asyncio.gather(*tasks)

    # Enrich each successful job payload with snapshot_date and filter out failures (None).
    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    enriched: list[dict[str, Any]] = []
    for job in detailed_jobs:
        if job is None:
            continue
        # Ensure we do not overwrite the field if Greenhouse ever adds it.
        job["snapshot_date"] = snapshot_date  # partition key used downstream
        enriched.append(job)

    return enriched


# ---------------------------------------------------------------------------
# Convenience synchronous wrapper + CLI entry-point
# ---------------------------------------------------------------------------


def scrape_sync(concurrency: int = DEFAULT_CONCURRENCY) -> List[dict[str, Any]]:
    """Blocking wrapper around `scrape()` suitable for synchronous code."""

    return asyncio.run(scrape(concurrency=concurrency))


def _store_raw(jobs: List[dict[str, Any]], *, out_dir: str | os.PathLike = "data/raw") -> Path:
    """Persist job payloads under *out_dir* partitioned by snapshot_date.

    The data are written as Parquet (preferred). If Parquet dependencies are
    unavailable, the function gracefully falls back to line-delimited JSON so
    that no data are lost."""

    # Determine partition directory using snapshot_date information. We expect every job
    # dict to carry this key thanks to `scrape()`. Fallback to today if list is empty.
    if jobs and "snapshot_date" in jobs[0]:
        snapshot_date = jobs[0]["snapshot_date"]  # yyyy-mm-dd
    else:
        snapshot_date = datetime.now(timezone.utc).date().isoformat()

    partition_dir = Path(out_dir) / f"snapshot_date={snapshot_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # File naming scheme: part-<timestamp>.parquet so multiple runs append cleanly.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = partition_dir / f"part-{timestamp}.parquet"

    # Serialize to Parquet for efficient analytics downstream.
    try:
        import pandas as pd  # Lazy import to keep top-level requirements light for users.

        df = pd.DataFrame(jobs)
        # Ensure consistent column order (optional) – rely on pandas default otherwise.
        df.to_parquet(out_path, index=False)
    except Exception as exc:  # Broad except to fall back to JSON if Parquet write fails.
        # Fallback path keeps data accessible even without pyarrow installed.
        json_fallback = out_path.with_suffix(".json")
        json_fallback.write_text(json.dumps(jobs, indent=2))
        out_path = json_fallback
        print(
            f"[warn] Could not write Parquet ({exc}). Wrote JSON to {json_fallback}",
            file=sys.stderr,
        )

    return out_path


def main(argv: list[str] | None = None) -> None:  # pragma: no cover
    """CLI entry-point: fetch & persist job postings, print summary."""

    argv = argv or sys.argv[1:]
    # Very light arg-parse (only concurrency for now)
    try:
        concurrency = int(argv[0]) if argv else DEFAULT_CONCURRENCY
    except ValueError:
        print("Usage: python -m upst.scraper [concurrency:int]", file=sys.stderr)
        sys.exit(1)

    print(f"[info] Scraping Upstart jobs with concurrency={concurrency} …")
    jobs = scrape_sync(concurrency=concurrency)
    print(f"[info] Retrieved {len(jobs)} detailed job postings.")

    # Always save to the project's data/raw directory.
    project_root = Path(__file__).parent.parent
    output_dir = project_root / "data" / "raw"
    path = _store_raw(jobs, out_dir=output_dir)
    # If the returned path is already relative, printing it directly is fine. However,
    # when the path is absolute we prefer a cleaner relative representation for UX.
    try:
        display_path = path.relative_to(Path.cwd())
    except ValueError:
        # `path` is not under the current working directory (or is already relative)
        # so just use it as-is.
        display_path = path

    print(f"[info] Raw JSON saved to {display_path}")


if __name__ == "__main__":
    main() 