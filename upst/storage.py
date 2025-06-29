from __future__ import annotations

"""Utilities for managing raw snapshot storage.

This module formalises the *Storage Layout* described in
`Implementation_framework.md` (section 2).  It is intentionally very small –
just enough functionality to

1. persist a list of job-posting dictionaries returned by ``upst.scraper.scrape``
   to the canonical location under ``data/raw/``; and
2. efficiently load *all* raw snapshots into a single ``pandas.DataFrame`` while
   enforcing the primary-key constraint (``job_id``, ``snapshot_date``).

Keeping this logic in a dedicated module avoids polluting the scraper with
file-system concerns and provides a clean public API for the forthcoming
analysis layer.
"""

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, List
import json

import pandas as pd

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "RAW_ROOT",
    "CLEAN_ROOT",
    "write_snapshot",
    "load_raw",
    "load_clean",
]

# Project-relative root where raw data live.  During unit tests callers can
# pass a different directory to each function so the default should *not*
# be resolved eagerly (so that tmp_path fixtures work nicely).
RAW_ROOT = Path("data") / "raw"
CLEAN_ROOT = Path("data") / "clean"


def write_snapshot(
    jobs: List[dict[str, Any]],
    *,
    out_dir: str | Path = RAW_ROOT,
) -> Path:
    """Validate *jobs* and write them out as a Parquet file.

    The function materialises the raw payload exactly as scraped **but**:

    1. It renames the ``id`` column to ``job_id`` because ``id`` is ambiguous
       in downstream analytics and clashes with some SQL engines' reserved
       keywords.
    2. It removes duplicate rows based on the primary-key columns
       ``(job_id, snapshot_date)``.

    The resulting file is placed under ``{out_dir}/snapshot_date=<YYYY-MM-DD>/``
    with a monotonically increasing *part-NNN.parquet* name so that repeated
    scrapes on the same day append cleanly.
    """

    if not jobs:
        raise ValueError("'jobs' list is empty – nothing to write.")

    # ---------------------------------------------------------------------
    # Build *snapshot_date* and partition directory.
    # ---------------------------------------------------------------------
    snapshot_date: str | None = jobs[0].get("snapshot_date")
    if snapshot_date is None:
        # Fall back to today – this should never happen because the scraper
        # injects the field, but defensive coding never hurts.
        snapshot_date = datetime.now(timezone.utc).date().isoformat()

    partition_dir = Path(out_dir) / f"snapshot_date={snapshot_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------------
    # Determine part filename (monotonically increasing integer, zero-padded).
    # ---------------------------------------------------------------------
    existing_parts = sorted(partition_dir.glob("part-*.parquet"))
    if existing_parts:
        # Extract the integer, assume naming scheme part-006.parquet etc.
        last_idx = max(int(p.stem.split("-")[-1]) for p in existing_parts)
        next_idx = last_idx + 1
    else:
        next_idx = 0

    out_path = partition_dir / f"part-{next_idx:03d}.parquet"

    # ---------------------------------------------------------------------
    # Create DataFrame and enforce minimal schema guarantees.
    # ---------------------------------------------------------------------
    df = pd.DataFrame(jobs)

    # Determine canonical primary-key column *job_id*.
    # Greenhouse exposes multiple identifiers – we treat ``requisition_id``
    # as the stable public ID.  Fallbacks retain compatibility with older
    # scrapes that used ``id``.

    if "job_id" not in df.columns:
        if "requisition_id" in df.columns:
            df = df.rename(columns={"requisition_id": "job_id"})
        elif "id" in df.columns:
            df = df.rename(columns={"id": "job_id"})

    # Drop legacy / unneeded identifier columns to keep the schema lean.
    _DROP_COLS = [
        "id",  # Greenhouse internal – replaced by requisition_id → job_id
        "internal_job_id",  # rarely useful, verbose GUID
        "metadata",  # opaque misc structure
        "data_compliance",  # only relevant to GH front-end
        "company_name",  # redundant for Upstart-specific scraper
    ]
    cols_to_drop = [c for c in _DROP_COLS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Ensure snapshot_date column exists (string / ISO date).
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = snapshot_date

    # Drop duplicates on the primary key.
    df = df.drop_duplicates(subset=["job_id", "snapshot_date"], keep="first")

    # Finally write to Parquet.  We rely on pyarrow which is already declared
    # in requirements.txt.  In very unlikely event of failure we fall back to
    # JSON so no data is lost.
    try:
        df.to_parquet(out_path, index=False)
    except Exception as exc:  # pragma: no cover – fallback rarely exercised.
        json_fallback = out_path.with_suffix(".json")
        json_fallback.write_text(json.dumps(df.to_dict(orient="records"), indent=2))
        out_path = json_fallback

    return out_path


def load_raw(*, base_dir: str | Path = RAW_ROOT) -> pd.DataFrame:
    """Load *all* raw snapshots into a single ``pandas.DataFrame``.

    The function recursively finds every *part-*.parquet* file, concatenates
    them **row-wise**, enforces the `(job_id, snapshot_date)` primary key and
    returns the de-duplicated DataFrame.  If no snapshots exist an *empty*
    DataFrame with the expected columns is returned.
    """

    root = Path(base_dir)
    if not root.exists():
        # Return an empty frame with the canonical columns so downstream code
        # does not have to special-case missing data.
        return pd.DataFrame(
            columns=[
                "job_id",
                "snapshot_date",
                "title",
                "department",
                "employment_type",
                "salary_min",
                "salary_max",
                "seniority",
            ]
        )

    # glob is safe here – the directory structure is flat beyond the partition
    # level.  Using rglob would work too but is slower.
    files = list(root.glob("snapshot_date=*/part-*.parquet"))
    if not files:
        return pd.DataFrame()

    frames = []
    for fp in files:
        try:
            df = pd.read_parquet(fp)
        except Exception as exc:
            # Log and skip corrupted file rather than failing entirely.
            print(f"[warn] Could not read {fp}: {exc}")
            continue

        # Harmonise job identifier as above when loading from disk as the
        # snapshot may pre-date the schema change.
        if "job_id" not in df.columns:
            if "requisition_id" in df.columns:
                df = df.rename(columns={"requisition_id": "job_id"})
            elif "id" in df.columns:
                df = df.rename(columns={"id": "job_id"})

        # Drop unwanted columns consistently with write_snapshot().
        drop_cols = [c for c in [
            "id",
            "internal_job_id",
            "metadata",
            "data_compliance",
            "company_name",
        ] if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    consolidated = pd.concat(frames, ignore_index=True, copy=False)

    # Deduplicate across *all* snapshots.  Keep "first" so the earliest file on disk wins.
    consolidated = consolidated.drop_duplicates(subset=["job_id", "snapshot_date"], keep="first")

    return consolidated


def load_clean(*, base_dir: str | Path = CLEAN_ROOT) -> pd.DataFrame:
    """Load *all* cleaned snapshots into a single ``pandas.DataFrame``.

    This function is similar to load_raw() but loads from the data/clean 
    directory where cleaned, flattened data is stored.

    The function recursively finds every *part-*.parquet* file, concatenates
    them **row-wise**, enforces the `(job_id, snapshot_date)` primary key and
    returns the de-duplicated DataFrame.  If no snapshots exist an *empty*
    DataFrame with the expected columns is returned.
    """

    root = Path(base_dir)
    if not root.exists():
        # Return an empty frame with the canonical columns so downstream code
        # does not have to special-case missing data.
        return pd.DataFrame(
            columns=[
                "job_id",
                "snapshot_date",
                "title",
                "department",
                "employment_type",
                "salary_min",
                "salary_max",
                "seniority",
                "location",
                "departments",
                "offices",
            ]
        )

    # glob is safe here – the directory structure is flat beyond the partition
    # level.  Using rglob would work too but is slower.
    files = list(root.glob("snapshot_date=*/part-*.parquet"))
    if not files:
        return pd.DataFrame()

    frames = []
    for fp in files:
        try:
            df = pd.read_parquet(fp)
        except Exception as exc:
            # Log and skip corrupted file rather than failing entirely.
            print(f"[warn] Could not read {fp}: {exc}")
            continue

        # Drop unwanted columns consistently with write_snapshot().
        drop_cols = [c for c in [
            "id",
            "internal_job_id",
            "metadata",
            "data_compliance",
            "company_name",
        ] if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    consolidated = pd.concat(frames, ignore_index=True, copy=False)

    # Deduplicate across *all* snapshots.  Keep "first" so the earliest file on disk wins.
    if "job_id" in consolidated.columns and "snapshot_date" in consolidated.columns:
        consolidated = consolidated.drop_duplicates(subset=["job_id", "snapshot_date"], keep="first")

    return consolidated 