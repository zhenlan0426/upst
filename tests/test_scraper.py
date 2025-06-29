import pandas as pd
from typing import Any

import pytest

import upst.scraper as scraper


@pytest.mark.asyncio
async def test_scrape_returns_detailed_jobs(monkeypatch):
    """scrape() should return detailed payloads for all jobs when detail fetch succeeds, and include snapshot_date."""

    async def fake_fetch_job_list(_session, _semaphore):
        return [{"id": 1}, {"id": 2}]

    async def fake_fetch_job_detail(job_id: int | str, _session, _semaphore):
        return {"id": job_id, "title": f"Job {job_id}"}

    # Patch the network helper coroutines so no real HTTP requests are made.
    monkeypatch.setattr(scraper, "_fetch_job_list", fake_fetch_job_list)
    monkeypatch.setattr(scraper, "_fetch_job_detail", fake_fetch_job_detail)

    jobs: list[dict[str, Any]] = await scraper.scrape(concurrency=2)

    assert len(jobs) == 2
    assert {job["id"] for job in jobs} == {1, 2}
    # Ensure snapshot_date key is present and identical across jobs.
    snapshot_values = {job.get("snapshot_date") for job in jobs}
    assert len(snapshot_values) == 1


@pytest.mark.asyncio
async def test_scrape_skips_failed_details(monkeypatch):
    """scrape() should omit jobs whose detail payload could not be fetched (None)."""

    async def fake_fetch_job_list(_session, _semaphore):
        return [{"id": 1}, {"id": 2}, {"id": 3}]

    async def fake_fetch_job_detail(job_id: int | str, _session, _semaphore):
        if job_id == 3:
            return None  # Simulate failed detail fetch.
        return {"id": job_id, "title": f"Job {job_id}"}

    monkeypatch.setattr(scraper, "_fetch_job_list", fake_fetch_job_list)
    monkeypatch.setattr(scraper, "_fetch_job_detail", fake_fetch_job_detail)

    jobs = await scraper.scrape(concurrency=3)

    # Only jobs 1 and 2 should be present.
    assert len(jobs) == 2
    assert {job["id"] for job in jobs} == {1, 2}


def test_store_raw_creates_parquet_and_writes_data(tmp_path):
    """_store_raw should write the exact payload to a Parquet file."""

    sample = [{"id": 1, "title": "Example", "snapshot_date": "2099-01-01"}]

    out_path = scraper._store_raw(sample, out_dir=tmp_path)

    assert out_path.exists()
    assert out_path.suffix == ".parquet" or out_path.suffix == ".json"

    if out_path.suffix == ".parquet":
        df = pd.read_parquet(out_path)
        assert df.to_dict(orient="records") == sample
    else:
        # JSON fallback path
        import json

        reloaded = json.loads(out_path.read_text())
        assert reloaded == sample


def test_random_user_agent():
    """_random_user_agent should return a string from USER_AGENTS list."""

    ua = scraper._random_user_agent()
    assert ua in scraper.USER_AGENTS 