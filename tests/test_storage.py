import json
import pandas as pd

from upst import storage


def test_write_snapshot_creates_partition(tmp_path):
    # Prepare sample jobs (duplicate id within same snapshot) to verify deduping.
    jobs = [
        {"id": 1, "title": "Engineer", "snapshot_date": "2099-01-01"},
        {"id": 1, "title": "Engineer", "snapshot_date": "2099-01-01"},  # duplicate
        {"id": 2, "title": "Analyst", "snapshot_date": "2099-01-01"},
    ]

    out_path = storage.write_snapshot(jobs, out_dir=tmp_path)

    assert out_path.exists()
    assert out_path.suffix == ".parquet" or out_path.suffix == ".json"

    if out_path.suffix == ".parquet":
        df = pd.read_parquet(out_path)
        # Duplicates should be dropped -> only 2 rows.
        assert len(df) == 2
        assert {"job_id", "snapshot_date"}.issubset(df.columns)
    else:
        # Fallback JSON path
        loaded = json.loads(out_path.read_text())
        assert len(loaded) == 2
        assert all("job_id" in rec for rec in loaded)


def test_load_raw_aggregates_and_dedupes(tmp_path):
    # Create two separate snapshot partitions each with overlapping job_id.
    day1 = tmp_path / "snapshot_date=2099-01-01"
    day1.mkdir(parents=True)
    pd.DataFrame([
        {"job_id": 1, "snapshot_date": "2099-01-01"},
        {"job_id": 2, "snapshot_date": "2099-01-01"},
    ]).to_parquet(day1 / "part-000.parquet", index=False)

    day2 = tmp_path / "snapshot_date=2099-01-02"
    day2.mkdir(parents=True)
    pd.DataFrame([
        {"job_id": 2, "snapshot_date": "2099-01-02"},  # same id but different date => keep
        {"job_id": 3, "snapshot_date": "2099-01-02"},
    ]).to_parquet(day2 / "part-000.parquet", index=False)

    df = storage.load_raw(base_dir=tmp_path)

    # Expect 4 unique rows (dedupe within same date only)
    assert len(df) == 4
    # Ensure columns exist
    assert {"job_id", "snapshot_date"}.issubset(df.columns) 