# Upst Job Scraper

A small utility library for tracking open positions on Upstart's careers site. It scrapes the public Greenhouse endpoints, stores the results as Parquet snapshots and provides helper functions to load the data for analysis.

## Overview

The project was created to monitor hiring trends at [Upstart](https://careers.upstart.com/jobs/search). The high level design is documented in [Implementation_framework.md](Implementation_framework.md). In short it:

- fetches the job list and each detail page from Greenhouse's JSON API
- optionally cleans nested fields so they are easier to analyse
- writes each scrape to `data/raw/` or `data/clean/` partitioned by `snapshot_date`

## Installation

Use Python 3.11 or newer. Create a virtual environment and install the requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run the scraper from the command line:

```bash
python -m upst.scraper [concurrency]
```

This fetches the latest postings and writes a snapshot under `data/clean/` by default. You can also call the public API from Python:

```python
from upst import scrape_sync, load_clean

jobs = scrape_sync(concurrency=5)
print(len(jobs), "jobs fetched")

# Later, load all snapshots as a DataFrame
df = load_clean()
print(df.head())
```

The raw snapshot files live under `data/raw/` and cleaned versions under `data/clean/` in directories named `snapshot_date=<YYYY-MM-DD>/part-XXX.parquet`.

## Running Tests

Install the requirements above and execute:

```bash
pytest -q
```

Tests mock network access and verify that the scraper and storage helpers behave as expected.

## Further Reading

See [Implementation_framework.md](Implementation_framework.md) for the original design notes and [notes.md](notes.md) for the investor oriented metrics that motivated this project.
