# High-Level Implementation Framework for Tracking Upstart Job Postings

## Overview
This framework is designed to periodically monitor Upstart's open job positions at `https://careers.upstart.com/jobs/search` to provide insights into the company's hiring trends and strategic focus as an investor. It involves web scraping, data storage, and analysis to compute specified metrics over time.

## Components

### 1. Scraper Module
**Purpose:** Collect job posting data from Upstart's careers page.

- **Functionality:**
  - Pull every job card on https://careers.upstart.com/jobs/search and its detail page	Why two hops? The list page often omits salary, posting date, or full description. We need the detail page for those fields.
  - Handle pagination & API back-doors. Upstart's site is built on Greenhouse. List pages query a public JSON endpoint you can hit directly (faster & less brittle than HTML scraping). Fall back to BeautifulSoup if that endpoint changes.
  - Perform asynchronous fetching of detail pages using `aiohttp`/`asyncio` with a configurable concurrency limit (≈5 req/s) to minimize runtime while respecting rate-limits.
  - Rotate `User-Agent` headers and implement exponential back-off & retry logic for HTTP 429 / 5xx responses.
  - Cache detail-page responses locally during a single run (e.g., with `requests_cache`) to avoid duplicate hits and speed up re-runs.
  - Persist raw JSON payloads (e.g., to S3) for auditing and future schema changes.

- **Considerations:**
  - Use Requisition Identifier as unique identifiers to track jobs across scrapes.
  - Normalize, e.g., "Capital Markets" vs "Capital Markets & Structured Products" means the same thing.
  - Respect Upstart's `robots.txt` directives and impose polite random delays between requests.

---

### 2. Data Storage Module
**Purpose:** Store job posting data persistently to track changes over time.

- **Database Choice:** Use SQLite for simplicity or PostgreSQL for scalability.
- **Schema:**
  - **Jobs Table:** Stores static job details.
    - Columns: `job_id` (unique), `title`, `department`, `location`, `salary_range`, `description`.
  - **Scrapes Table:** Logs each scraping event.
    - Columns: `scrape_id`, `scrape_date`.
  - **Job_Scrapes Table:** Tracks job presence across scrapes.
    - Columns: `job_id`, `scrape_id`.
  - **Job_Changes Table:** Captures field-level changes across scrapes.
    - Columns: `change_id`, `job_id`, `field_name`, `old_value`, `new_value`, `changed_at`.
- **Functionality:**
  - Insert new job data with timestamps.
  - Query historical data for trend analysis.
- **Considerations:**
  - Store descriptions in the database or fetch on-demand for analysis.
  - Use timestamps to approximate posting and closing dates.
  - Add indexes on `job_id` and composite (`job_id`, `scrape_id`) columns to speed up temporal queries.

---

### 3. Analysis Module
**Purpose:** Compute metrics to provide insights into hiring trends and strategic focus.

- **Metrics and Functions:**
  1. **High-Level Hiring:**
     - `total_open_positions()`: Count current job listings.
     - `newly_opened_positions(last_check_date)`: Identify jobs present in the latest scrape but not the previous one.
     - `recently_closed_positions(last_check_date)`: Identify jobs in the previous scrape but not the latest.
     - `average_lifespan()`: Calculate the duration between first and last appearance of closed jobs, based on scrape frequency.
  2. **Strategic Focus & Departmental Allocation:**
     - `department_distribution()`: Count open positions per department.
  3. **Talent Seniority:**
     - `seniority_distribution()`: Parse job titles for keywords (e.g., "Associate," "Senior," "Manager") and categorize.
     - `salary_analysis()`:
       - Parse salary ranges (if available), compute average per position.
       - Calculate median salaries by seniority level and department.
       - Sum averages for all open positions to estimate potential salary expenditure.
  4. **Keyword Analysis:**
     - `keyword_analysis()`:
       - Predefined keywords for strategic initiatives (e.g., "AI," "Machine Learning") and technology stack (e.g., "Python," "AWS").
       - Dynamic analysis using word frequency (e.g., with NLTK or `Counter`) to identify trending terms in titles and descriptions.
  5. **Hiring Velocity & Churn:**
     - `time_to_hire_distribution()`: Estimate the distribution of days roles stay open (first vs. last appearance).
     - `monthly_openings_closed()`: Aggregate new vs. closed positions by calendar month to visualize hiring velocity.

- **Tools:**
  - Use `pandas` for data manipulation.
  - Use `NLTK` or `spaCy` for text processing (e.g., removing stop words, counting keywords).

- **Considerations:**
  - Handle missing salary data by excluding affected positions from salary metrics.
  - Approximate lifespan using scrape intervals (e.g., weekly checks).

---
