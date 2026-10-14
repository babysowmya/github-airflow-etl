# GitHub Archive ETL Pipeline (Apache Airflow + Pandas + PostgreSQL)

A batch ETL pipeline that downloads one day of public GitHub events from [GH Archive](https://www.gharchive.org), keeps only `PushEvent` records, aggregates them into **weekly per-repository metrics**, and loads the result into PostgreSQL. Everything runs locally with Docker Compose.

## Architecture

```
gharchive.org (24 hourly .json.gz files)
        |
        v
[ extract_push_events ]  -> data/raw/pushevents_<ds>.jsonl
        |
        v
[ transform_events ]     -> data/transformed/metrics_<ds>.csv
        |
        v
[ load_to_postgres ]     -> PostgreSQL table: github_weekly_metrics
```

| Component | Details |
|---|---|
| Orchestrator | Apache Airflow 2.9.3 (`LocalExecutor`) |
| Warehouse | PostgreSQL 15 |
| Processing | Pandas |
| Environment | Docker Compose |

## Project structure

```
.
├── docker-compose.yml        # Airflow + PostgreSQL services
├── requirements.txt          # Python dependencies
├── dags/
│   └── github_events_dag.py  # DAG: github_archive_pipeline
└── data/
    ├── raw/                  # Extract output (JSONL)
    └── transformed/          # Transform output (CSV)
```

## DAG details

| Property | Value |
|---|---|
| `dag_id` | `github_archive_pipeline` |
| `schedule_interval` | `@daily` |
| `start_date` | `2023-01-01` |
| `catchup` | `False` |
| Retries | 3, with a 5 minute delay |
| Task order | `extract_push_events >> transform_events >> load_to_postgres` |

### 1. Extract (`extract_push_events`)
- Downloads the 24 hourly files for the execution date (`ds`), e.g. `https://data.gharchive.org/2023-01-15-0.json.gz`.
- Streams and decompresses each file, so memory use stays low.
- Writes only events where `type == "PushEvent"` to `data/raw/pushevents_<ds>.jsonl`.
- Writes to a `.tmp` file first and renames it when complete, so a failed run never leaves a partial file.

### 2. Transform (`transform_events`)
- Reads the raw JSONL into a Pandas DataFrame.
- Parses `created_at` and derives the **Monday of the week** as `week_start_date`.
- Groups by `repo.name` and `week_start_date` and computes:
  - `commit_count`: total commits in `payload.commits`
  - `avg_commit_message_length`: average character length of commit messages
  - `unique_contributors`: distinct `actor.login` values
- Writes `data/transformed/metrics_<ds>.csv` with the header:
  `repo_name,week_start_date,commit_count,avg_commit_message_length,unique_contributors`

### 3. Load (`load_to_postgres`)
- Creates `github_weekly_metrics` if it does not exist.
- **Idempotent:** in a single transaction it deletes existing rows for the week(s) in the CSV, then inserts the new rows. Re-running a date replaces the data and never duplicates it.
- Connects through the Airflow connection `warehouse_postgres` (created from an environment variable in `docker-compose.yml`), so no credentials are hardcoded in the DAG.

### Target table

| Column | Type | Constraints |
|---|---|---|
| `repo_name` | VARCHAR(255) | PRIMARY KEY |
| `week_start_date` | DATE | PRIMARY KEY |
| `commit_count` | INTEGER | NOT NULL |
| `avg_commit_message_length` | FLOAT | NOT NULL |
| `unique_contributors` | INTEGER | NOT NULL |
| `updated_at` | TIMESTAMP | DEFAULT NOW() |

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) (running)
- Ports **8080** (Airflow UI) and **5432** (PostgreSQL) free
- About 2 GB of free disk space for a full-day run and a stable internet connection

## How to run

1. **Start the stack** from the project root:
   ```bash
   docker compose up
   ```
   Wait 1 to 2 minutes until the webserver is up.

2. **Open the Airflow UI:** <http://localhost:8080>
   Login: `admin` / `admin`

3. **Trigger a run** for a past date, either in the UI (press the play button on `github_archive_pipeline`) or from the command line:
   ```bash
   docker compose exec airflow airflow dags trigger github_archive_pipeline -e 2023-01-15
   ```

4. **Watch the Graph view** until all three tasks turn dark green. A full day (24 files) can take 30 to 60+ minutes depending on your connection.

5. **Stop the stack:**
   ```bash
   docker compose down
   ```
   Add `-v` only if you want to delete the database as well.

### Faster testing

The number of hourly files downloaded is controlled by the `GHA_HOURS` environment variable in `docker-compose.yml` (default `"24"`). Set it to `"2"` for a quick test run, then set it back to `"24"` for a full-day run.

## Verifying the results

**Raw and transformed files** (Windows: use `dir` and `type`; Linux/macOS: `ls` and `head`):
```bash
dir data\raw
dir data\transformed
type data\transformed\metrics_2023-01-15.csv
```

**Table schema:**
```bash
docker compose exec postgres psql -U airflow -d airflow -c "\d github_weekly_metrics"
```

**Loaded data:**
```bash
docker compose exec postgres psql -U airflow -d airflow -c "SELECT week_start_date, COUNT(*), MAX(updated_at) FROM github_weekly_metrics GROUP BY 1 ORDER BY 1;"
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM github_weekly_metrics LIMIT 5;"
```

## Testing idempotency

1. Run the DAG for a date (e.g. `2023-01-15`) and note the row count from the query above.
2. Clear and re-run the load task:
   ```bash
   docker compose exec airflow airflow tasks clear github_archive_pipeline -t load_to_postgres -s 2023-01-15 -e 2023-01-15 -y
   ```
3. Query again. The row count must be identical, and `MAX(updated_at)` will have moved forward, which shows the load actually re-ran without creating duplicates.

## Notes and design decisions

- **One day per run, weekly aggregation:** each run only sees one day of data, but metrics are grouped by week. Loading a date replaces all rows for that week, as the project specification requires. Running a second day in the same week therefore replaces the first day's numbers for that week.
- **Recent dates:** GH Archive PushEvents for very recent dates may not include commit lists, which gives `commit_count = 0`. Use dates from 2023 for meaningful results.
- **Early filtering:** only `PushEvent` rows are kept at the extract stage to reduce disk and memory use downstream.

## Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` on 8080 | Stop whatever uses 8080 (`docker ps`), or change the left side of the port mapping in `docker-compose.yml`, e.g. `"8081:8080"` |
| Webserver fails with `Already running on PID` | `docker compose down`, then `docker compose up` (always stop with `down`, not by closing the window) |
| DAG not showing up | `docker compose exec airflow airflow dags list-import-errors` |
| Permission denied writing to `data/` (Linux) | `chmod -R 777 data` |
| Docker image pull times out | Retry the pull, check proxy/VPN/DNS settings, or switch networks |
| Need a completely fresh start | `docker compose down -v` (also deletes the database) |

## Tech stack

Apache Airflow 2.9.3 · Python 3.11 · Pandas · PostgreSQL 15 · Docker Compose

## Author
Sunkara Sowmya