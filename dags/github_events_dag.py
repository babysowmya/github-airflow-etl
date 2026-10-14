import gzip
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

DATA_DIR = "/opt/airflow/data"
RAW_DIR = f"{DATA_DIR}/raw"
TRANSFORMED_DIR = f"{DATA_DIR}/transformed"
HOURS = int(os.getenv("GHA_HOURS", "24"))  # 24 = full day

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS github_weekly_metrics (
    repo_name VARCHAR(255),
    week_start_date DATE,
    commit_count INTEGER NOT NULL,
    avg_commit_message_length FLOAT NOT NULL,
    unique_contributors INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (repo_name, week_start_date)
);
"""

COLUMNS = ["repo_name", "week_start_date", "commit_count",
           "avg_commit_message_length", "unique_contributors"]


# ---------- 1. EXTRACT ----------
def extract_push_events(ds, **_):
    os.makedirs(RAW_DIR, exist_ok=True)
    final_path = f"{RAW_DIR}/pushevents_{ds}.jsonl"
    tmp_path = final_path + ".tmp"

    with open(tmp_path, "w") as out:
        for hour in range(HOURS):
            url = f"https://data.gharchive.org/{ds}-{hour}.json.gz"
            with requests.get(url, stream=True, timeout=120) as resp:
                resp.raise_for_status()
                with gzip.GzipFile(fileobj=resp.raw) as gz:
                    for line in gz:               # streams, so low memory
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if event.get("type") == "PushEvent":
                            out.write(json.dumps(event) + "\n")

    os.replace(tmp_path, final_path)  # file appears only when complete
    print(f"Wrote {final_path}")


# ---------- 2. TRANSFORM ----------
def transform_events(ds, **_):
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    raw_path = f"{RAW_DIR}/pushevents_{ds}.jsonl"
    out_path = f"{TRANSFORMED_DIR}/metrics_{ds}.csv"

    rows = []
    with open(raw_path) as f:
        for line in f:
            e = json.loads(line)
            commits = (e.get("payload") or {}).get("commits") or []
            rows.append((
                (e.get("repo") or {}).get("name"),
                e.get("created_at"),
                (e.get("actor") or {}).get("login"),
                len(commits),
                sum(len(c.get("message") or "") for c in commits),
            ))

    df = pd.DataFrame(rows, columns=["repo_name", "created_at", "actor",
                                     "commit_count", "msg_len"])
    df = df.dropna(subset=["repo_name", "created_at"])

    if df.empty:
        pd.DataFrame(columns=COLUMNS).to_csv(out_path, index=False)
        return

    ts = pd.to_datetime(df["created_at"], utc=True)
    monday = ts.dt.normalize() - pd.to_timedelta(ts.dt.weekday, unit="D")
    df["week_start_date"] = monday.dt.strftime("%Y-%m-%d")

    agg = (df.groupby(["repo_name", "week_start_date"])
             .agg(commit_count=("commit_count", "sum"),
                  total_len=("msg_len", "sum"),
                  unique_contributors=("actor", "nunique"))
             .reset_index())
    agg["avg_commit_message_length"] = (
        agg["total_len"] / agg["commit_count"].where(agg["commit_count"] > 0)
    ).fillna(0.0)

    agg[COLUMNS].to_csv(out_path, index=False)
    print(f"Wrote {len(agg)} rows to {out_path}")


# ---------- 3. LOAD ----------
def load_to_postgres(ds, **_):
    df = pd.read_csv(f"{TRANSFORMED_DIR}/metrics_{ds}.csv")
    rows = [(r.repo_name, r.week_start_date, int(r.commit_count),
             float(r.avg_commit_message_length), int(r.unique_contributors))
            for r in df.itertuples(index=False)]
    weeks = sorted(df["week_start_date"].unique().tolist())

    conn = PostgresHook(postgres_conn_id="warehouse_postgres").get_conn()
    try:
        with conn:  # one transaction: commit on success, rollback on error
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                if weeks:
                    cur.execute(
                        "DELETE FROM github_weekly_metrics "
                        "WHERE week_start_date = ANY(%s::date[])", (weeks,))
                if rows:
                    execute_values(
                        cur,
                        "INSERT INTO github_weekly_metrics "
                        "(repo_name, week_start_date, commit_count, "
                        "avg_commit_message_length, unique_contributors) "
                        "VALUES %s", rows, page_size=5000)
    finally:
        conn.close()


# ---------- DAG ----------
default_args = {"owner": "airflow", "retries": 3,
                "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="github_archive_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    extract_push_events_task = PythonOperator(
        task_id="extract_push_events", python_callable=extract_push_events)
    transform_events_task = PythonOperator(
        task_id="transform_events", python_callable=transform_events)
    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres", python_callable=load_to_postgres)

    extract_push_events_task >> transform_events_task >> load_to_postgres_task