from datetime import datetime, timedelta
import json
import time
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

REGION = "us-east-1"
LAMBDA_ARCHIVE = "grid-pulse-archive-current"
LAMBDA_PULL = "gridiron-pulse-nfl-ingest"
RAW_CRAWLER = "grid-pulse-crawler"
GLUE_JOB = "grid-pulse-etl"
ATHENA_DB = "gridiron_insights_db"
ATHENA_OUTPUT = "s3://grid-pulse-athena-results-va/results/"


def invoke_lambda(function_name, payload=None):
    client = boto3.client("lambda", region_name=REGION)
    response = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload or {}).encode("utf-8"),
    )
    print(response["Payload"].read().decode("utf-8"))


def archive_current_data():
    invoke_lambda(LAMBDA_ARCHIVE, {})


def pull_latest_data():
    invoke_lambda(LAMBDA_PULL, {})


def start_raw_crawler():
    client = boto3.client("glue", region_name=REGION)
    client.start_crawler(Name=RAW_CRAWLER)


def wait_for_raw_crawler():
    client = boto3.client("glue", region_name=REGION)
    while True:
        state = client.get_crawler(Name=RAW_CRAWLER)["Crawler"]["State"]
        print(f"Raw crawler state: {state}")
        if state == "READY":
            break
        time.sleep(30)


def run_glue_etl():
    client = boto3.client("glue", region_name=REGION)
    response = client.start_job_run(JobName=GLUE_JOB)
    job_run_id = response["JobRunId"]

    while True:
        run = client.get_job_run(JobName=GLUE_JOB, RunId=job_run_id)["JobRun"]
        state = run["JobRunState"]
        print(f"Glue ETL state: {state}")
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "STOPPED", "TIMEOUT"}:
            raise Exception(f"Glue job failed with state: {state}")
        time.sleep(30)


def run_athena_query(query):
    client = boto3.client("athena", region_name=REGION)
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    execution_id = response["QueryExecutionId"]

    while True:
        result = client.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        print(f"Athena query state: {state}")
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            raise Exception(f"Athena query failed: {state}")
        time.sleep(10)


def repair_games_curated():
    run_athena_query("MSCK REPAIR TABLE gridiron_insights_db.games_curated;")


def repair_plays_curated():
    run_athena_query("MSCK REPAIR TABLE gridiron_insights_db.plays_curated;")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="grid_pulse_pipeline",
    default_args=default_args,
    description="Grid Pulse AWS pipeline orchestration",
    start_date=datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
    tags=["aws", "sports", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="archive_current_data",
        python_callable=archive_current_data,
    )

    t2 = PythonOperator(
        task_id="run_ingestion_lambda",
        python_callable=pull_latest_data,
    )

    t3 = PythonOperator(
        task_id="start_raw_crawler",
        python_callable=start_raw_crawler,
    )

    t4 = PythonOperator(
        task_id="wait_for_raw_crawler",
        python_callable=wait_for_raw_crawler,
    )

    t5 = PythonOperator(
        task_id="run_glue_etl",
        python_callable=run_glue_etl,
    )

    t6 = PythonOperator(
        task_id="repair_games_curated",
        python_callable=repair_games_curated,
    )

    t7 = PythonOperator(
        task_id="repair_plays_curated",
        python_callable=repair_plays_curated,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
