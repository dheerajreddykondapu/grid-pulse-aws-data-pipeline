import os
import time
import boto3

athena = boto3.client("athena")

DATABASE = os.environ["ATHENA_DATABASE"]
OUTPUT = os.environ["ATHENA_OUTPUT"]

QUERIES = [
    "MSCK REPAIR TABLE gridiron_insights_db.games_curated;",
    "MSCK REPAIR TABLE gridiron_insights_db.plays_curated;"
]


def run_query(query: str):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )
    execution_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in {"FAILED", "CANCELLED"}:
            raise Exception(f"Athena query failed: {query} | state={state}")
        time.sleep(5)


def lambda_handler(event, context):
    for query in QUERIES:
        run_query(query)

    return {
        "statusCode": 200,
        "message": "Athena repair complete"
    }
