import json
import os
from datetime import datetime, timezone
from urllib.request import urlopen, quote

import boto3

s3 = boto3.client("s3")

BUCKET = os.environ["BUCKET_NAME"]
SPORT = os.environ.get("SPORT", "football")
LEAGUE = os.environ.get("LEAGUE", "nfl")
DATE_RANGE = os.environ.get("DATE_RANGE", "").strip()
MAX_GAMES = int(os.environ.get("MAX_GAMES", "20"))

SCOREBOARD_BASE = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT}/{LEAGUE}/scoreboard"
SUMMARY_URL = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT}/{LEAGUE}/summary?event={{event_id}}"


def get_json(url: str):
    with urlopen(url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def put_json(bucket: str, key: str, payload: dict):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json"
    )


def build_scoreboard_url():
    if DATE_RANGE:
        return f"{SCOREBOARD_BASE}?dates={quote(DATE_RANGE)}&limit=200"
    return f"{SCOREBOARD_BASE}?limit=200"


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    scoreboard_url = build_scoreboard_url()
    scoreboard = get_json(scoreboard_url)

    put_json(
        BUCKET,
        f"raw/games/scoreboard_{ts}.json",
        {
            "requested_date_range": DATE_RANGE,
            "max_games_requested": MAX_GAMES,
            "scoreboard_response": scoreboard
        }
    )

    events = scoreboard.get("events", [])[:MAX_GAMES]
    saved_games = []

    for game in events:
        event_id = game.get("id")
        if not event_id:
            continue

        summary = get_json(SUMMARY_URL.format(event_id=event_id))

        put_json(
            BUCKET,
            f"raw/plays/game_{event_id}_{ts}.json",
            summary
        )

        saved_games.append(event_id)

    return {
        "statusCode": 200,
        "message": "Ingestion complete",
        "date_range_used": DATE_RANGE,
        "games_found": len(scoreboard.get("events", [])),
        "games_saved": len(saved_games),
        "game_ids": saved_games
    }
