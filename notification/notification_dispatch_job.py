import argparse

import json
import uuid

from pyspark.sql import SparkSession
from databricks_openai import DatabricksOpenAI
import mlflow

spark = SparkSession.builder.getOrCreate()

CURSOR_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {cursor_table} (
        event_id STRING,
        dispatched_at TIMESTAMP
    )
"""

# Doubles as the outbox record, since rider_notification_outbox also isn't writable by the
# serving endpoint. Populated from the MLflow trace (the drafts the agent actually sent),
# written from this job's own identity, which has full grants.
OUTBOX_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {outbox_table} (
        notification_id STRING,
        event_id STRING,
        user_id STRING,
        email STRING,
        subject STRING,
        message STRING,
        recorded_at TIMESTAMP
    )
"""

LOOKBACK_DAYS = 1


def _alert_to_prompt(row) -> str:
    fields = row.asDict()
    return " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _extract_drafts_from_response(response, event_id):
    """Pull the drafts list actually sent to record_notifications for this event_id, by
    reading the most recent matching trace. Best-effort -- returns [] if not found."""
    try:
        traces = mlflow.search_traces(
            experiment_ids=["2981512244591491"],  # PULSE_MLFLOW_EXPERIMENT_ID
            max_results=5,
            order_by=["timestamp DESC"],
        )
        for _, trace in traces.iterrows():
            for item in trace["response"]["output"]:
                if item.get("name") == "record_notifications":
                    args = json.loads(item["arguments"])
                    if args.get("event_id") == event_id:
                        return args.get("drafts", [])
    except Exception as e:
        print(f"Could not extract drafts from trace for event_id={event_id}: {e}")
    return []


def main(catalog=None, schema=None, notification_endpoint_name=None, lookback_days=None):
    if catalog is None and schema is None and notification_endpoint_name is None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--CATALOG", default="bootcamp_students")
        parser.add_argument("--SCHEMA", default="pulse")
        parser.add_argument("--NOTIFICATION_ENDPOINT_NAME", default="rail-notification-agent")
        parser.add_argument("--LOOKBACK_DAYS", type=int, default=LOOKBACK_DAYS)
        args = parser.parse_known_args()[0]
        catalog = args.CATALOG
        schema = args.SCHEMA
        notification_endpoint_name = args.NOTIFICATION_ENDPOINT_NAME
        lookback_days = args.LOOKBACK_DAYS
    else:
        catalog = catalog or "bootcamp_students"
        schema = schema or "pulse"
        notification_endpoint_name = notification_endpoint_name or "rail-notification-agent"
        lookback_days = lookback_days if lookback_days is not None else LOOKBACK_DAYS

    ai_alert = f"{catalog}.{schema}.ai_alert"
    # WORKAROUND (still active): the serving endpoint's identity lacks SELECT/MODIFY on
    # ai_alert and rider_notification_outbox, so record_notifications' writes fail from inside
    # the agent. Track "already dispatched" AND reconstruct the outbox here instead, written
    # from this job's own identity (which has full grants), until an admin grants the serving
    # identity directly.
    cursor_table = f"{catalog}.{schema}.dispatch_cursor"
    outbox_table = f"{catalog}.{schema}.dispatch_outbox"

    spark.sql(CURSOR_TABLE_DDL.format(cursor_table=cursor_table))
    spark.sql(OUTBOX_TABLE_DDL.format(outbox_table=outbox_table))

    pending = spark.sql(f"""
        SELECT a.event_id, a.train_id, a.route, a.current_station, a.next_station, a.city_name,
               a.event_type, a.event_date, a.delay_minutes, a.is_delayed, a.is_major_delay,
               a.is_cancelled, a.offroute_ind, a.train_terminated,
               a.current_temperature, a.current_feels_like, a.current_wind_speed,
               a.current_wind_gust, a.current_wind_direction, a.current_humidity,
               a.current_pressure, a.current_weather_description,
               a.forecast_temperature, a.forecast_feels_like, a.forecast_wind_speed,
               a.forecast_wind_gust, a.forecast_wind_direction, a.forecast_humidity,
               a.forecast_pressure, a.forecast_weather_description,
               a.route_reliability_index, a.station_reliability_index
        FROM {ai_alert} a
        LEFT ANTI JOIN {cursor_table} c ON a.event_id = c.event_id
        WHERE a.event_date >= current_date() - INTERVAL {lookback_days} DAY
          AND (a.is_delayed = true OR a.is_cancelled = true OR a.is_major_delay = true)
    """).collect()

    print(f"Found {len(pending)} unprocessed, relevant alert(s) in {ai_alert} "
          f"(last {lookback_days} day(s))")
    if not pending:
        return

    client = DatabricksOpenAI()
    failures = []

    for row in pending:
        prompt = _alert_to_prompt(row)
        try:
            response = client.responses.create(
                model=notification_endpoint_name,
                input=[{"role": "user", "content": prompt}],
            )
            event_id_literal = _escape_sql_literal(row["event_id"])
            spark.sql(f"""
                INSERT INTO {cursor_table} (event_id, dispatched_at)
                VALUES ('{event_id_literal}', current_timestamp())
            """)

            # Pull the actual drafted notifications from this call's trace, since the response
            # object itself doesn't carry structured draft data -- the tool call to
            # record_notifications does. Reconstruct outbox rows from it.
            drafts = _extract_drafts_from_response(response, row["event_id"])
            for draft in drafts:
                notification_id = str(uuid.uuid4())
                user_id = _escape_sql_literal(draft.get("user_id", ""))
                email = _escape_sql_literal(draft.get("email", ""))
                subject = _escape_sql_literal(draft.get("subject", ""))
                message = _escape_sql_literal(draft.get("message", ""))
                spark.sql(f"""
                    INSERT INTO {outbox_table}
                        (notification_id, event_id, user_id, email, subject, message, recorded_at)
                    VALUES ('{notification_id}', '{event_id_literal}', '{user_id}', '{email}',
                            '{subject}', '{message}', current_timestamp())
                """)

            print(f"Dispatched event_id={row['event_id']} ({len(drafts)} notification(s))")
        except Exception as e:
            print(f"Failed to dispatch event_id={row['event_id']}: {e}")
            failures.append(row["event_id"])

    if failures:
        raise RuntimeError(f"{len(failures)} alert(s) failed to dispatch: {failures}")


if __name__ == "__main__":
    main()