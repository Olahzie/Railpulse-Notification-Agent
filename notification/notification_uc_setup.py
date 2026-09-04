from ..streaming import get_param, get_secret

import psycopg
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# TODO: hardcoded Supabase credentials below are a known security gap -- move to
# get_secret(secret_scope, "SUPABASE_HOST") etc. when there's time to revisit this.


def create_supabase_connection(supabase_host, supabase_port, supabase_db, supabase_user, supabase_password):
    """Creates a PostgreSQL connection to Supabase using the Session Pooler."""
    conn = psycopg.connect(
        host=supabase_host,
        port=supabase_port,
        dbname=supabase_db,
        user=supabase_user,
        password=supabase_password,
        sslmode="require",
    )
    print("Connected to Supabase")
    return conn


def create_rider_preferences(connection, table_name):
    """Reads rider preferences from Supabase and persists them as a Delta table."""
    with connection.cursor() as cur:
        cur.execute("""
            SELECT
                user_id::TEXT AS user_id,
                email,
                route,
                station,
                delay_threshold_minute,
                notify_delay,
                notify_cancellation,
                is_active
            FROM user_preferences
        """)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    rider_preferences_df = spark.createDataFrame(rows, columns)

    rider_preferences_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    print(f"Rider preferences Delta table refreshed: {table_name} ({len(rows)} row(s))")


def create_get_matching_preferences_function(table_name, function_name):
    """Creates a Unity Catalog SQL function returning riders eligible for a disruption alert."""
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {function_name}(
            p_route STRING,
            p_station STRING,
            p_delay_minutes INT,
            p_is_delayed BOOLEAN,
            p_is_cancelled BOOLEAN
        )
        RETURNS TABLE(
            user_id STRING, email STRING, route STRING, station STRING, delay_threshold_minute INT
        )
        LANGUAGE SQL
        COMMENT 'Returns riders eligible for disruption notifications'
        RETURN
            SELECT user_id, email, route, station, delay_threshold_minute
            FROM {table_name}
            WHERE
                is_active = true
                AND (
                    LOWER(TRIM(route)) = LOWER(TRIM(p_route))
                    OR
                    LOWER(TRIM(station)) = LOWER(TRIM(p_station))
                )
                AND (
                    (p_is_delayed = true AND notify_delay = true AND delay_threshold_minute <= p_delay_minutes)
                    OR
                    (p_is_cancelled = true AND notify_cancellation = true)
                )
    """)
    print(f"Function created: {function_name}")


def create_get_alert_history_function(source_table, function_name):
    """
    SQL UC function tool: already-processed (notification_status = 'SENT') ai_alert rows for
    this route/station within a lookback window, most recent first.
    """
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {function_name}(
            p_route STRING,
            p_station STRING,
            p_event_date DATE,
            p_lookback_days INT
        )
        RETURNS TABLE(
            event_id STRING,
            route STRING,
            current_station STRING,
            next_station STRING,
            event_type STRING,
            delay_minutes INT,
            is_delayed BOOLEAN,
            is_major_delay BOOLEAN,
            is_cancelled BOOLEAN,
            event_date DATE,
            notified_at TIMESTAMP
        )
        LANGUAGE SQL
        COMMENT 'Recently processed alerts for this route/station (last N days, up to 20)'
        RETURN
            SELECT event_id, route, current_station, next_station, event_type, delay_minutes,
                   is_delayed, is_major_delay, is_cancelled, event_date, notified_at
            FROM {source_table}
            WHERE notification_status = 'SENT'
              AND (route = p_route OR current_station = p_station)
              AND event_date BETWEEN date_sub(p_event_date, p_lookback_days) AND p_event_date
            ORDER BY event_date DESC
            LIMIT 20
    """)
    print(f"Function ready: {function_name}")


def main(catalog=None, schema=None):
    if catalog is None and schema is None:
        catalog = get_param("CATALOG", "bootcamp_students")
        schema = get_param("SCHEMA", "pulse")
    else:
        catalog = catalog or "bootcamp_students"
        schema = schema or "pulse"

    secret_scope = get_param("DATABRICKS_SECRET_SCOPE", "pulse")
    supabase_port = get_param("SUPABASE_PORT", "5432")
    supabase_db = get_param("SUPABASE_DB", "postgres")
    supabase_host = get_secret(secret_scope, "SUPABASE_HOST")  
    supabase_user = get_secret(secret_scope, "SUPABASE_USER")  
    supabase_password = get_secret(secret_scope, "SUPABASE_PASSWORD") 

    rider_preferences_table = f"{catalog}.{schema}.rider_preferences"
    get_matching_preferences_fn = f"{catalog}.{schema}.get_matching_preferences"
    get_alert_history_fn = f"{catalog}.{schema}.get_alert_history"
    ai_alert_table = f"{catalog}.{schema}.ai_alert"

    conn = create_supabase_connection(supabase_host, supabase_port, supabase_db, supabase_user, supabase_password)
    try:
        create_rider_preferences(conn, rider_preferences_table)
    finally:
        conn.close()

    create_get_matching_preferences_function(rider_preferences_table, get_matching_preferences_fn)
    create_get_alert_history_function(ai_alert_table, get_alert_history_fn)

    # CREATE OR REPLACE FUNCTION wipes existing grants -- must re-grant every time this runs.
    spark.sql(f"GRANT EXECUTE ON FUNCTION {get_matching_preferences_fn} TO `olayodeogunniran@gmail.com`")
    spark.sql(f"GRANT EXECUTE ON FUNCTION {get_alert_history_fn} TO `olayodeogunniran@gmail.com`")
    print("Re-granted EXECUTE on both functions.")


if __name__ == "__main__":
    main()
