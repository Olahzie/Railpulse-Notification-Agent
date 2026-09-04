from ...streaming import get_param
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()



catalog = get_param("CATALOG", "bootcamp_students")
schema = get_param("SCHEMA", "pulse")
train_weather_enriched = f"{catalog}.{schema}.gold_weather_train"
train_forecast_enriched = f"{catalog}.{schema}.gold_forecast_train"
gold_route_reliability  = f"{catalog}.{schema}.gold_route_reliability"
gold_station_reliability = f"{catalog}.{schema}.gold_station_reliability"
ai_alert = f"{catalog}.{schema}.ai_alert"
rider_notification_outbox = f"{catalog}.{schema}.rider_notification_outbox"



def create_alerts_table(table_name = ai_alert):

    """Create the daily metrics table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ai_alert} (
            event_id STRING,
            train_id STRING,
            route STRING,
            current_station STRING,
            next_station STRING,
            city_name STRING,
            event_type STRING,
            event_date DATE,
            variation_status STRING,
            delay_minutes INT,
            offroute_ind BOOLEAN,
            train_terminated BOOLEAN,
            planned_timestamp TIMESTAMP,
            actual_timestamp TIMESTAMP,
            current_weather_timestamp TIMESTAMP,
            current_temperature DOUBLE,
            current_feels_like DOUBLE,
            current_weather_description STRING,
            current_pressure INT,
            current_humidity INT,
            current_wind_speed DOUBLE,
            current_wind_gust DOUBLE,
            current_wind_direction STRING,
            forecast_timestamp TIMESTAMP,
            forecast_temperature DOUBLE,
            forecast_feels_like DOUBLE,
            forecast_humidity INT,
            forecast_pressure INT,
            forecast_wind_speed DOUBLE,
            forecast_wind_gust DOUBLE,
            forecast_weather_description STRING,
            forecast_wind_direction STRING,
            hour_of_day INT,
            day_of_week STRING,
            weekend_flag BOOLEAN,
            is_peak_hour BOOLEAN,
            is_cancelled BOOLEAN,
            is_delayed BOOLEAN,
            is_major_delay BOOLEAN,
            route_average_delay DOUBLE,
            route_scheduled_trains INT,
            route_on_time_trains INT,
            route_late_trains INT,
            route_reliability_index DOUBLE,
            station_average_delay DOUBLE,
            station_scheduled_trains INT,
            station_on_time_trains INT,
            station_late_trains INT,
            station_reliability_index DOUBLE,
            ingest_timestamp TIMESTAMP,

            -- Notification dispatch tracking (set by the rail-notification-agent, not by this MERGE)
            notification_status STRING,
            notified_at TIMESTAMP

        )
        USING DELTA
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

   

    print(f"✅ Table ready: {ai_alert}")


def create_notification_outbox_table(table_name=rider_notification_outbox):
    """Create the drafted/emailed-notifications outbox (delivery log) table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            notification_id STRING,
            event_id STRING,
            user_id STRING,
            email STRING,
            route STRING,
            station STRING,
            subject STRING,
            message STRING,
            delivery_status STRING,
            created_at TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

   
    print(f"✅ Table ready: {table_name}")




def build_train_events(source_weather, source_forecast, source_station, source_route, source_final):

    train_weather_enriched = spark.table(source_weather)
    train_forecast_weather = spark.table(source_forecast)
    gold_station_reliability = spark.table(source_station)
    gold_route_reliability = spark.table(source_route)

    
    """
    Clean Bronze Network Rail events and MERGE into the Silver table.
    """
    # Read Bronze table
    ai_alert_context = (
        train_weather_enriched.alias("t")

        # Forecast weather
        .join(
            train_forecast_weather.alias("f"),
            "event_id",
            "left"
        )

        # Route metrics
        .join(
            gold_route_reliability.alias("r"),
            (
                (F.col("t.route") == F.col("r.route")) &
                (
                    F.to_date("t.actual_timestamp") ==
                    F.col("r.date")
                )
            ),
            "left"
        )

        # Station metrics
        .join(
            gold_station_reliability.alias("s"),
            (
                (F.col("t.current_station_name") == F.col("s.current_station_name")) &
                (F.col("t.event_type") == F.col("s.event_type")) &
                (
                    F.to_date("t.actual_timestamp") ==
                    F.col("s.date")
                )
            ),
            "left"
        )

        .select(

            # Train Event
            F.col("t.event_id"),
            F.col("t.train_id"),
            F.col("t.route"),
            F.col("t.current_station_name").alias("current_station"),
            F.col("t.next_station_name").alias("next_station"),
            F.col("t.city_name"),
            F.to_date("t.actual_timestamp").alias("event_date"),
            F.col("t.event_type"),
            F.col("t.variation_status"),
            F.col("t.delay_minutes"),
            F.col("t.offroute_ind"),
            F.col("t.train_terminated"),
            F.col("t.planned_timestamp"),
            F.col("t.actual_timestamp"),

            # Current Weather
            F.col("t.current_weather_timestamp"),
            F.col("t.current_temperature"),
            F.col("t.current_feels_like"),
            F.col("t.current_weather_description"),
            F.col("t.current_pressure"),
            F.col("t.current_humidity"),
            F.col("t.current_wind_speed"),
            F.col("t.current_wind_gust"),
            F.col("t.current_wind_direction"),

            # Forecast Weather
            F.col("f.forecast_weather_timestamp").alias("forecast_timestamp"),
            F.col("f.forecast_temperature"),
            F.col("f.forecast_feels_like"),
            F.col("f.forecast_pressure"),
            F.col("f.forecast_wind_speed"),
            F.col("f.forecast_humidity"),
            F.col("f.forecast_wind_gust"),
            F.col("f.forecast_weather_description"),
            F.col("f.forecast_wind_direction"),

            # Time Context
            F.col("t.hour_of_day"),
            F.col("t.day_of_week"),
            F.col("t.weekend_flag"),
            F.col("t.is_peak_hour"),
            F.col("t.is_cancelled"),
            F.col("t.is_delayed"),
            F.col("t.is_major_delay"),
            # Route Metrics
            F.col("r.avg_delay").alias("route_average_delay"),
            F.col("r.scheduled_trains").alias("route_scheduled_trains"),
            F.col("r.on_time_trains").alias("route_on_time_trains"),
            F.col("r.late_trains").alias("route_late_trains"),
            F.col("r.reliability_index").alias("route_reliability_index"),

            # Station Metrics
            F.col("s.avg_delay").alias("station_average_delay"),
            F.col("s.scheduled_trains").alias("station_scheduled_trains"),
            F.col("s.on_time_trains").alias("station_on_time_trains"),
            F.col("s.late_trains").alias("station_late_trains"),
            F.col("s.reliability_index").alias("station_reliability_index"),
            F.lit(None).cast("string").alias("notification_status"),
            F.lit(None).cast("timestamp").alias("notified_at"),

            # Metadata
            F.current_timestamp().alias("ingest_timestamp")
        )
            .filter(
                (F.col("t.is_delayed") == True)
                | (F.col("t.is_cancelled") == True)
                | (F.col("t.is_major_delay") == True)
            )
        )



    ai_alert_context.createOrReplaceTempView("ai_alert_context_incoming")

    spark.sql(f"""
    MERGE INTO {source_final} AS target
    USING ai_alert_context_incoming AS source
    ON target.event_id = source.event_id
    WHEN MATCHED THEN
    UPDATE SET *
    WHEN NOT MATCHED THEN
    INSERT *
    """)



def main():
    create_alerts_table()
    create_notification_outbox_table()

    build_train_events(
        train_weather_enriched,
        train_forecast_enriched,
        gold_station_reliability,
        gold_route_reliability,
        ai_alert
    )


if __name__ == "__main__":
    main()

