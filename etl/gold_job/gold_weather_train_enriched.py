from ...streaming import get_param
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


catalog = get_param("CATALOG", "bootcamp_students")
schema = get_param("SCHEMA", "pulse")
source_table = f"{catalog}.{schema}.rail_silver_events"
source_table_w = f"{catalog}.{schema}.silver_weather_current"
gold_table = f"{catalog}.{schema}.gold_weather_train"



def create_gold_table(table_name = gold_table):

    """Create the daily metrics table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING,
            train_id STRING,
            route STRING,
            current_station_name STRING,
            next_station_name STRING,
            city_name STRING,
            event_type STRING,
            offroute_ind BOOLEAN,
            train_terminated BOOLEAN,
            variation_status STRING,
            delay_minutes INT,
            actual_timestamp TIMESTAMP,
            planned_timestamp TIMESTAMP,
            current_weather_timestamp TIMESTAMP,
            current_temperature DOUBLE,
            current_feels_like DOUBLE,
            current_weather_description STRING,
            current_humidity INT,
            current_pressure INT,
            current_wind_speed DOUBLE,
            current_wind_gust DOUBLE,
            current_wind_direction STRING,
            hour_of_day INT,
            day_of_week STRING,
            weekend_flag BOOLEAN,
            is_peak_hour BOOLEAN,
            month INT,
            year INT,
            is_delayed BOOLEAN,
            is_major_delay BOOLEAN,
            is_cancelled BOOLEAN,
            ingest_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (city_name)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    print(f"✅ Table ready: {gold_table}")




def build_train_weather_events(source, source_w, final):
    
    """
    Clean Silver Network Rail events and MERGE into the Gold table.
    """

    df = spark.table(source)
    df_w = spark.table(source_w)

    current_join = (
    df.alias("t")
    .join(
        F.broadcast(df_w.alias("w")),
        (
            (F.col("t.city_name") == F.col("w.city_name")) &
            (
                F.col("w.weather_timestamp")
                .between(
                    F.col("t.actual_timestamp") - F.expr("INTERVAL 30 MINUTES"),
                    F.col("t.actual_timestamp")
                )
            )
        ),
        "left"
    )
)

    current_window = (
        Window
        .partitionBy("t.event_id")
        .orderBy(F.col("w.weather_timestamp").desc())
    )

    current_weather_df = (
        current_join
        .withColumn(
            "rn",
            F.row_number().over(current_window)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn(
            "hour_of_day",
            F.hour("actual_timestamp")
        )
        .withColumn(
            "day_of_week",
            F.date_format("actual_timestamp", "EEEE")
        )
        .withColumn(
            "weekend_flag",
            F.dayofweek("actual_timestamp").isin([1, 7])
        )

        .withColumn(
        "month",
        F.month("actual_timestamp")
        )

        .withColumn(
            "year",
            F.year("actual_timestamp")
        )

        .withColumn(
            "date",
            F.to_date("actual_timestamp")
        )

        .withColumn(
            "minutes_delay",
            F.col("delay_minutes")
        )

        .withColumn(
            "is_delayed",
            (F.col("delay_minutes") > 5) & (F.col("variation_status") == 'LATE')
        )

        .withColumn(
            "is_major_delay",
            (F.col("delay_minutes") >= 15) & (F.col("variation_status") == 'LATE')
        )

        .withColumn(
            "is_cancelled",
            F.col("variation_status") == "CANCELLED"
        )

        .withColumn(
            "is_peak_hour",
            (
                (F.hour("actual_timestamp").between(6, 9)) |
                (F.hour("actual_timestamp").between(16, 19))
            )
        )
        .select(
            # Train event information
            F.col("t.event_id"),
            F.col("t.train_id"),
            F.col("t.route"),
            F.col("t.current_station_name"),
            F.col("t.next_station_name"),
            F.col("t.city_name"),

            F.col("t.event_type"),
            F.col("t.variation_status"),
            F.col("t.delay_minutes"),
            F.col("t.offroute_ind"),
            F.col("t.train_terminated"),
            F.col("t.actual_timestamp"),
            F.col("t.planned_timestamp"),

            # Current weather
            F.col("w.weather_timestamp")
                .alias("current_weather_timestamp"),
            F.col("w.temperature")
                .alias("current_temperature"),
            F.col("w.feels_like")
                .alias("current_feels_like"),
            F.col("w.weather_description")
                .alias("current_weather_description"),
            F.col("w.humidity")
                .alias("current_humidity"),
            F.col("w.pressure")
                .alias("current_pressure"),
            F.col("w.wind_speed")
                .alias("current_wind_speed"),
            F.col("w.wind_gust")
                .alias("current_wind_gust"),
            F.col("w.wind_direction")
                .alias("current_wind_direction"),

            # Time context
            "hour_of_day",
            "day_of_week",
            "weekend_flag",
            "is_peak_hour",
            "month",
            "year",
            "is_delayed",
            "is_major_delay",
            "is_cancelled",

            # Metadata
            F.current_timestamp()
                .alias("ingest_timestamp")
        )
    )

    current_weather_df.createOrReplaceTempView("train_weather_incoming")

    spark.sql(f"""
        MERGE INTO {final} AS target
        USING train_weather_incoming AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
    """)



def main(): 
    # Create the target Gold table 
    create_gold_table(gold_table) 
    # Build and populate the Gold table 
    build_train_weather_events(source_table, source_table_w, gold_table ) 
if __name__ == "__main__": 
    main()