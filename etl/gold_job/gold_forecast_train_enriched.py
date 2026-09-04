from ...streaming import get_param
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


catalog = get_param("CATALOG", "bootcamp_students")
schema = get_param("SCHEMA", "pulse")
source_table = f"{catalog}.{schema}.rail_silver_events"
source_table_w = f"{catalog}.{schema}.silver_weather_forecast"
gold_table = f"{catalog}.{schema}.gold_forecast_train"



def create_gold_table(table_name = gold_table):

    """Create the daily metrics table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
             -- Train event identifiers
            event_id STRING,
            train_id STRING,
            city_name STRING,
            -- Forecast weather
            forecast_weather_timestamp TIMESTAMP,
            forecast_temperature DOUBLE,
            forecast_feels_like DOUBLE,
            forecast_humidity INT,
            forecast_pressure INT,
            forecast_wind_speed DOUBLE,
            forecast_wind_gust DOUBLE,
            forecast_wind_direction STRING,
            forecast_weather_description STRING,
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

    forecast_join = (
    df.alias("t")
    .join(
        F.broadcast(df_w.alias("f")),
        (
            (F.col("t.city_name") == F.col("f.city_name")) &
            (
                F.col("f.forecast_timestamp")
                .between(
                    F.col("t.actual_timestamp"),
                    F.col("t.actual_timestamp") + F.expr("INTERVAL 3 HOURS")
                )
            )
        ),
        "left"
    )
)

    window_forecast = (
        Window
        .partitionBy("t.event_id")
        .orderBy(F.col("f.forecast_timestamp").asc())
    )

    forecast_weather_df = (
        forecast_join
        .withColumn(
            "rn",
            F.row_number().over(window_forecast)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
        
        .select(
            # Train event information
            F.col("t.event_id"),
            F.col("t.train_id"),
            F.col("t.city_name"),
            # Forecasted weather
            F.col("f.forecast_timestamp")
                .alias("forecast_weather_timestamp"),
            F.col("f.temperature")
                .alias("forecast_temperature"),
            F.col("f.feels_like")
                .alias("forecast_feels_like"),
            F.col("f.humidity")
                .alias("forecast_humidity"),
            F.col("f.pressure")
                .alias("forecast_pressure"),
            F.col("f.wind_speed")
                .alias("forecast_wind_speed"),
            F.col("f.wind_gust")
                .alias("forecast_wind_gust"),
            F.col("f.wind_direction")
                .alias("forecast_wind_direction"),
            F.col("f.weather_description")
                .alias("forecast_weather_description"),
            # Metadata
            F.current_timestamp()
                .alias("ingest_timestamp")
        )
    )

    forecast_weather_df.createOrReplaceTempView("train_weather_incoming")

    spark.sql(f"""
        MERGE INTO {final} AS target
        USING train_weather_incoming AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
    """)


    print(f"✅ Forecast weather data merged into {final}")


def main(): 
    # Create the target Gold table 
    create_gold_table(gold_table) 
    # Build and populate the Gold table 
    build_train_weather_events(source_table, source_table_w, gold_table ) 
if __name__ == "__main__": 
    main()