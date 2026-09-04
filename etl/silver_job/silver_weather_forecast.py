from ...streaming import get_param
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()



catalog = get_param("CATALOG", "bootcamp_students")
schema = get_param("SCHEMA", "pulse")
source_table = f"{catalog}.{schema}.weather_forecast_bronze"
silver_table = f"{catalog}.{schema}.silver_weather_forecast"



def create_silver_table(table_name = silver_table):

    """Create the daily metrics table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING NOT NULL,
            city_name STRING,
            forecast_timestamp TIMESTAMP,
            temperature DOUBLE,
            feels_like DOUBLE,
            humidity INT,
            pressure INT,
            wind_speed DOUBLE,
            wind_gust DOUBLE,
            wind_direction INT,
            weather_description STRING,
            visibility INT,
            event_date DATE,
            event_hour INT,
            ingest_timestamp TIMESTAMP
        )
            USING DELTA
            PARTITIONED BY (event_date)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
    """)
    print(f"✅ Table ready: {silver_table}")




def build_weather_forecast(source, final):
    
    """
    Clean Bronze Weather Rail events and MERGE into the Silver table.
    """

    dfw = spark.table(source)

    clean_weather_f = (
            dfw
            .dropDuplicates(
            ["city_name", "forecast_timestamp"]
            )
            .filter(F.col("city_name").isNotNull())

            .filter(F.col("forecast_timestamp").isNotNull())

            # Final schema for Silver
            .select(
                F.concat_ws(
                    "_",
                    "city_name",
                    "forecast_timestamp"
                    ).alias("event_id"),
                "city_name",
                "forecast_timestamp",
                "temperature",
                "feels_like",
                "humidity",
                "pressure",
                "wind_speed",
                "wind_gust",
                "wind_direction",
                "weather_description",
                "visibility",
                F.to_date("forecast_timestamp").alias("event_date"),
                F.hour("forecast_timestamp").alias("event_hour"),
                F.current_timestamp().alias("ingest_timestamp")
            )
        )

    clean_weather_f.createOrReplaceTempView("weather_events_incoming")

    spark.sql(f"""
        MERGE INTO {final} AS target
        USING weather_events_incoming AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
    """)

        

def main(): 
    # Create Silver table 
    create_silver_table(silver_table) 
    # Build Silver train events 
    build_weather_forecast(source_table, silver_table) 
    if __name__ == "__main__": 
        main()




