from ...streaming import build_weather_events_stream, get_param, get_secret

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


def main():
    kafka_topic = get_param("KAFKA_TOPIC", "weather-updates")  
    secret_scope = get_param("DATABRICKS_SECRET_SCOPE", "pulse")
    kafka_bootstrap = get_secret(secret_scope, "CONFLUENT_BOOTSTRAP_SERVER")
    kafka_api_key = get_secret(secret_scope, "CONFLUENT_API_KEY")
    kafka_api_secret = get_secret(secret_scope, "CONFLUENT_API_SECRET")
    kafka_security_protocol = get_param("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    kafka_sasl_mechanism = get_param("KAFKA_SASL_MECHANISM", "PLAIN")

    trigger_interval = get_param("TRIGGER_INTERVAL", "10 seconds")
    checkpoint_base = get_param("CHECKPOINT_BASE", "/Volumes/bootcamp_students/pulse/checkpoints")
    catalog = get_param("CATALOG", "bootcamp_students")
    schema = get_param("SCHEMA", "pulse")

    bronze_table = f"{catalog}.{schema}.weather_bronze"
    checkpoint = f"{checkpoint_base}/bronze/weather"

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
      event_key STRING,
      kafka_timestamp TIMESTAMP,
      raw_json STRING,
      city_name STRING,
      weather_timestamp TIMESTAMP,
      temperature DOUBLE,
      temp_min DOUBLE,
      temp_max DOUBLE,
      feels_like DOUBLE,
      weather_description STRING,
      humidity INT,
      pressure INT,
      sea_level INT,
      ground_level INT,
      wind_speed DOUBLE,
      wind_direction INT,
      wind_gust DOUBLE,
      ingest_timestamp TIMESTAMP
    ) USING DELTA
    """)

    web_events = build_weather_events_stream(
        spark=spark,
        kafka_bootstrap=kafka_bootstrap,
        kafka_topic=kafka_topic,
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
        starting_offsets='earliest'
    )

    query = (
        web_events.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )


    print("Bronze stream started:", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()