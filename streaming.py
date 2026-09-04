import sys
from typing import Dict

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


def get_param(name: str, default: str) -> str:
    # Databricks Jobs can pass task parameters as notebook widgets (notebook_task)...
    try:
        return dbutils.widgets.get(name)
    except Exception:
        pass

    # ...or as CLI-style arguments (python_wheel_task / spark_python_task), e.g.
    # "--CATALOG bootcamp_students" or "--CATALOG=bootcamp_students" in sys.argv.
    flag = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == flag and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]

    return default


def get_secret(scope: str, key: str, default: str = "") -> str:
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        print(f"get_secret failed for scope={scope!r}, key={key!r}: {e}")
        if not default:
            raise RuntimeError(
                f"Required secret '{key}' in scope '{scope}' could not be retrieved and no default was provided."
            ) from e
        return default


def _confluent_auth_options(
    kafka_api_key: str,
    kafka_api_secret: str,
    kafka_security_protocol: str,
    kafka_sasl_mechanism: str,
) -> Dict[str, str]:
    options = {
        "kafka.security.protocol": kafka_security_protocol,
        "kafka.sasl.mechanism": kafka_sasl_mechanism,
    }

    if kafka_security_protocol.upper().startswith("SASL"):
        if not kafka_api_key or not kafka_api_secret:
            raise ValueError(
                "KAFKA_API_KEY and KAFKA_API_SECRET are required for SASL Kafka connections "
                "(e.g., Confluent Cloud)."
            )
        options["kafka.sasl.jaas.config"] = (
            'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_api_key}" password="{kafka_api_secret}";'
        )

    return options




def build_rail_events_stream(
    spark,
    kafka_bootstrap: str,
    kafka_topic: str,
    starting_offsets: str = "latest",
    kafka_api_key: str = "",
    kafka_api_secret: str = "",
    kafka_security_protocol: str = "SASL_SSL",
    kafka_sasl_mechanism: str = "PLAIN",
) -> DataFrame:
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
    )

    auth_options = _confluent_auth_options(
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
    )
    for key, value in auth_options.items():
        reader = reader.option(key, value)

    raw_stream = reader.load()
    json_str_col = F.col("value").cast("string")

    return (
        raw_stream
        .select(
            F.col("key").cast("string").alias("event_key"),
            F.col("timestamp").alias("kafka_timestamp"),
            json_str_col.alias("raw_json"),
        )
        .select(
            "event_key",
            "kafka_timestamp",
            "raw_json",
            F.get_json_object("raw_json", "$.raw_payload.body.train_id").alias("train_id"),
            F.get_json_object("raw_json", "$.raw_payload.body.actual_timestamp").alias("actual_timestamp"),
            F.get_json_object("raw_json", "$.raw_payload.body.loc_stanox").alias("loc_stanox"),
            F.get_json_object("raw_json", "$.raw_payload.body.gbtt_timestamp").alias("gbtt_timestamp"),
            F.get_json_object("raw_json", "$.raw_payload.body.planned_timestamp").alias("planned_timestamp"),
            F.get_json_object("raw_json", "$.raw_payload.body.planned_event_type").alias("planned_event_type"),
            F.get_json_object("raw_json", "$.raw_payload.body.event_type").alias("event_type"),
            F.get_json_object("raw_json", "$.raw_payload.body.event_source").alias("event_source"),
            F.get_json_object("raw_json", "$.raw_payload.body.correction_ind").alias("correction_ind"),
            F.get_json_object("raw_json", "$.raw_payload.body.offroute_ind").alias("offroute_ind"),
            F.get_json_object("raw_json", "$.raw_payload.body.train_service_code").alias("train_service_code"),
            F.get_json_object("raw_json", "$.raw_payload.body.division_code").alias("division_code"),
            F.get_json_object("raw_json", "$.raw_payload.body.toc_id").alias("toc_id"),
            F.get_json_object("raw_json", "$.raw_payload.body.timetable_variation").alias("timetable_variation"),
            F.get_json_object("raw_json", "$.raw_payload.body.variation_status").alias("variation_status"),
            F.get_json_object("raw_json", "$.raw_payload.body.next_report_stanox").alias("next_report_stanox"),
            F.get_json_object("raw_json", "$.raw_payload.body.next_report_run_time").alias("next_report_run_time"),
            F.get_json_object("raw_json", "$.raw_payload.body.train_terminated").alias("train_terminated"),
            F.from_json(
                    F.get_json_object("raw_json", "$.raw_payload.header"),
                    T.MapType(T.StringType(), T.StringType()),
                    ).alias("header"),
            F.get_json_object("raw_json", "$.raw_payload.body.delay_monitoring_point").alias("delay_monitoring_point"),
            F.get_json_object("raw_json", "$.raw_payload.body.reporting_stanox").alias("reporting_stanox"),
            )
            .withColumn("actual_timestamp", 
                        F.to_timestamp(F.from_unixtime((F.col("actual_timestamp").cast("long") / 1000)))
            )
            .withColumn("planned_timestamp", 
                        F.to_timestamp(F.from_unixtime((F.col("planned_timestamp").cast("long") / 1000)))
            )
            .withColumn("gbtt_timestamp", 
                        F.to_timestamp(F.from_unixtime((F.col("gbtt_timestamp").cast("long") / 1000)))
            )
            .withColumn("ingest_time", F.current_timestamp())
    )





def build_weather_forecast_events_stream(
    spark,
    kafka_bootstrap: str,
    kafka_topic: str,
    starting_offsets: str = "latest",
    kafka_api_key: str = "",
    kafka_api_secret: str = "",
    kafka_security_protocol: str = "SASL_SSL",
    kafka_sasl_mechanism: str = "PLAIN",
) -> DataFrame:
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
    )

    auth_options = _confluent_auth_options(
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
    )
    for key, value in auth_options.items():
        reader = reader.option(key, value)

    raw_stream = reader.load()
    json_str_col = F.col("value").cast("string")

    return (
        raw_stream
    .select(
        F.col("key").cast("string").alias("event_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        json_str_col.alias("raw_json"),
    )
    .select(
        "event_key",
        "kafka_timestamp",
        "raw_json",
        # City
        F.get_json_object("raw_json", "$.raw_payload.city.name").alias("city_name"),
        # Forecast
        F.get_json_object("raw_json", "$.raw_payload.forecast.dt").alias("forecast_timestamp"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.dt_txt").alias("forecast_time"),

        # Main weather
        F.get_json_object("raw_json", "$.raw_payload.forecast.main.temp").cast("double").alias("temperature"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.main.feels_like").cast("double").alias("feels_like"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.main.humidity").cast("int").alias("humidity"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.main.pressure").cast("int").alias("pressure"),

        # Wind
        F.get_json_object("raw_json", "$.raw_payload.forecast.wind.speed").cast("double").alias("wind_speed"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.wind.gust").cast("double").alias("wind_gust"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.wind.deg").cast("int").alias("wind_direction"),

        # Weather array (first element)
        F.get_json_object("raw_json", "$.raw_payload.forecast.weather[0].description").alias("weather_description"),

        # Other
        F.get_json_object("raw_json", "$.raw_payload.forecast.visibility").cast("int").alias("visibility"),
        F.get_json_object("raw_json", "$.raw_payload.forecast.pop").cast("double").alias("precipitation_probability"),
        )
        .withColumn(
        "forecast_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("forecast_timestamp").cast("long")))
        )
        .withColumn(
            "forecast_time",
            F.to_timestamp(F.col("forecast_time"),"yyyy-MM-dd HH:mm:ss")
        )
        
        .withColumn("ingest_timestamp",F.current_timestamp())
    
    )
    



def build_weather_events_stream(
    spark,
    kafka_bootstrap: str,
    kafka_topic: str,
    starting_offsets: str = "latest",
    kafka_api_key: str = "",
    kafka_api_secret: str = "",
    kafka_security_protocol: str = "SASL_SSL",
    kafka_sasl_mechanism: str = "PLAIN",
) -> DataFrame:
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
    )

    auth_options = _confluent_auth_options(
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        kafka_security_protocol=kafka_security_protocol,
        kafka_sasl_mechanism=kafka_sasl_mechanism,
    )
    for key, value in auth_options.items():
        reader = reader.option(key, value)

    raw_stream = reader.load()
    json_str_col = F.col("value").cast("string")

    return (
        raw_stream
    .select(
        F.col("key").cast("string").alias("event_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        json_str_col.alias("raw_json"),
    )
    .select(
        "event_key",
        "kafka_timestamp",
        "raw_json",
        # City
        F.get_json_object("raw_json", "$.raw_payload.name").alias("city_name"),

        # Timestamp
        F.get_json_object("raw_json", "$.raw_payload.dt").alias("weather_timestamp"),

        # Main
        F.get_json_object("raw_json", "$.raw_payload.main.temp").cast("double").alias("temperature"),
        F.get_json_object("raw_json", "$.raw_payload.main.feels_like").cast("double").alias("feels_like"),
        F.get_json_object("raw_json", "$.raw_payload.weather[0].description").alias("weather_description"),
        F.get_json_object("raw_json", "$.raw_payload.main.temp_min").cast("double").alias("temp_min"),
        F.get_json_object("raw_json", "$.raw_payload.main.temp_max").cast("double").alias("temp_max"),
        F.get_json_object("raw_json", "$.raw_payload.main.pressure").cast("int").alias("pressure"),
        F.get_json_object("raw_json", "$.raw_payload.main.humidity").cast("int").alias("humidity"),
        F.get_json_object("raw_json", "$.raw_payload.main.sea_level").cast("int").alias("sea_level"),
        F.get_json_object("raw_json", "$.raw_payload.main.grnd_level").cast("int").alias("ground_level"),

        # Wind
        F.get_json_object("raw_json", "$.raw_payload.wind.speed").cast("double").alias("wind_speed"),
        F.get_json_object("raw_json", "$.raw_payload.wind.deg").cast("int").alias("wind_direction"),
        F.get_json_object("raw_json", "$.raw_payload.wind.gust").cast("double").alias("wind_gust"),
    )
    .withColumn(
        "weather_timestamp",
        F.to_timestamp(
            F.from_unixtime(F.col("weather_timestamp").cast("long"))
        )
    )
    .withColumn(
        "ingest_timestamp",F.current_timestamp())
    
)