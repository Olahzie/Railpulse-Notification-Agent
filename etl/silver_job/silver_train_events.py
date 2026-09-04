from ...streaming import get_param
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()




lookup_base = get_param("CHECKPOINT_BASE", "/Volumes/bootcamp_students/pulse/lookup")
catalog = get_param("CATALOG", "bootcamp_students")
schema = get_param("SCHEMA", "pulse")
source_table = f"{catalog}.{schema}.rail_bronze"
silver_table = f"{catalog}.{schema}.rail_silver_events"



def create_silver_table(table_name = silver_table):

    """Create the daily metrics table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING,
            train_id STRING,
            train_service_code STRING,
            city_name STRING,
            route STRING,
            event_type STRING,
            variation_status STRING,
            actual_timestamp TIMESTAMP,
            planned_timestamp TIMESTAMP,
            delay_minutes INT,
            loc_stanox STRING,
            current_station_name STRING,
            next_report_stanox STRING,
            next_station_name STRING,
            offroute_ind BOOLEAN,
            correction_ind BOOLEAN,
            train_terminated BOOLEAN,
            delay_monitoring_point BOOLEAN,
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





def build_train_events(source, final, lookup):
    
    """
    Clean Bronze Network Rail events and MERGE into the Silver table.
    """
    # Read Bronze table
    station_code_df = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{lookup}/station_codes.csv")
    )

   
    mapping_df = (
        spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(f"{lookup}/mapping.csv")
    )




    df = spark.table(source)

    clean_df = (
            df
            .dropDuplicates(
            ["train_id", "actual_timestamp", "loc_stanox"]
            )

            .withColumn(
                "offroute_ind",
                F.col("offroute_ind").cast("boolean")
            )

            .withColumn(
                "train_terminated",
                F.col("train_terminated").cast("boolean")
            )

            .withColumn(
                "delay_monitoring_point",
                F.col("delay_monitoring_point").cast("boolean")
            )

            .withColumn(
                "correction_ind",
                F.col("correction_ind").cast("boolean")
            )

            .withColumn(
                "next_report_run_time",
                F.col("next_report_run_time").cast("int")
            )

            .withColumn(
                "timetable_variation",
                F.col("timetable_variation").cast("int")
            )
            .withColumn(
                "toc_id",
                F.col("toc_id").cast("int")
            )
            .filter(F.col("train_id").isNotNull())

            .filter(F.col("loc_stanox").isNotNull())

            # Final schema for Silver
            .select(
                "train_id",
                "train_service_code",
                "actual_timestamp",
                "planned_timestamp",
                "event_type",
                "planned_event_type",
                "variation_status",
                "event_source",
                "loc_stanox",
                "next_report_stanox",
                "next_report_run_time",
                "toc_id",
                "division_code",
                "offroute_ind",
                "train_terminated",
                "delay_monitoring_point",
                "correction_ind",
                "timetable_variation"
            )
        )

    current_station = F.broadcast(station_code_df)
    next_station = F.broadcast(station_code_df)

    

    train_events = (
        clean_df.alias("t")

        # Current station
        .join(
            current_station.alias("current"),
            F.col("t.loc_stanox") == F.col("current.STANOX_NO"),
            "left"
        )

        # Next station
        .join(
            next_station.alias("next"),
            F.col("t.next_report_stanox") == F.col("next.STANOX_NO"),
            "left"
        )
        # Route mapping
        .join(
            F.broadcast(mapping_df).alias("map"),
            F.col("current.Route_Description") == F.col("map.Route"),
            "inner"
        )

        .select(
            F.concat_ws(
                "_",
                F.col("t.train_id"),
                F.col("t.actual_timestamp"),
                F.col("t.loc_stanox")
            ).alias("event_id"),
            F.col("t.train_id"),
            F.col("t.train_service_code"),
            F.col("t.actual_timestamp"),
            F.col("t.planned_timestamp"),
            F.col("t.event_type"),
            F.col("t.planned_event_type"),
            F.col("t.variation_status"),
            F.col("t.offroute_ind"),
            F.col("t.correction_ind"),
            F.col("t.delay_monitoring_point"),
            F.col("t.train_terminated"),
            F.col("t.timetable_variation").alias("delay_minutes"),
            F.col("t.loc_stanox"),
            F.col("t.next_report_stanox"),
            F.col("current.FULL_NAME").alias("current_station_name"),
            F.col("next.FULL_NAME").alias("next_station_name"),
            F.col("current.Route_Description").alias("route"),
            F.col("map.City").alias("city_name"),
            F.to_date("t.actual_timestamp").alias("event_date"),
            F.hour("t.actual_timestamp").alias("event_hour"),
            F.current_timestamp().alias("ingest_timestamp")

        )
    )
    

    train_events.createOrReplaceTempView("train_events_incoming")

    spark.sql(f"""
        MERGE INTO {final} AS target
        USING train_events_incoming AS source
        ON target.train_id = source.train_id
        AND target.actual_timestamp = source.actual_timestamp
        AND target.loc_stanox = source.loc_stanox
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
    """)


def main(): 
    # Create Silver table 
    create_silver_table(silver_table) 
    # Build Silver train events 
    build_train_events( source_table, silver_table, lookup_base ) 
if __name__ == "__main__": 
    main()
