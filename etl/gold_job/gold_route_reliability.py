from ...streaming import get_param
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


catalog = get_param("CATALOG", "bootcamp_students")
schema = get_param("SCHEMA", "pulse")
source_table = f"{catalog}.{schema}.rail_silver_events"
gold_table = f"{catalog}.{schema}.gold_route_reliability"



def create_gold_table(table_name = gold_table):

    """Create the route performance metrics table if it doesn't exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            route STRING,
            date DATE,
            scheduled_trains INT,
            on_time_trains INT,
            late_trains INT,
            avg_delay DOUBLE,
            reliability_index DOUBLE,
            ingest_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    print(f"✅ Table ready: {gold_table}")

create_gold_table()




def build_route_metrics(source, final):
    """
    Build daily route performance metrics and overwrite the Gold table.
    """

    # Read Silver train events
    silver_data_t = spark.table(source)

    # Create one record per train/route/date
    train_daily_status = (
        silver_data_t

        .withColumn(
            "date",
            F.to_date("actual_timestamp")
        )

        .groupBy(
            "train_id",
            "route",
            "date",
            "variation_status"
        )

        .agg(
            F.max("delay_minutes")
            .alias("final_delay_minutes")
        )

        .withColumn(
            "train_status",
            F.when(
                (F.col("final_delay_minutes") <= 5) & (F.col("variation_status") == "LATE"),
                "ON_TIME"
            )
            .otherwise("LATE")
        )
    )

    route_metrics = (
        train_daily_status

        .groupBy(
            "route",
            "date"
        )

        .agg(

            F.countDistinct("train_id")
            .alias("scheduled_trains"),

            F.countDistinct(
                F.when(
                    F.col("train_status") == "ON_TIME",
                    F.col("train_id")
                )
            )
            .alias("on_time_trains"),

            F.countDistinct(
                F.when(
                    F.col("train_status") == "LATE",
                    F.col("train_id")
                )
            )
            .alias("late_trains"),

            F.round(
                F.avg("final_delay_minutes"),
                2
            )
            .alias("avg_delay")
        )
    )



    route_metrics = (
        route_metrics

        .withColumn(
            "reliability_index",
            F.round(
                (
                    F.col("on_time_trains") /
                    F.col("scheduled_trains")
                ) * 100,
                2
            )
        )
        .withColumn(
            "ingest_timestamp",
            F.current_timestamp()
    )
    )
        
    # Overwrite Gold table
    (
        route_metrics
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(final)
    )

    print(f"Successfully rebuilt {final}")



def main(): 
    # Create the Gold table 
    create_gold_table(gold_table) 
    # Build route reliability metrics 
    build_route_metrics( source_table, gold_table ) 
if __name__ == "__main__": 
    main()
