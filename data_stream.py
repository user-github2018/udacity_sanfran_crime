import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "sanfrancisco.police.stats.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetPerTrigger", 200) \
        .option("maxRatePerPartition", 200) \
        
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)", "timestamp")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_TABLE"), psf.col('timestamp')) \
        .select("SERVICE_TABLE.*", "timestamp")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(
        psf.col("original_crime_type_name"),
        psf.col("disposition"),
        psf.col("call_date_time"),
        psf.col("timestamp")
    ) \
        .withWatermark("call_date_time", "30 seconds")

    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name", psf.window("call_date_time", "60 minutes")).count()

    # batch ingestion of the aggregation
    # write output stream
    query = agg_df \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()

    query.awaitTermination()
    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .queryName("join") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    sparkSession = SparkSession \
        .builder \
        .config('spark.ui.port', 3000) \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    logger.info("Spark started")
    
    run_spark_job(sparkSession)

    sparkSession.stop()
