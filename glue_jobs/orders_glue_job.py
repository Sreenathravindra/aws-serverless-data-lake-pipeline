import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, lower, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import IntegerType, DoubleType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ----------------------------------
# Logging Setup
# ----------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Glue Job")


# ----------------------------------
# Read Job Parameters
# ----------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "OUTPUT_PATH"
    ]
)

input_path = args["INPUT_PATH"]
output_path = args["OUTPUT_PATH"]


# ----------------------------------
# Initialize Spark & Glue
# ----------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


logger.info(f"Input Path: {input_path}")
logger.info(f"Output Path: {output_path}")


try:

    # ----------------------------------
    # Read Source Data
    # ----------------------------------

    logger.info("Reading CSV files from S3")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    logger.info("Printing schema")
    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows read: {row_count}")


    # ----------------------------------
    # Data Cleaning & Transformation
    # ----------------------------------

    logger.info("Applying transformations")

    df_clean = (

        df.withColumn("order_id", col("order_id").cast(IntegerType()))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("product", lower(col("product")))
        .withColumn("processed_timestamp", current_timestamp())

    )


    # ----------------------------------
    # Add Partition Columns
    # ----------------------------------

    logger.info("Creating partition columns")

    df_partitioned = (

        df_clean
        .withColumn("year", year("processed_timestamp"))
        .withColumn("month", month("processed_timestamp"))
        .withColumn("day", dayofmonth("processed_timestamp"))

    )


    # ----------------------------------
    # Write Data to S3 (Parquet)
    # ----------------------------------

    logger.info("Writing partitioned parquet to S3")

    (
        df_partitioned.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )


    logger.info("Data successfully written to S3")


except Exception as e:

    logger.error("Glue job failed")
    logger.error(str(e))
    raise


# ----------------------------------
# Commit Job
# ----------------------------------

job.commit()

logger.info("Glue Job Completed Successfully")