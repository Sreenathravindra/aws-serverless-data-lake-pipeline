import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ----------------------------------
# Logging Setup
# ----------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ----------------------------------
# Job Parameters
# ----------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "CURATED_PATH"
    ]
)

input_path = args["INPUT_PATH"]
curated_path = args["CURATED_PATH"]


# ----------------------------------
# Initialize Spark & Glue
# ----------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ----------------------------------
# Read Validated Data from S3
# ----------------------------------

logger.info("Reading validated data from S3")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .csv(input_path)
)


# ----------------------------------
# Data Cleaning
# ----------------------------------

logger.info("Applying transformations")

df_clean = (

    df.withColumn("price", col("price").cast(DoubleType()))
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("discount", col("discount").cast(DoubleType()))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("product_name", lower(col("product_name")))
    .withColumn(
        "order_timestamp",
        to_timestamp(col("order_timestamp"), "dd-MM-yyyy HH:mm")
    )

)


# ----------------------------------
# Create Date Partition Columns
# ----------------------------------

logger.info("Creating partition columns")

df_clean = (
    df_clean
    .withColumn("year", year("order_timestamp"))
    .withColumn("month", month("order_timestamp"))
    .withColumn("day", dayofmonth("order_timestamp"))
)


# ----------------------------------
# Write Curated Parquet to S3
# ----------------------------------

logger.info("Writing curated parquet to S3")

(
    df_clean.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(curated_path)
)


logger.info("Glue Job Completed Successfully")

job.commit()