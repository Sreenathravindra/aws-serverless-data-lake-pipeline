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

logger.info(f"Starting Glue Job: {args['JOB_NAME']}")
logger.info(f"Reading input data from: {input_path}")


# ----------------------------------
# Read Validated Data from S3
# ----------------------------------

df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("recursiveFileLookup", "true")
    .csv(input_path)
)

logger.info("Input data successfully loaded")

df.printSchema()


# ----------------------------------
# Data Cleaning & Transformations
# ----------------------------------

logger.info("Applying data transformations")

df_clean = (

    df
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("discount", col("discount").cast(DoubleType()))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("product_name", lower(col("product_name")))
    .withColumn(
        "order_timestamp",
        to_timestamp(col("order_timestamp"), "dd-MM-yyyy HH:mm")
    )

)

# Remove records with invalid timestamps

df_clean = df_clean.filter(col("order_timestamp").isNotNull())


# ----------------------------------
# Add Processing Metadata
# ----------------------------------

df_clean = df_clean.withColumn(
    "processed_timestamp",
    current_timestamp()
)


# ----------------------------------
# Create Partition Columns
# ----------------------------------

logger.info("Creating partition columns")

df_partitioned = (
    df_clean
    .withColumn("year", year("order_timestamp"))
    .withColumn("month", month("order_timestamp"))
    .withColumn("day", dayofmonth("order_timestamp"))
)


# ----------------------------------
# Write Curated Parquet to S3
# ----------------------------------

logger.info("Writing curated parquet data to S3")

(
    df_partitioned
    .repartition("year", "month")  # better write performance
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(curated_path)
)

logger.info("Data successfully written to curated layer")

# ----------------------------------
# Job Completion
# ----------------------------------

logger.info("Glue Job Completed Successfully")

job.commit()
