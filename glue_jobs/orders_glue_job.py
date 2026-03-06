import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lower, trim, year, month, dayofmonth, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------
# Read job parameters
# -----------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "OUTPUT_PATH"
    ]
)

INPUT_PATH = args["INPUT_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"]

# -----------------------------
# Initialize Spark & Glue
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("=======================================")
logger.info("Glue ETL Job Started")
logger.info(f"INPUT_PATH : {INPUT_PATH}")
logger.info(f"OUTPUT_PATH: {OUTPUT_PATH}")
logger.info("=======================================")

try:

    # -----------------------------
    # Read data from S3
    # -----------------------------
    logger.info("Reading CSV files from S3")

    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [INPUT_PATH],
            "recurse": True
        },
        format="csv",
        format_options={"withHeader": True}
    )

    df = datasource.toDF()

    record_count = df.count()
    logger.info(f"Total records read: {record_count}")

    if record_count == 0:
        logger.warning("No data found in input path. Exiting job.")
        job.commit()
        sys.exit(0)

    logger.info("Input schema:")
    df.printSchema()

    # -----------------------------
    # Transformations
    # -----------------------------
    logger.info("Applying transformations")

    df_clean = (
        df.withColumn("order_id", col("order_id").cast(IntegerType()))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("product", lower(col("product")))
        .withColumn("processed_timestamp", current_timestamp())
    )

    # -----------------------------
    # Add partition columns
    # -----------------------------
    logger.info("Adding partition columns")

    df_partitioned = (
        df_clean
        .withColumn("year", year("processed_timestamp"))
        .withColumn("month", month("processed_timestamp"))
        .withColumn("day", dayofmonth("processed_timestamp"))
    )

    # -----------------------------
    # Write parquet to S3
    # -----------------------------
    logger.info("Writing parquet files to curated layer")

    (
        df_partitioned.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(OUTPUT_PATH)
    )

    logger.info("Parquet write completed successfully")

except Exception as e:

    logger.error("Glue job failed due to exception")
    logger.error(str(e))
    raise

# -----------------------------
# Commit job
# -----------------------------
job.commit()

logger.info("Glue ETL Job Completed Successfully")
logger.info("=======================================")