import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lower, trim, year, month, dayofmonth, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame


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


# -----------------------------
# Initialize Glue Context
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Glue job started")


try:

    # -----------------------------
    # Read CSV files from S3
    # -----------------------------
    logger.info(f"Reading data from {args['INPUT_PATH']}")

    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [args["INPUT_PATH"]],
            "recurse": True
        },
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ","
        }
    )

    df = datasource.toDF()


    # -----------------------------
    # Data Transformations
    # -----------------------------
    logger.info("Applying transformations")

    df_clean = (
        df
        .withColumn("order_id", col("order_id").cast(IntegerType()))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("product", lower(col("product")))
        .withColumn("processed_timestamp", current_timestamp())
    )


    # -----------------------------
    # Add Partition Columns
    # -----------------------------
    df_partitioned = (
        df_clean
        .withColumn("year", year("processed_timestamp"))
        .withColumn("month", month("processed_timestamp"))
        .withColumn("day", dayofmonth("processed_timestamp"))
    )


    # -----------------------------
    # Convert DataFrame → DynamicFrame
    # -----------------------------
    logger.info("Converting DataFrame to DynamicFrame")

    dynamic_frame = DynamicFrame.fromDF(
        df_partitioned,
        glueContext,
        "dynamic_frame"
    )


    # -----------------------------
    # Write Partitioned Parquet to S3
    # -----------------------------
    logger.info(f"Writing parquet to {args['OUTPUT_PATH']}")

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": args["OUTPUT_PATH"],
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet"
    )


    logger.info("Data successfully written to curated layer")


except Exception as e:

    logger.error("Glue job failed")
    logger.error(str(e))
    raise


# -----------------------------
# Commit Glue Job
# -----------------------------
job.commit()

logger.info("Glue job completed successfully")