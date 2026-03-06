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

# -----------------------------
# Initialize Glue
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Glue job started")

try:

    # -----------------------------
    # Read staging data
    # -----------------------------
    logger.info(f"Reading data from {args['INPUT_PATH']}")

    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [args["INPUT_PATH"]],
            "recurse": True
        },
        format="csv",
        format_options={"withHeader": True}
    )

    df = datasource.toDF()

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
    # Partition columns
    # -----------------------------
    df_partitioned = (
        df_clean
        .withColumn("year", year("processed_timestamp"))
        .withColumn("month", month("processed_timestamp"))
        .withColumn("day", dayofmonth("processed_timestamp"))
    )

    # -----------------------------
    # Write parquet
    # -----------------------------
    logger.info(f"Writing parquet to {args['OUTPUT_PATH']}")

    (
        df_partitioned.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(args["OUTPUT_PATH"])
    )

    logger.info("Write completed successfully")

except Exception as e:
    logger.error("Glue job failed")
    logger.error(str(e))
    raise

# -----------------------------
# Commit bookmark
# -----------------------------
job.commit()

logger.info("Glue job completed")