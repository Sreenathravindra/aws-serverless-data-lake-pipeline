import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame


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
        "CURATED_PATH",
        "REDSHIFT_TEMP_DIR",
        "REDSHIFT_CONNECTION"
    ]
)

input_path = args["INPUT_PATH"]
curated_path = args["CURATED_PATH"]
temp_dir = args["REDSHIFT_TEMP_DIR"]
redshift_conn = args["REDSHIFT_CONNECTION"]


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
          to_timestamp("order_timestamp", "dd-MM-yyyy HH:mm")
      )
)


# ----------------------------------
# Create Date Columns
# ----------------------------------

df_clean = (
    df_clean
    .withColumn("year", year("order_timestamp"))
    .withColumn("month", month("order_timestamp"))
    .withColumn("day", dayofmonth("order_timestamp"))
)


# ==================================================
# Create Dimension Tables
# ==================================================

logger.info("Creating dimension tables")


dim_customer = (
    df_clean
    .select("customer_id", "customer_name", "email")
    .dropDuplicates()
)


dim_product = (
    df_clean
    .select("product_id", "product_name", "category")
    .dropDuplicates()
)


dim_location = (
    df_clean
    .select(
        "shipping_city",
        "shipping_state",
        "shipping_country"
    )
    .dropDuplicates()
)


dim_payment = (
    df_clean
    .select("payment_method")
    .dropDuplicates()
)


# ⭐ Better Date Dimension
dim_date = (
    df_clean
    .select(
        "year",
        "month",
        "day"
    )
    .dropDuplicates()
)


# ==================================================
# Create Fact Table
# ==================================================

logger.info("Creating fact table")

fact_sales = (
    df_clean.select(
        "order_id",
        "customer_id",
        "product_id",
        "payment_method",
        "price",
        "quantity",
        "discount",
        "order_status",
        "year",
        "month",
        "day"
    )
)


# ==================================================
# Write Curated Data to S3 (Parquet)
# ==================================================

logger.info("Writing curated parquet to S3")

(
    df_clean.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(curated_path)
)


# ==================================================
# Function to Write to Redshift
# ==================================================

def write_to_redshift(df, table):

    logger.info(f"Writing {table} to Redshift")

    dyf = DynamicFrame.fromDF(df, glueContext, table)

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="redshift",
        connection_options={
            "connectionName": redshift_conn,
            "dbtable": table,
            "redshiftTmpDir": temp_dir
        }
    )


# ==================================================
# Load Dimension Tables
# ==================================================

logger.info("Loading dimension tables to Redshift")

write_to_redshift(dim_customer, "dim_customer")
write_to_redshift(dim_product, "dim_product")
write_to_redshift(dim_location, "dim_location")
write_to_redshift(dim_payment, "dim_payment")
write_to_redshift(dim_date, "dim_date")


# ==================================================
# Load Fact Table
# ==================================================

logger.info("Loading fact table to Redshift")

write_to_redshift(fact_sales, "fact_sales")


# ----------------------------------
# Commit Job
# ----------------------------------

job.commit()

logger.info("Glue Job Completed Successfully")