import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, lower, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import IntegerType, DoubleType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Read job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_PATH','OUTPUT_PATH'])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("===== JOB STARTED =====")

input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

print("Reading CSV from:", input_path)

# Read CSV
df = spark.read.option("header","true").csv(input_path)

print("Schema:")
df.printSchema()

rows = df.count()
print("Total rows:", rows)

# -------------------------
# Transformations
# -------------------------

df_clean = (
    df.withColumn("order_id", col("order_id").cast(IntegerType()))
      .withColumn("amount", col("amount").cast(DoubleType()))
      .withColumn("quantity", col("quantity").cast(IntegerType()))
      .withColumn("customer_name", trim(col("customer_name")))
      .withColumn("product", lower(col("product")))
      .withColumn("processed_timestamp", current_timestamp())
)

# -------------------------
# Partition columns
# -------------------------

df_partitioned = (
    df_clean
    .withColumn("year", year("processed_timestamp"))
    .withColumn("month", month("processed_timestamp"))
    .withColumn("day", dayofmonth("processed_timestamp"))
)

print("Writing partitioned parquet to:", output_path)

df_partitioned.write \
    .mode("append") \
    .partitionBy("year","month","day") \
    .parquet(output_path)

print("Write completed successfully")

job.commit()

print("===== JOB COMPLETED =====")