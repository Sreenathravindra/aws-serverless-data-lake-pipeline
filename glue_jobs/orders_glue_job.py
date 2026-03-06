import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Read job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_PATH','OUTPUT_PATH'])

# Initialize Spark + Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

print("===== JOB STARTED =====")

print("Reading data from:", input_path)

df = spark.read.option("header","true").csv(input_path)

print("Schema:")
df.printSchema()

print("Sample data:")
df.show(5)

row_count = df.count()

print("Total rows read:", row_count)

if row_count == 0:
    print("⚠️ No data found in input path!")
else:
    print("Transforming data...")

    df = df.withColumn("amount", col("amount").cast("double")) \
           .withColumn("quantity", col("quantity").cast("int"))

    print("Writing parquet to:", output_path)

    df.write \
        .mode("append") \
        .parquet(output_path)

    print("Write completed!")

job.commit()

print("===== JOB COMPLETED =====")