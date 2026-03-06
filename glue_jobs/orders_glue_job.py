import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME','INPUT_PATH','OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

print("Reading data from:", input_path)

df = spark.read.option("header","true").csv(input_path)

print("Total rows:", df.count())

df.show(5)

# Cast columns
df = df.withColumn("amount", col("amount").cast("double"))
df = df.withColumn("quantity", col("quantity").cast("int"))

print("Writing data to:", output_path)

df.write.mode("overwrite").parquet(output_path)

job.commit()

print("Job completed successfully")