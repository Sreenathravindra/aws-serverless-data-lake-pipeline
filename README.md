# AWS Glue PySpark Batch Data Pipeline (S3 → Glue → Parquet)

## Overview

This project demonstrates a scalable **batch ETL data pipeline using AWS Glue and PySpark** to process raw CSV datasets stored in Amazon S3 and convert them into **optimized partitioned Parquet files** for analytics workloads.

The pipeline follows a **modern data lake architecture**, enabling efficient storage, faster queries, and scalable data processing.

---

## Architecture

```
Raw Data (CSV)
      │
      ▼
Amazon S3 (Raw Layer)
      │
      ▼
AWS Glue PySpark ETL Job
      │
      ▼
Data Cleaning & Transformations
      │
      ▼
Partitioned Parquet Files
      │
      ▼
Amazon S3 (Curated Layer)
```

---

## Features

- Built a **serverless batch ETL pipeline using AWS Glue (PySpark)**
- Processed raw **CSV datasets stored in Amazon S3**
- Implemented **data cleaning and schema casting using Spark DataFrames**
- Converted datasets into **partitioned Parquet format for optimized analytics**
- Designed a **partitioned data lake structure in Amazon S3**
- Automated **ETL script deployment using GitHub Actions CI/CD**
- Automated **batch processing using AWS Glue scheduled triggers**

---

## Tech Stack

| Technology | Purpose |
|---|---|
| AWS Glue | Serverless ETL processing |
| PySpark | Distributed data processing |
| Amazon S3 | Data lake storage |
| Parquet | Columnar storage format |
| GitHub Actions | CI/CD automation |
| Data Lake Architecture | Structured storage layers |

---

## Project Structure

```
aws-glue-pyspark-batch-pipeline
│
├── scripts
│   └── glue_etl_job.py
│
├── data
│   └── sample_orders.csv
│
├── .github
│   └── workflows
│       └── deploy_glue_job.yml
│
└── README.md
```

---

## ETL Pipeline Workflow

1. Raw CSV files are uploaded to **Amazon S3 (raw layer)**

2. **AWS Glue PySpark job** reads the data

3. Transformations performed:
   - Schema casting
   - Data cleaning
   - Column transformations

4. Data is written as **partitioned Parquet files**

5. Output data is stored in **Amazon S3 curated layer**

---

## Example PySpark Transformation

```python
df = spark.read.option("header","true").csv(input_path)

df_clean = (
    df.withColumn("amount", col("amount").cast(DoubleType()))
      .withColumn("quantity", col("quantity").cast(IntegerType()))
      .withColumn("customer_name", trim(col("customer_name")))
)
```

---

## Data Lake Structure

```
s3://data-lake/

raw/
   orders.csv

curated/
   year=2026/
      month=3/
         day=10/
            part-0000.snappy.parquet
```

---

## CI/CD Deployment

The project uses **GitHub Actions** to automate deployment.

Workflow steps:

- Push code to GitHub
- GitHub Actions uploads the updated Glue script to S3
- AWS Glue job runs using the updated script

---

## Future Improvements

- Integrate **AWS Athena for SQL querying**
- Add **AWS Glue Data Catalog**
- Implement **data quality validation**
- Add **incremental data processing**

---

## Author

**R Sreenath**

AWS Data Engineer

GitHub  
https://github.com/Sreenathravindra

LinkedIn  
https://www.linkedin.com/in/r-sreenath-9190b2256
