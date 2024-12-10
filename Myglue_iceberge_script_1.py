import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# AWS Glue Job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set paths and catalog
warehouse_path = 's3://my-bucket-ice/iceberg_data_2/'
catalog_nm = 'glue_catalog'
database_op = "my_database_2"
table_op = "iceberg_table_2"

# Initialize Spark session with Iceberg configurations
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()

# Sample data for insertion
data = [
    (1, 'Alice', 30, 'alice@example.com', '2024-11-19 14:00:00'),
    (2, 'Bob', 25, 'bob@example.com', '2024-11-19 15:00:00'),
    (3, 'Charlie', 35, 'charlie@example.com', '2024-11-20 14:00:00'),
    (4, 'David', 40, 'david@example.com', '2024-11-21 15:00:00'),
]

schema = ['id', 'name', 'age', 'email', 'created_at'] 
df = spark.createDataFrame(data, schema)

# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True),
#     StructField("email", StringType(), True),
#     StructField("created_at", StringType(), True),
# ])

df = spark.createDataFrame(data, schema)

# Cast `created_at` to `TimestampType` using PySpark's `withColumn`
df = df.withColumn("created_at", df["created_at"].cast("timestamp"))

# Write the data to the Iceberg table
df.write \
    .format("iceberg") \
    .mode("append") \
    .save(f"{catalog_nm}.{database_op}.{table_op}")

# Show the data
df.show()

job.commit()
