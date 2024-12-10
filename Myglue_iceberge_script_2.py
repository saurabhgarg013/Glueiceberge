import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job

# S3 and Glue Catalog Setup
warehouse_path = 's3://my-bucket-ice/iceberg_data/'
catalog_nm = 'glue_catalog'

# Initialize Spark Session with Iceberg Catalog configuration
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# AWS Glue Job Arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize GlueContext with the SparkContext
ibsc = spark.sparkContext
ibglueContext = GlueContext(ibsc)
ibjob = Job(ibglueContext)
ibjob.init(args["JOB_NAME"], args)

# Input and Output Table Information
in_database = "my_database"
in_table_name = "csv_table"
database_op = 'my_database'
table_op = 'iceberg_table'

print(f"\nINPUT Database : {in_database}")
print(f"\nINPUT Table : {in_table_name}")
print(f"\nOUTPUT Iceberg Database : {database_op}")
print(f"\nOUTPUT Iceberg Table : {table_op}")

# Read the Glue input table as a DynamicFrame
InputDynamicFrameTable = ibglueContext.create_dynamic_frame.from_catalog(database=in_database, table_name=in_table_name)

# Convert the DynamicFrame to a Spark DataFrame
InputDynamicFrameTable_DF = InputDynamicFrameTable.toDF()

# Register the DataFrame as a TempView
InputDynamicFrameTable_DF.createOrReplaceTempView("InputDataFrameTable")
spark.sql("SELECT * FROM InputDataFrameTable LIMIT 10").show()

# Filter the DataFrame for rows where age < 30
colname_df = spark.sql("SELECT * FROM InputDataFrameTable WHERE age < 30")
colname_df.createOrReplaceTempView("OutputDataFrameTable")

# Write the filtered data to an Iceberg table
ib_Write_SQL = f"""
    CREATE OR REPLACE TABLE {catalog_nm}.{database_op}.{table_op}
    USING iceberg
    TBLPROPERTIES ("format-version"="2", "write_compression"="gzip")
    AS SELECT * FROM OutputDataFrameTable;
    """

# Run the Spark SQL query
spark.sql(ib_Write_SQL)

spark.sql(f"SELECT * FROM {catalog_nm}.`{database_op}`.{table_op}")

update_sql = f"""
    UPDATE {catalog_nm}.{database_op}.{table_op}
    SET age = 55
    WHERE id = 6
"""

# Execute the update command
spark.sql(update_sql)

spark.sql(f""" ALTER TABLE {catalog_nm}.{database_op}.{table_op} ADD COLUMN department STRING """)

history_df = spark.sql(f"""
SELECT * FROM {catalog_nm}.{database_op}.{table_op}.history LIMIT 5
""")
history_df.show()

# Fetch the latest snapshot ID
latest_snapshot_df = spark.sql(f"SELECT snapshot_id FROM {catalog_nm}.{database_op}.{table_op}.history ORDER BY snapshot_id DESC LIMIT 1")
latest_snapshot_id = latest_snapshot_df.collect()[0]['snapshot_id']
print(latest_snapshot_id)

# Perform the time travel query using the latest snapshot ID
latest_snapshot_query = f"""
    SELECT * FROM {catalog_nm}.{database_op}.{table_op} FOR VERSION AS OF {latest_snapshot_id}
"""
spark.sql(latest_snapshot_query).show()


###time travel

# Fetch the latest timestamp
latest_timestamp_df = spark.sql(f"SELECT made_current_at FROM {catalog_nm}.{database_op}.{table_op}.history ORDER BY made_current_at DESC LIMIT 1")
latest_timestamp = latest_timestamp_df.collect()[0]['made_current_at']

print(latest_timestamp)


# Perform the time travel query using the latest timestamp
time_travel_timestamp_query = f"""
    SELECT * FROM {catalog_nm}.{database_op}.{table_op} FOR SYSTEM_TIME AS OF '{latest_timestamp}'
"""
spark.sql(time_travel_timestamp_query).show()







