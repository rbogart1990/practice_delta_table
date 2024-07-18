from delta import *
import logging
import os
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType

from utils import generate_data

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create logger object:
LOG = logging.getLogger(os.path.basename(__file__))

LOG.info("Creating Spark connection...")
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define schema for the table
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("amount", DecimalType(10, 2), False)
])

# Path to save the Delta table
delta_table_path = "/tmp/delta-table"


LOG.info("Create Spark DataFrame and save as Delta Table")
# Generate data
LOG.info("Generating data...")
data = generate_data(start_id=1, end_id=5)

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Remove existing Delta table if it exists
if os.path.exists(delta_table_path):
    LOG.info(f"Removing existing Delta table at {delta_table_path}...")
    spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).delete(
        spark._jvm.org.apache.hadoop.fs.Path(delta_table_path), True
    )

# Save DataFrame as Delta table
LOG.info("Saving DataFrame as Delta table...")
df.write.format("delta").mode("overwrite").save(delta_table_path)

# Read the Delta table and show the records
LOG.info("Reading and displaying the Delta table...")
df = spark.read.format("delta").load(delta_table_path)
df.orderBy("id").show()

LOG.info("Getting history...")
deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.history().show()

# Stop the Spark session
LOG.info("Stopping Spark session...")
spark.stop()