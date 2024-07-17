import logging
import os
import pyspark
from delta import *
from datetime import datetime, timedelta
import random
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType

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
# schema = "id INT, transaction_date DATE, amount DECIMAL(10, 2)"
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("amount", DecimalType(10, 2), False)
])

# Generate data
LOG.info("Generating data...")
today = datetime.now()
data = []

for i in range(1, 6):
    id = i
    transaction_date = today - timedelta(days=random.randint(0, 365))
    amount = Decimal(round(random.uniform(10.0, 1000.0), 2))
    data.append((id, transaction_date, amount))

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.show()

# Path to save the Delta table
delta_table_path = "/tmp/delta-table"

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
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

LOG.info("Getting history...")
deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
history = deltaTable.history()
LOG.info(f"history.count: \n {history.count}")
# LOG.info(f"history: {history}")
# LOG.info(f"type(history): {type(history)}")
# history.select("version", "timestamp", "operation", "operationParameters", "userMetadata").display()

# Stop the Spark session
LOG.info("Stopping Spark session...")
spark.stop()