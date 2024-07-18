from delta import *
from delta.tables import DeltaTable
import logging
import os
import pyspark
# from pyspark.sql.functions import *
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

LOG.info("Reading Delta table...")
deltaTable = DeltaTable.forPath(spark, delta_table_path)
deltaTable.toDF().orderBy("id").show()

LOG.info("Generating new data to add to Delta table...")
newData = generate_data(start_id=6, end_id=10)

# Convert to DataFrame
newData = spark.createDataFrame(newData, schema=schema)
newData.orderBy("id").show()

LOG.info("Upserting (merging) new data...")
deltaTable.alias("oldData") \
.merge(
    newData.alias("newData"),
    "oldData.id = newData.id") \
.whenMatchedUpdate(set = \
    { \
        "id": pyspark.sql.functions.col("newData.id"),
        "transaction_date": pyspark.sql.functions.col("newData.transaction_date"),
        "amount": pyspark.sql.functions.col("newData.amount")
    }
) \
.whenNotMatchedInsert(values = \
    { \
        "id": pyspark.sql.functions.col("newData.id"),
        "transaction_date": pyspark.sql.functions.col("newData.transaction_date"),
        "amount": pyspark.sql.functions.col("newData.amount")
    }
) \
.execute()

LOG.info("Showing merged Delta table...")
deltaTable.toDF().orderBy("id").show()

LOG.info("Displaying history...")
deltaTable.history().show()