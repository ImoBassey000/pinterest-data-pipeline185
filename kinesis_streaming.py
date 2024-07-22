from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)


# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

%sql
-- Disable format checks during the reading of Delta tables
SET spark.databricks.delta.formatCheck.enabled=false

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-126a38c82913-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-126a38c82913-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-126a38c82913-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

df_pin = df_pin.selectExpr("CAST(data as STRING)")
df_geo = df_geo.selectExpr("CAST(data as STRING)")
df_user = df_user.selectExpr("CAST(data as STRING)")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

spark = SparkSession.builder.appName("JsonToDataFrame").getOrCreate()


schema_user = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", TimestampType(), True)
])

schema_geo = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("country", StringType(), True)
])

schema_pin = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", IntegerType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])

df_user = df_user.select(from_json("data", schema_user).alias("data")).select("data.*")
df_geo = df_geo.select(from_json("data", schema_geo).alias("data")).select("data.*")
df_pin = df_pin.select(from_json("data", schema_pin).alias("data")).select("data.*")

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

def write_stream(dataframe, checkpoint, table_name):
    dataframe.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint) \
        .toTable(table_name)

# Assuming df is a streaming DataFrame
write_stream(df, "/tmp/kinesis/_checkpoints/df_user/", "126a38c82913_user_table")
write_stream(df, "/tmp/kinesis/_checkpoints/df_pin/", "126a38c82913_pin_table")
write_stream(df, "/tmp/kinesis/_checkpoints/df_geo/", "126a38c82913_geo_table")