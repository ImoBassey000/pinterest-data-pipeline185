from pyspark.sql.functions import *
import urllib

ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

AWS_S3_BUCKET = "user-126a38c82913-bucket"
MOUNT_NAME = "/mnt/new-mount"
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

display(dbutils.fs.ls("/mnt/new-mount/topics/126a38c82913.pin/partition=0/"))

path_pin = "/mnt/new-mount/topics/126a38c82913.pin/partition=0/*json"
path_geo = "/mnt/new-mount/topics/126a38c82913.geo/partition=0/*json"
path_user = "/mnt/new-mount/topics/126a38c82913.user/partition=0/*json"
file_type = "json"
infer_schema = "true"

df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(path_pin)

df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(path_geo)

df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(path_user)