from pyspark.sql.functions import when, col
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType


def clean_column_data(df_pin, columns):
    for col in columns:
        df_pin = df_pin.withColumn(col, F.when(
            (F.col(col) == "") | (F.col(col).isNull()) | (F.col(col).rlike("Error|No|N/A")), None).otherwise(F.col(col)))
    return df_pin

columns_to_clean_pin = ["_corrupt_record", "category", "description", "downloaded", "title", "follower_count", "image_src", "index", "save_location", "is_image_or_video", "poster_name", "tag_list", "unique_id"]

df_pin = clean_column_data(df_pin, columns_to_clean_pin)

df_pin = df_pin.withColumn("follower_count", F.regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", F.col("follower_count").cast(IntegerType()))

df_pin = df_pin.withColumn("save_location", F.regexp_extract("save_location", r'Local save in (.+)', 1))

df_pin = df_pin.withColumnRenamed("index", "ind")

df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

df_pin.show(15)

# task 2
from pyspark.sql.functions import array, col, to_timestamp

df_geo = df_geo.withColumn('coordinates', array('latitude', 'longitude'))

df_geo = df_geo.drop('latitude').drop('longitude')

df_geo = df_geo.withColumn('timestamp', to_timestamp(col('timestamp')))

df_geo = df_geo.select('ind', 'country', 'coordinates', 'timestamp')

df_geo.show()

# task 3
from pyspark.sql.functions import concat_ws, col, to_timestamp
df_user = df_user.withColumn('user_name', concat_ws(' ', col('first_name'), col('last_name')))
df_user = df_user.drop('first_name').drop('last_name')
df_user = df_user.withColumn('date_joined', to_timestamp(col('date_joined')))
df_user = df_user.select('ind', 'user_name', 'age', 'date_joined')

df_user.show()

# task 4
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Pinterest Category Analysis") \
    .getOrCreate()

df_joined = df_pin.join(df_geo, on='ind')

window_spec = Window.partitionBy('country').orderBy(col('category_count').desc())

df_category_counts = df_joined.groupBy('country', 'category') \
    .agg(count('*').alias('category_count')) \
    .withColumn('rank', rank().over(window_spec)) \
    .filter(col('rank') == 1) \
    .drop('rank')

df_category_counts.select('country', 'category', 'category_count').show()

# task 5
from pyspark.sql.functions import year

df_joined = df_joined.withColumn('post_year', year(col('timestamp')))
df_filtered = df_joined.filter((col('post_year') >= 2018) & (col('post_year') <= 2022))

window_spec_year = Window.partitionBy('post_year').orderBy(col('category_count').desc())

df_yearly_category_counts = df_filtered.groupBy('post_year', 'category') \
    .agg(count('*').alias('category_count')) \
    .withColumn('rank', rank().over(window_spec_year)) \
    .filter(col('rank') == 1) \
    .drop('rank')

df_category_post_counts = df_filtered.groupBy('post_year', 'category') \
    .agg(count('*').alias('category_count')) \
    .orderBy('post_year', 'category')

df_yearly_category_counts.select('post_year', 'category', 'category_count').show()
df_category_post_counts.select('post_year', 'category', 'category_count').show()
