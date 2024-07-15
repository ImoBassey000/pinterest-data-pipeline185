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

#Task 2
from pyspark.sql.functions import array, col, to_timestamp

# Create a new column 'coordinates' that contains an array of latitude and longitude
df_geo = df_geo.withColumn('coordinates', array('latitude', 'longitude'))

# Drop the latitude and longitude columns
df_geo = df_geo.drop('latitude').drop('longitude')

# Convert the timestamp column from a string to a timestamp data type
df_geo = df_geo.withColumn('timestamp', to_timestamp(col('timestamp')))

# Reorder the DataFrame columns
df_geo = df_geo.select('ind', 'country', 'coordinates', 'timestamp')

df_geo.show()

#Task 3
from pyspark.sql.functions import concat_ws, col, to_timestamp

# Create a new column 'user_name' that concatenates the information found in the 'first_name' and 'last_name' columns
df_user = df_user.withColumn('user_name', concat_ws(' ', col('first_name'), col('last_name')))

# Drop the first_name and last_name columns
df_user = df_user.drop('first_name').drop('last_name')

# Convert the date_joined column from a string to a timestamp data type
df_user = df_user.withColumn('date_joined', to_timestamp(col('date_joined')))

# Reorder the DataFrame columns
df_user = df_user.select('ind', 'user_name', 'age', 'date_joined')

df_user.show()

#Task 4
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Pinterest Category Analysis") \
    .getOrCreate()

# Assuming df_pin and df_geo are your DataFrames containing the Pinterest post data and geolocation data respectively

# Join df_pin and df_geo on 'ind' column
df_pin_geo = df_pin.join(df_geo, on='ind')

# Group by country and category, count occurrences, and add a rank based on category count
window_spec = Window.partitionBy('country').orderBy(col('category_count').desc())

df_category_counts = df_pin_geo.groupBy('country', 'category') \
    .agg(count('*').alias('category_count')) \
    .withColumn('rank', rank().over(window_spec)) \
    .filter(col('rank') == 1) \
    .drop('rank')

df_category_counts.select('country', 'category', 'category_count').show()

#Task 5
from pyspark.sql import functions as F

# Join df_pin and df_geo on the ind column gave us the dataframe (df_pin_geo)

# Extract the year from the timestamp column
df_with_year = df_pin_geo.withColumn('post_year', F.year('timestamp'))

# Filter the data to include only the posts between 2018 and 2022
df_filtered = df_with_year.filter((F.col('post_year') >= 2018) & (F.col('post_year') <= 2022))

# Group by the extracted year and category, and count the number of posts for each category in each year
df_category_count = df_filtered.groupBy('post_year', 'category').agg(F.count('*').alias('category_count'))

# Select the desired columns
df_category_count = df_category_count.select('post_year', 'category', 'category_count')

df_category_count.show()

from pyspark.sql import functions as F
from pyspark.sql.window import Window

#Task 6
# Join df_pin and df_geo on the ind column gave us the dataframe (df_pin_geo)
# Define window specification
window_spec = Window.partitionBy('country').orderBy(F.desc('follower_count'))

# Add a row number to each row within each country partition
df_with_rank = df_pin_geo.withColumn('rank', F.row_number().over(window_spec))

# Filter to keep only the top-ranked row within each country partition
df_top_followers = df_with_rank.filter(F.col('rank') == 1).select('country', 'poster_name', 'follower_count')

df_top_followers.show()

# Find the country with the user who has the most followers
df_most_followed_user = df_top_followers.orderBy(F.desc('follower_count')).limit(1)

# Select the desired columns
df_most_followed_user = df_most_followed_user.select('country', 'follower_count')

df_most_followed_user.show()

#Task 7
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Creating an age group column to define the age range
df_user = df_user.withColumn(
    "age_group", 
    F.when((F.col("age") >= 18) & (F.col("age") <= 24), "18-24")
     .when((F.col("age") >= 25) & (F.col("age") <= 35), "25-35")
     .when((F.col("age") >= 36) & (F.col("age") <= 50), "36-50")
     .when(F.col("age") > 50, "+50")
)

# Joining df_pin with df_user on the ind column
df_pin_user = df_pin.join(df_user, on='ind', how='inner')

# Group by age group and category, and count the number of posts
df_category_count = df_pin_user.groupBy("age_group", "category").count().withColumnRenamed("count", "category_count")

#  Find the most popular category for each age group
window_spec = Window.partitionBy("age_group").orderBy(F.desc("category_count"))
df_with_rank = df_category_count.withColumn("rank", F.row_number().over(window_spec))
df_most_popular_category = df_with_rank.filter(F.col("rank") == 1).select("age_group", "category", "category_count")

df_most_popular_category.show()

#Task 8
from pyspark.sql import functions as F

# Group by age group and calculate the median follower count
df_median_follower_count = df_pin_user.groupBy("age_group").agg(
    F.expr('percentile_approx(follower_count, 0.5)').alias('median_follower_count')
)

df_median_follower_count.show()

#Task 9
from pyspark.sql import functions as F

#  Extract the year from the date_joined column
df_user = df_user.withColumn("post_year", F.year("date_joined"))

#  Filter the DataFrame to include only the users who joined between 2015 and 2020
df_filtered = df_user.filter((F.col("post_year") >= 2015) & (F.col("post_year") <= 2020))

#  Group by year and count the number of users
df_users_joined = df_filtered.groupBy("post_year").count().withColumnRenamed("count", "number_users_joined")

df_users_joined.show()

