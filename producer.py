producer :
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, udf
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import expr
from kafka import KafkaProducer
import json

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Music Recommendation System") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mfcc_database.mfcc_collection") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mfcc_database.mfcc_collection") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Kafka producer setup
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Load data from MongoDB into DataFrame
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Extract 'track_id' column from DataFrame and store in list
names = df.select("track_id").rdd.flatMap(lambda x: x).collect()

# Data Preprocessing
df = df.withColumn("genre_all", split(regexp_replace(col("genre_all"), r"[\[\]]", ""), ", ").cast(ArrayType(IntegerType())))
df = df.withColumn("mfcc_features", col("mfcc_features").cast(ArrayType(DoubleType())))
mfcc_df = df.select(col("mfcc_features"))

# Define a function to extract the first element of the array and convert to string
first_element_udf = udf(lambda x: str(x[0]) if x else None, StringType())

# Create the new DataFrame
features_df = mfcc_df.withColumn("features", first_element_udf("mfcc_features")).select("features")

# ALS Model
indexer = StringIndexer(inputCol="features", outputCol="features_index")
indexed_df = indexer.fit(features_df).transform(features_df)

# Add a placeholder rating column
indexed_df = indexed_df.withColumn("rating2", col("features_index").cast(IntegerType())) # Convert features_index to IntegerType
indexed_df = indexed_df.withColumn("fasih", col("features").cast(IntegerType())) # Convert features_index to IntegerType
als = ALS(maxIter=10, regParam=0.01, userCol="features_index", itemCol= "rating2" , ratingCol="fasih",
          coldStartStrategy="drop")

model = als.fit(indexed_df)

# Generate top 5 song recommendations for each user
userRecs = model.recommendForAllUsers(5)
# Let's say you want to retrieve recommendations for feature index value 12
feature_index_value = 12

# Filter the DataFrame to get recommendations for the specified feature index value
filtered_recommendations = userRecs.filter(userRecs["features_index"] == feature_index_value).select("recommendations")
# Convert Spark DataFrame to Pandas DataFrame
filtered_recommendations_df = filtered_recommendations.toPandas()

# Extract the recommendations column as a list
recommendations_list = filtered_recommendations_df['recommendations'].iloc[0]

# Extract the first values from the sets in the list
first_values_list = [list(item)[0] for item in recommendations_list]

# Send recommendations to Kafka topic
for i in first_values_list:
    producer.send('music_recommendations', value=names[i])

producer.flush()