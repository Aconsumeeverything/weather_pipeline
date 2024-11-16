from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, when, lit, date_format, udf
import uuid

# Create a Spark session with necessary connectors
spark = SparkSession.builder \
    .appName("WeatherConsumer") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Configure Kafka connection
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Define the weather data schema
schema = schema_of_json('''{
    "location": {
        "name": "", 
        "region": "", 
        "country": "", 
        "lat": 0.0, 
        "lon": 0.0, 
        "tz_id": "", 
        "localtime_epoch": 0, 
        "localtime": ""
    }, 
    "current": {
        "last_updated_epoch": 0, 
        "last_updated": "", 
        "temp_c": 0.0, 
        "temp_f": 0.0, 
        "is_day": 0, 
        "condition": {
            "text": "", 
            "icon": "", 
            "code": 0
        }, 
        "wind_mph": 0.0, 
        "wind_kph": 0.0, 
        "wind_degree": 0, 
        "wind_dir": "", 
        "pressure_mb": 0.0, 
        "pressure_in": 0.0, 
        "precip_mm": 0.0, 
        "precip_in": 0.0, 
        "humidity": 0, 
        "cloud": 0, 
        "feelslike_c": 0.0, 
        "feelslike_f": 0.0, 
        "vis_km": 0.0, 
        "vis_miles": 0.0, 
        "uv": 0.0, 
        "gust_mph": 0.0, 
        "gust_kph": 0.0
    }
}''')

# Transform Kafka data
weather_data = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data"))

# Extract and transform weather data
weather_info = weather_data.select(
    col("data.location.name").alias("location_name"),
    col("data.location.region").alias("region"),
    col("data.location.country").alias("country"),
    col("data.location.lat").alias("latitude"),
    col("data.location.lon").alias("longitude"),
    col("data.location.localtime").alias("localtime"),
    col("data.current.temp_c").alias("temperature_c"),
    col("data.current.temp_f").alias("temperature_f"),
    col("data.current.condition.text").alias("condition_text"),
    col("data.current.condition.icon").alias("condition_icon"),
    col("data.current.humidity").alias("humidity"),
    col("data.current.wind_mph").alias("wind_mph"),
    col("data.current.wind_kph").alias("wind_kph"),
    col("data.current.pressure_mb").alias("pressure_mb"),
    col("data.current.feelslike_c").alias("feelslike_c"),
    col("data.current.feelslike_f").alias("feelslike_f"),
    col("data.current.is_day").alias("is_day"),
    col("data.current.last_updated").alias("last_updated"),
    col("data.current.precip_mm").alias("precipitation_mm")
)

# Add additional transformations
transformed_weather_info = weather_info \
    .withColumn("formatted_time", date_format(col("localtime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("day_or_night", when(col("is_day") == 1, lit("Day")).otherwise(lit("Night"))) \
    .withColumn("wind_intensity", when(col("wind_kph") > 50, lit("High Wind")).otherwise(lit("Normal Wind"))) \
    .withColumn("humidity_level", when(col("humidity") > 80, lit("High")).otherwise(lit("Normal")))

# Conditional logic for irrigation
transformed_weather_info = transformed_weather_info \
    .withColumn("needs_irrigation", when(
        (col("temperature_c").between(5, 15)) & 
        (col("humidity") < 60) & 
        (col("precipitation_mm") < 1),
        lit("Yes")
    ).when(
        (col("wind_kph") > 30) & 
        (col("precipitation_mm") < 2),
        lit("Yes")
    ).when(
        (col("temperature_c") > 25) & 
        (col("humidity") < 50) & 
        (col("precipitation_mm") < 2),
        lit("Yes")
    ).otherwise(lit("No")))

# Add a UUID column
generate_uuid = udf(lambda: str(uuid.uuid4()))
transformed_weather_info = transformed_weather_info.withColumn("id", generate_uuid())

# Filter data for France and write to Cassandra
filtered_weather_info = transformed_weather_info.filter(col("country") == "France")

cassandra_query = filtered_weather_info.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .outputMode("append") \
    .option("keyspace", "weather_keyspace") \
    .option("table", "weather_data") \
    .option("checkpointLocation", "/tmp/checkpoints/weather_data") \
    .start()

# Wait for termination
cassandra_query.awaitTermination()
