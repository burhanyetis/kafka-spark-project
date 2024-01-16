from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, trim, row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.window import Window
import configparser

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaXMLConsumer").getOrCreate()

# Load Kafka configuration
config = configparser.ConfigParser()
config.read("config/kafka_config.properties")

kafka_server = config["Kafka"]["kafka.bootstrap.servers"]
kafka_topic = config["Kafka"]["kafka.topic"]

# Define HDFS Parquet path for RAW Zone
hdfs_raw_path = "hdfs://user/projects/kafka-spark-project/raw_data.parquet"

# Define the schema for the incoming XML data (customize as needed)
schema = StructType([
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("date", StringType(), True)
])

try:
    # Read XML data from Kafka using Structured Streaming Kafka XML
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "topic1") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("minPartitions", "10") \
        .option("mode", "PERMISSIVE") \
        .option("truncate", False) \
        .load()

    # Print DF
    df.printSchema()

    # Schema Validation: Ensure the schema matches the expected structure
    df = df.selectExpr(
        "CAST(title AS STRING) as title",
        "CAST(author AS STRING) as author",
        "CAST(genre AS STRING) as genre",
        "CAST(price AS DOUBLE) as price",
        "CAST(date AS DOUBLE) as date"
    )

    # Data Type Validation: Ensure data types are correct
    df = df.filter(
        col("price").cast(DoubleType()).isNotNull() &
        col("title").cast(StringType()).isNotNull() &
        col("author").cast(StringType()).isNotNull() &
        col("genre").cast(StringType()).isNotNull() &
        col("date").cast(StringType()).isNotNull()
    )

    # Data formatting (trim): Remove leading and trailing spaces
    df = df.withColumn("title", trim(col("title")))
    df = df.withColumn("author", trim(col("author")))
    df = df.withColumn("genre", trim(col("genre")))

    # Define a window specification for windowed aggregation
    window_spec = Window.partitionBy("genre").orderBy("price").rowsBetween(-2, 0)
    # Perform windowed aggregation, for example, calculate a rolling sum of 'price' over a 3-row window
    df = df.withColumn("rolling_sum", sum(col("price")).over(window_spec))

    # De-duplication: Assuming a unique key (e.g., message ID or timestamp) is present in the XML data
    window_spec = Window.partitionBy("author").orderBy("date")
    df = df.withColumn("row_number", row_number().over(window_spec))
    df = df.filter(col("row_number") == 1).drop("row_number")

    # Write the processed data to HDFS Parquet in RAW Zone
    query = df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .trigger(processingTime='1 second') \
        .option("checkpointLocation", "/user/projects/kafka-spark-project/logs/checkpoint_location") \
        .partitionBy("date") \
        .queryName("KafkaXMLQuery") \
        .start("/user/projects/kafka-spark-project/data/raw_output") \
        .awaitTermination()

    # Wait for the query to terminate
    query.awaitTermination()

except Exception as e:
    # Handle exceptions
    error_message = str(e)
    with open("/user/projects/kafka-spark-project/logs/error.log", "a") as error_file:
        error_file.write(f"Error: {error_message}\n")

# Stop the Spark session
spark.stop()
