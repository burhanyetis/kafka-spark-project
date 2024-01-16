# Import necessary libraries
from pyspark.sql import SparkSession
import configparser

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Load Kafka configuration
config = configparser.ConfigParser()
config.read("config/kafka_config.properties")

kafka_server = config["Kafka"]["kafka.bootstrap.servers"]
kafka_topic = config["Kafka"]["kafka.topic"]

# Define a function to read XML data from a file
def read_xml_data(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Read XML data from a file located one directory above and inside the "data/input" directory
xml_file_path = "/user/projects/kafka-spark-project/data/input/sample.xml"  # Adjust the path as needed
sample_xml_data = read_xml_data(xml_file_path)

# Create a Spark DataFrame with the sample XML data
df = spark.read \
    .format("xml") \
    .option("rowTag", "book") \
    .option("charset", "UTF-8") \
    .load(spark.sparkContext.parallelize([sample_xml_data]))

# Write the DataFrame to Kafka topic
df.selectExpr("CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("topic", kafka_topic) \
    .option("key", "book123") \
    .save()

print(f"Published data to Kafka from file: {xml_file_path}")

# Stop the Spark session
spark.stop()