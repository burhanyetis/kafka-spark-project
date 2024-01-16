from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("RawToProcessedBatch").getOrCreate()

# Define HDFS paths
hdfs_raw_path = "hdfs://user/projects/kafka-spark-project/raw_data.parquet"  # RAW Zone HDFS path
hdfs_processed_path = "hdfs://user/projects/kafka-spark-project/processed_data.parquet"  # Processed Zone HDFS path

try:
    # Read data from RAW Zone
    #raw_data = spark.read.parquet(hdfs_raw_path)
    raw_data = spark.read.parquet(hdfs_raw_path)

    # Perform any additional data transformations or processing here if needed
    # For example, you can filter, aggregate, or transform the data as required
    processed_data = raw_data.filter(col("genre") == "Fantasy")

    # Add a timestamp column to track when the data was processed
    processed_data = processed_data.withColumn("processing_time", col("date").cast("timestamp"))

    # Write the processed data to the Processed Zone in Parquet format
    #processed_data.write.mode("overwrite").parquet(hdfs_processed_path)
    processed_data.write.mode("overwrite").parquet(hdfs_processed_path)

    # Log the successful execution
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("/user/projects/kafka-spark-project/logs/processing.log", "a") as log_file:
        log_file.write(f"Processed data at {current_time}\n")

except Exception as e:
    # Handle exceptions
    error_message = str(e)
    with open("/user/projects/kafka-spark-project/logs/error.log", "a") as error_file:
        error_file.write(f"Error: {error_message}\n")

# Stop the Spark session
spark.stop()