#!/bin/bash

# Load Spark configuration from the properties file
SPARK_CONFIG_FILE="/user/projects/kafka-spark-project/config/spark_config.properties"
if [ ! -f "$SPARK_CONFIG_FILE" ]; then
  echo "Spark config file not found: $SPARK_CONFIG_FILE"
  exit 1
fi

# Load the Spark application name, master, and additional Spark configuration from the properties file
SPARK_APP_NAME="$(grep 'spark.app.name' $SPARK_CONFIG_FILE | cut -d'=' -f2)"
SPARK_MASTER="$(grep 'spark.master' $SPARK_CONFIG_FILE | cut -d'=' -f2)"
SPARK_CONF_OPTIONS="$(grep 'spark.conf.options' $SPARK_CONFIG_FILE | cut -d'=' -f2)"
SPARK_SCRIPT="$(grep 'spark.script.path' $SPARK_CONFIG_FILE | cut -d'=' -f2)"
SPARK_CONFIG_DIR="$(grep 'spark.config.dir' $SPARK_CONFIG_FILE | cut -d'=' -f2)"

# Construct the Spark submit command
SPARK_SUBMIT_COMMAND="$SPARK_HOME/bin/spark-submit \\
  --master $SPARK_MASTER \\
  --name '$SPARK_APP_NAME' \\
  --py-files $SPARK_CONFIG_DIR \\
  --files '$SPARK_CONFIG_DIR/spark_config.properties' \\
  $SPARK_CONF_OPTIONS \\
  $SPARK_SCRIPT"

echo "Spark Submit Command: $SPARK_SUBMIT_COMMAND"
eval $SPARK_SUBMIT_COMMAND
exit $?