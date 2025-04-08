# Databricks notebook source
# MAGIC %md
# MAGIC # Tribunal Decision Archive from Gold to Event Hubs
# MAGIC <table style='float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Name: </b></td>
# MAGIC          <td>ARIA_TD_ADLS_TO_EVENTHUBS</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Description: </b></td>
# MAGIC          <td>Notebook to transfer a set of HTML, JSON, and A360 files, each representing data on Tribunal Decisions stored in ARIA, to Event Hubs.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>First Created: </b></td>
# MAGIC          <td>Oct-2024</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left;'><b>Changelog (JIRA ref / initials / date):</b></th>
# MAGIC          <th>Comments</th>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><a href="https://tools.hmcts.net/jira/browse/ARIADM-xxx">ARIADM-xxx</a> / NSA / OCT-2024</td>
# MAGIC          <td>Tribunal Decision and IRIS: Transfer data from Gold ADLS to Event Hubs</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Binaries
from confluent_kafka import Producer
import json
# from  itertools import islice
# import numpy as np
from pyspark.sql.functions import col, decode, split, element_at
import logging
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext


# COMMAND ----------

# DBTITLE 1,Get EventHub  NamespaceKey
secret = dbutils.secrets.get("ingest00-keyvault-sbox", "ingest00-eventhubns-sharedaccesskey-sbox")

# COMMAND ----------

# DBTITLE 1,Efficient Data Ingestion with Spark and Event Hubs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, element_at
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext

# Initialize Spark session
spark = SparkSession.builder.appName("OptimizedEventHubs").getOrCreate()

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("OptimizedEventHubs") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

sc = spark.sparkContext


# Event Hub configurations
eventhubs_hostname = "sbox-dlrm-eventhub-ns.servicebus.windows.net:9093"   # Event Hubs hostname review
conf = {
    'bootstrap.servers': eventhubs_hostname,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': f'Endpoint=sb://sbox-dlrm-eventhub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={secret}',
    'retries': 5,                     # Increased retries
    'enable.idempotence': True,        # Enable idempotent producer
}

broadcast_conf = sc.broadcast(conf)

# Read and prepare data HTML files
# json_mount = '/mnt/ingest00curatedsboxgold/ARIADM/ARM/TD/'
# json_mount = '/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH/'
json_mount = '/mnt/ingest00curatedsboxgold/ARIADM/ARM/APPEALS/'
binary_df = spark.read.format('binaryFile') \
                     .option('pathGlobFilter', '*.{html,json}') \
                     .option('recursiveFileLookup', 'true') \
                     .load(json_mount)



html_df = binary_df.withColumn("content_str", decode(col('content'), 'utf-8')) \
                   .withColumn('file_path', element_at(split(col('path'), '/'), -1))\
                   .withColumn('file_size_bytes', col('length'))  # Capture file size in bytes
# html_df = html_df.select('content_str','file_path').filter(col('file_path').like('tribunal_decision_NS%'))
html_df = html_df.select('content_str','file_path','file_size_bytes')

# Repartition based on cluster resources
num_spark_partitions =  64
optimized_html_df = html_df.repartition(num_spark_partitions)


# COMMAND ----------

display(optimized_html_df)

# COMMAND ----------

# DBTITLE 1,Spark Streaming with Kafka and Throughput Control
from pyspark.sql.types import StructType, StructField, StringType
import time

# Set throughput limits as parameters (1000 events or 1 MB per second)
Throughput_Units = 20
MAX_EVENTS_PER_SEC = Throughput_Units * 1000
MAX_BYTES_PER_SEC = Throughput_Units * 1024 * 1024  # 1 MB in bytes

def process_partition_with_throughput(partition, throughput_limit=(MAX_EVENTS_PER_SEC, MAX_BYTES_PER_SEC)):
    import logging
    from confluent_kafka import Producer
    from time import time, sleep
    
    # Initialize logger
    logging.basicConfig(level=logging.ERROR)
    logger = logging.getLogger('KafkaProducer')
    failure_list = []
    success_list = []
    
    # Initialize producer with broadcasted config
    producer = Producer(**broadcast_conf.value)
    
    def delivery_report(err, msg):
        if err is not None:
            logger.error(f"Message delivery failed for key {msg.key()}: {err}")
            if msg.key() is not None:
                failure_list.append((msg.key().decode('utf-8'), str(err)))
        else:
            success_list.append((msg.key().decode('utf-8'), "success"))

    # Throughput control variables
    events_sent = 0
    bytes_sent = 0
    start_time = time()

    for row in partition:
        # Check for valid data
        if row.file_path is None or row.content_str is None or row.file_size_bytes is None:
            continue

        # Handle different content_str types
        if isinstance(row.content_str, str):
            value = row.content_str.encode('utf-8')
        elif isinstance(row.content_str, (bytes, bytearray)):
            value = bytes(row.content_str)
        else:
            logger.error(f"Unsupported type for content_str: {type(row.content_str)}")
            failure_list.append((row.file_path, "Unsupported content_str type"))
            continue  # Skip this row

        # Check throughput limits
        current_time = time()
        elapsed_time = current_time - start_time
        if events_sent >= throughput_limit[0] or bytes_sent >= throughput_limit[1]:
            if elapsed_time < 1:
                sleep(1 - elapsed_time)  # Sleep to maintain rate limit
            # Reset counters for the next second
            events_sent = 0
            bytes_sent = 0
            start_time = time()

        retry_count = 0
        success = False
        while retry_count < 3 and not success:
            try:
                producer.produce(
                    topic='evh-bl-pub-dev-uks-dlrm-01',
                    key=row.file_path.encode('utf-8'),
                    value=value,
                    callback=delivery_report
                )
                # Update throughput counters
                events_sent += 1
                bytes_sent += row.file_size_bytes
                success = True
            except BufferError:
                logger.warning("Producer buffer full. Polling for events.")
                producer.poll(1)  # Allow the producer to handle events
                retry_count += 1
            except Exception as e:
                error_message = f"Unexpected error during production: {e}"
                logger.error(error_message)
                failure_list.append((row.file_path, error_message))
                break  # Stop retrying if an unexpected error occurs

    try:
        producer.flush()
        logger.info("Producer flushed successfully.")
    except Exception as e:
        logger.error(f"Unexpected error during flush: {e}")

    # Yield results for success and failure lists with error messages
    for key, message in success_list:
        yield (key, "success", message)
    for key, error in failure_list:
        yield (key, "failure", error)


# Schema for result DataFrame with error message column
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Apply the optimized processing
result_rdd = optimized_html_df.rdd.mapPartitions(
    lambda partition: process_partition_with_throughput(partition, throughput_limit=(MAX_EVENTS_PER_SEC, MAX_BYTES_PER_SEC))
)
result_df = result_rdd.toDF(schema).collect()


# COMMAND ----------

display(result_df)

# COMMAND ----------

final_result_df = spark.createDataFrame(result_df,schema)

failed_files = final_result_df.filter(col("status") == "failure")

# # Display failed files
display(failed_files)
failed_files.count()

# COMMAND ----------

# DBTITLE 1,Processing Optimized DataFrame for Failed Files
if failed_files.count() > 0:
    failed_list = failed_files.select('file_name').collect()
    failed_file_names = [row.file_name for row in failed_list]

    failed_optimized_df = optimized_html_df.filter(col('file_path').isin(failed_file_names))

    second_result_df = failed_optimized_df.rdd.mapPartitions(
    lambda partition: process_partition_with_throughput(partition, throughput_limit=(MAX_EVENTS_PER_SEC, MAX_BYTES_PER_SEC)))

    second_result_df =  second_result_df.toDF(schema).collect()

    display(second_result_df)
else: 
    second_result_df = None


# COMMAND ----------

if second_result_df is not None:
    second_result_df = spark.createDataFrame(second_result_df, schema)
    # if second_result_df.select(col('error_message') != 'success').count() > 0:
    display(second_result_df.filter(col('error_message') != 'success'))

# COMMAND ----------

# KafkaError{code=POLICY_VIOLATION,val=44,str="Broker: Policy violation"}
# KafkaError{code=INVALID_PRODUCER_ID_MAPPING,val=49,str="Broker: Producer attempted to use a producer id which is not currently assigned to its transactional id"}
