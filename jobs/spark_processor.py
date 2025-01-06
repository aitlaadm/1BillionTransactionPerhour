from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *



KAFKA_BROKERS="localhost:29092,localhost:39092,localhost:49092"
SOURCE_TOPIC='financial_transactions'
AGGREGATES_TOPIC='transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR='mnt/spark-checkpoints'
STATES_DIR='mnt/spark-state'


spark = (SparkSession.builder
         .appName('FinancialTransactionsProcessor')
         .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-010_2.12:3.5.0')
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
         .config('spark.sql.streaming.stateStore.stateStoreDir', STATES_DIR)
         .config('spark.sql.shuffle.partitions',20)).getOrCreate()

spark.sparkContext.setLoglevel("WARN")

transaction_schema= StructType([
        StructField("transactionId",StringType(), False),
        StructField("userId",StringType(), False),
        StructField("amount",DoubleType(), True),
        StructField("transactionTime",LongType(), True),
        StructField("merchantId",StringType(), False),
        StructField("transactionType",StringType(), True),
        StructField("location",StringType(), True),
        StructField("paymentMethod",StringType(), True),
        StructField("isInternational",StringType(), True),
        StructField("currency",StringType(), True),
])

#Read data from Kafka

kafka_stream = (spark.readStream
                .format("kafka")
                .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                .option('subscribe', SOURCE_TOPIC)
                .option('startingOffset', 'earliest')).load()

transaction_df= kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col('value'), transaction_schema).alias("data")) \
    .select("data.*")
    
transaction_df = transaction_df.withColumn("TransactionDate",to_timestamp(from_unix(col('transationTime',"yyyy-MM-dd:HH:mm:ss"))))

aggregated_df=transaction_df.groupby("marchentId") \
.agg(
    sum("amount").alias("totalAmount"),
    count("*").alias("TransactionsCount")
)

