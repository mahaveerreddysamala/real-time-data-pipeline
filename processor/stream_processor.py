"""PySpark Structured Streaming example for transaction events."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType()),
])


def build_stream(broker="localhost:9092", topic="transactions"):
    spark = SparkSession.builder.appName("TransactionStream").getOrCreate()
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", broker)
           .option("subscribe", topic).option("startingOffsets", "latest").load())
    events = raw.select(from_json(col("value").cast("string"), SCHEMA).alias("event")).select("event.*")
    return events.withColumn("event_time", to_timestamp("event_time")).dropDuplicates(["event_id"])


if __name__ == "__main__":
    stream = build_stream()
    query = stream.writeStream.format("console").outputMode("append").option("truncate", False).start()
    query.awaitTermination()
