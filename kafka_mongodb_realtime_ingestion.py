from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import logging
from typing import Optional


class KafkaMongoStreamer:
    def __init__(self, config: dict):
        """Initialize the streamer with configuration parameters."""
        self.config = config
        self.spark: Optional[SparkSession] = None
        self.setup_logging()

    @staticmethod
    def setup_logging():
        """Configure logging for the application."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_mongo_uri(self) -> str:
        """Construct MongoDB URI with authentication."""
        return (f"mongodb+srv://{self.config['mongo_user']}:{self.config['mongo_password']}@"
                f"{self.config['mongo_host']}/?retryWrites=true&w=majority&appName=Cluster0")

    def create_spark_session(self) -> None:
        """Create and configure Spark session with optimized settings."""
        mongo_uri = self.get_mongo_uri()

        self.spark = SparkSession.builder \
            .appName("KafkaMongoStreamer") \
            .config("spark.mongodb.output.uri", f"{mongo_uri}.{self.config['mongo_collection']}") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
                    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.sql.streaming.checkpointLocation", self.config['checkpoint_path']) \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        logging.info("Spark session created successfully")

    def get_schema(self) -> StructType:
        """Define the schema for the incoming data."""
        return StructType() \
            .add("user_id", IntegerType()) \
            .add("item_id", IntegerType()) \
            .add("interaction_type", StringType()) \
            .add("timestamp", TimestampType())

    def write_to_mongo(self, batch_df, batch_id):
        """Write batch to MongoDB with error handling."""
        try:
            # Add processing timestamp
            enriched_df = batch_df.withColumn("processed_at", current_timestamp())

            enriched_df.write \
                .format("mongo") \
                .mode("append") \
                .option("database", self.config['mongo_db']) \
                .option("collection", self.config['mongo_collection']) \
                .save()

            logging.info(f"Successfully wrote batch {batch_id} to MongoDB")
        except Exception as e:
            logging.error(f"Error writing batch {batch_id} to MongoDB: {str(e)}")
            raise

    def start_streaming(self):
        """Start the streaming process with error handling."""
        try:
            if not self.spark:
                self.create_spark_session()

            # Create streaming DataFrame
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config['kafka_bootstrap_servers']) \
                .option("subscribe", self.config['kafka_topic']) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", 10000) \
                .load()

            # Parse JSON data
            df_parsed = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), self.get_schema()).alias("data")) \
                .select("data.*")

            # Start streaming query
            query = df_parsed.writeStream \
                .foreachBatch(self.write_to_mongo) \
                .trigger(processingTime='1 minute') \
                .start()

            logging.info("Streaming query started successfully")
            query.awaitTermination()

        except Exception as e:
            logging.error(f"Error in streaming process: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Main entry point of the application."""
    config = {
        'mongo_user': "lakshmibarathy",
        'mongo_password': "oUuou4nP2Ea0nJQm",
        'mongo_host': "cluster0.ttx1h.mongodb.net",
        'mongo_db': "avrioc_usecase",
        'mongo_collection': "user_interactions",
        'kafka_bootstrap_servers': "localhost:9092",
        'kafka_topic': "user_interactions",
        'checkpoint_path': "/tmp/kafka_checkpoint"
    }

    streamer = KafkaMongoStreamer(config)
    streamer.start_streaming()


if __name__ == "__main__":
    main()