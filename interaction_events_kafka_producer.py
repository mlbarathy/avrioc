from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import random
import time
from datetime import datetime
import logging
from typing import Dict, Any
import signal
from dataclasses import dataclass
import queue
from threading import Thread, Event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Configuration for Kafka Producer"""
    bootstrap_servers: str
    topic: str
    batch_size: int = 100
    queue_buffering_max_messages: int = 100000
    compression_type: str = 'snappy'
    retries: int = 3
    retry_backoff_ms: int = 100
    linger_ms: int = 100


class KafkaMessageProducer:
    def __init__(self, config: KafkaConfig):
        """Initialize the Kafka producer with configuration"""
        self.config = config
        self.stop_event = Event()
        self.message_queue = queue.Queue(maxsize=1000)
        self.message_counter = 0
        self.error_counter = 0
        self.producer = self._create_producer()
        self._ensure_topic_exists()

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _create_producer(self) -> Producer:
        """Create and configure Kafka producer"""
        producer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'queue.buffering.max.messages': self.config.queue_buffering_max_messages,
            'compression.type': self.config.compression_type,
            'retries': self.config.retries,
            'retry.backoff.ms': self.config.retry_backoff_ms,
            'linger.ms': self.config.linger_ms,
            'acks': 'all'  # Ensure durability
        }
        return Producer(producer_config)

    def _ensure_topic_exists(self) -> None:
        """Ensure the Kafka topic exists, create if it doesn't"""
        try:
            admin_client = AdminClient({'bootstrap.servers': self.config.bootstrap_servers})
            topics = admin_client.list_topics().topics

            if self.config.topic not in topics:
                topic_list = [NewTopic(
                    self.config.topic,
                    num_partitions=3,  # Adjust based on your needs
                    replication_factor=1  # Adjust based on your cluster
                )]
                admin_client.create_topics(topic_list)
                logger.info(f"Created topic: {self.config.topic}")
        except Exception as e:
            logger.error(f"Error ensuring topic exists: {str(e)}")
            raise

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback function for message delivery reports"""
        if err is not None:
            self.error_counter += 1
            logger.error(f'Message delivery failed: {err}')
        else:
            self.message_counter += 1
            if self.message_counter % 1000 == 0:
                logger.info(f'Successfully delivered {self.message_counter} messages')

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals"""
        logger.info('Shutdown signal received, stopping producer...')
        self.stop_event.set()

    def generate_interaction(self) -> Dict[str, Any]:
        """Generate a random user interaction"""
        return {
            "user_id": random.randint(1, 20),
            "item_id": random.randint(1, 5),
            "interaction_type": random.choice(["click", "view", "purchase"]),
            "timestamp": datetime.utcnow().isoformat()
        }

    def _producer_thread(self) -> None:
        """Thread for producing messages in batches"""
        batch = []
        last_flush_time = time.time()

        while not self.stop_event.is_set() or not self.message_queue.empty():
            try:
                # Get message from queue with timeout
                try:
                    message = self.message_queue.get(timeout=0.1)
                    batch.append(message)
                except queue.Empty:
                    continue

                # Send batch if it reaches batch size or time threshold
                current_time = time.time()
                if (len(batch) >= self.config.batch_size or
                        current_time - last_flush_time >= 1):  # Flush every second

                    for data in batch:
                        self.producer.produce(
                            self.config.topic,
                            key=str(data["user_id"]),
                            value=json.dumps(data),
                            callback=self._delivery_callback
                        )

                    self.producer.poll(0)  # Trigger delivery reports
                    batch = []
                    last_flush_time = current_time

            except Exception as e:
                logger.error(f"Error in producer thread: {str(e)}")
                self.error_counter += 1

    def produce_messages(self) -> None:
        """Main method to start producing messages"""
        try:
            # Start producer thread
            producer_thread = Thread(target=self._producer_thread)
            producer_thread.start()

            start_time = time.time()
            while not self.stop_event.is_set():
                # Generate and queue message
                data = self.generate_interaction()
                self.message_queue.put(data)

                # Log statistics periodically
                if self.message_counter % 1000 == 0:
                    elapsed_time = time.time() - start_time
                    rate = self.message_counter / elapsed_time
                    logger.info(
                        f"Statistics: Messages={self.message_counter}, "
                        f"Errors={self.error_counter}, "
                        f"Rate={rate:.2f} msgs/sec"
                    )

                time.sleep(5)  # Small delay to prevent CPU overuse

        except Exception as e:
            logger.error(f"Error in message production: {str(e)}")
            self.stop_event.set()

        finally:
            # Cleanup
            self.stop_event.set()
            producer_thread.join()
            self.producer.flush(timeout=10)
            self.producer.close()

            logger.info(
                f"Producer stopped. Total messages sent: {self.message_counter}, "
                f"Errors: {self.error_counter}"
            )


def main():
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="user_interactions",
        batch_size=100,
        queue_buffering_max_messages=100000,
        compression_type='snappy',
        retries=3,
        linger_ms=100
    )

    producer = KafkaMessageProducer(config)
    producer.produce_messages()


if __name__ == "__main__":
    main()