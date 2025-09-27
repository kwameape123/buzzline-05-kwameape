# kafka_emitter.py

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
from utils.utils_logger import logger

# -------------------------------------------------
# Define basic Kafka configuration
# -------------------------------------------------
BOOTSTRAP_SERVERS = 'localhost:9092'  # Change if your Kafka is on a different host

# -------------------------------------------------
# Create topic if it doesn't exist
# -------------------------------------------------
def create_topic(topic_name: str) -> None:
    """Create a Kafka topic if it doesn't exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topic = NewTopic(
        name=topic_name,
        num_partitions=3,
        replication_factor=1
    )
    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.info(f"Topic not created:{e}")
    finally:
        admin_client.close()

# -------------------------------------------------
# Define a Kafka Producer
# -------------------------------------------------
def get_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# -------------------------------------------------
# Send messages to the topic
# -------------------------------------------------
def send_messages(producer: KafkaProducer, topic_name: str, message: dict) -> None:
    """Send a single message to the given Kafka topic."""
    try:
        producer.send(topic_name, value=message)
        producer.flush()
        logger.info(f"Sent to topic '{topic_name}': {message}")
    except Exception as e:
        logger.error(f"[Kafka_emitter] Failed to send to '{topic_name}': {e}")
