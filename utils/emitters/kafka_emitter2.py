from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from utils.utils_logger import logger

# -------------------------------------------------
# Define basic Kafka configuration
# -------------------------------------------------
BOOTSTRAP_SERVERS = 'localhost:9092' # Change if your Kafka is on a different host

# -------------------------------------------------
# Create topic if it doesn't exist
# -------------------------------------------------
def create_topic(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    topic = NewTopic(
    name=topic_name,
    num_partitions=3, # number of partitions for scalability
    replication_factor=1 # must be <= number of brokers
    )

    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    finally:
        admin_client.close()

# -------------------------------------------------
# Define a Kafka Producer
# -------------------------------------------------
def get_producer():
# We serialize data to JSON before sending
    producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

# -------------------------------------------------
# Send messages to the topic
# -------------------------------------------------
def send_messages(producer,topic_name,message):
    producer.send(topic_name, value=message)
    print(f"📤 Sent: {message}")
    time.sleep(1) # just to slow down sending for clarity

    producer.flush() # make sure all messages are sent
    logger.info("All messages sent.")

def kafka_emitter(topic_name:str,message):
    producer = None
    try:
        producer = get_producer()
        send_messages(producer,topic_name,message)
        logger.debug(f"kafka_emitter sent message to topic:{topic_name}")
        return True
    except Exception as e:
        logger.error(f"[Kafka_emitter] failed to send to '{topic_name}':{e}")
        return False
    finally:
        if producer:
            producer.close()
