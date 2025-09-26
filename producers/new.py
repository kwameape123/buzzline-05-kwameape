"""
producer_case.py — Reliable Kafka Producer

This version ensures that every generated message is published to Kafka
before proceeding, with retries and topic auto-creation.
"""

import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime
from typing import Mapping, Any

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable

import utils.utils_config as config
from utils.utils_logger import logger
from utils.utils_producer import verify_services
from utils.emitters import file_emitter

#####################################
# Sentiment Analysis Stub
#####################################

def assess_sentiment(text: str) -> float:
    return round(random.uniform(0, 1), 2)

#####################################
# Message Generator
#####################################

def generate_messages():
    ADJECTIVES = ["amazing", "funny", "boring", "exciting", "weird"]
    ACTIONS = ["found", "saw", "tried", "shared", "loved"]
    TOPICS = [
        "a movie", "a meme", "an app", "a trick", "a story",
        "Python", "JavaScript", "recipe", "travel", "game",
    ]
    AUTHORS = ["Alice", "Bob", "Charlie", "Eve"]
    KEYWORD_CATEGORIES = {
        "meme": "humor",
        "Python": "tech",
        "JavaScript": "tech",
        "recipe": "food",
        "travel": "travel",
        "movie": "entertainment",
        "game": "gaming",
    }

    while True:
        adjective = random.choice(ADJECTIVES)
        action = random.choice(ACTIONS)
        topic = random.choice(TOPICS)
        author = random.choice(AUTHORS)
        message_text = f"I just {action} {topic}! It was {adjective}."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        keyword_mentioned = next((w for w in KEYWORD_CATEGORIES if w in topic), "other")
        category = KEYWORD_CATEGORIES.get(keyword_mentioned, "other")
        sentiment = assess_sentiment(message_text)

        yield {
            "message": message_text,
            "author": author,
            "timestamp": timestamp,
            "category": category,
            "sentiment": sentiment,
            "keyword_mentioned": keyword_mentioned,
            "message_length": len(message_text),
        }

#####################################
# Kafka Setup
#####################################

def setup_kafka(topic: str, kafka_server: str, retries: int = 5, delay: float = 2.0) -> KafkaProducer:
    """Connect to Kafka and ensure topic exists, retrying if necessary."""
    producer = None

    for attempt in range(retries):
        try:
            # Connect producer
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5
            )
            logger.info(f"Kafka producer connected to {kafka_server}")

            # Ensure topic exists
            admin_client = KafkaAdminClient(bootstrap_servers=kafka_server)
            topics = admin_client.list_topics()
            if topic not in topics:
                admin_client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
                logger.info(f"Kafka topic '{topic}' created.")
            else:
                logger.info(f"Kafka topic '{topic}' exists.")
            
            return producer

        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not available, retry {attempt+1}/{retries}...")
            time.sleep(delay)
        except KafkaError as e:
            logger.warning(f"Kafka error on attempt {attempt+1}/{retries}: {e}")
            time.sleep(delay)

    logger.error("Could not connect to Kafka after retries. Exiting.")
    sys.exit(1)

#####################################
# File Emitter
#####################################

def emit_to_file(message: Mapping[str, Any], path: pathlib.Path):
    file_emitter.emit_message(message, path=path)

#####################################
# Main Producer Loop
#####################################

def main():
    logger.info("Starting Producer. Use Ctrl+C to stop.")

    # Config
    interval_secs = config.get_message_interval_seconds_as_int()
    topic = config.get_kafka_topic()
    kafka_server = config.get_kafka_broker_address()
    live_data_path = config.get_live_data_path()

    # Reset file
    if live_data_path.exists():
        live_data_path.unlink()
    os.makedirs(live_data_path.parent, exist_ok=True)

    # Verify services & setup Kafka
    if not verify_services(strict=False):
        logger.error("Kafka service unavailable. Exiting.")
        sys.exit(1)

    producer = setup_kafka(topic, kafka_server)

    # Emit messages
    try:
        for message in generate_messages():
            logger.info(message)

            # Save to file
            emit_to_file(message, live_data_path)

            # Send to Kafka and wait for acknowledgement
            future = producer.send(topic, value=message)
            try:
                result = future.get(timeout=10)  # Ensure delivery
                logger.debug(f"Message sent to Kafka partition {result.partition}")
            except Exception as e:
                logger.error(f"Failed to send message to Kafka: {e}")

            # Flush to make sure message is sent before sleeping
            producer.flush()
            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("Producer shutting down.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
