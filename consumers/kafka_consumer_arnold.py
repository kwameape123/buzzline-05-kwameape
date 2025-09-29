"""
kafka_consumer_arnold.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys

# import external modules
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# import SQLite and PostgreSQL functions
from consumers.sqlite_consumer_case import init_sqlite_db as init_sqlite_db, insert_message as insert_sqlite_message
from consumers.postgres_consumer_arnold import init_pg_db as init_postgres_db, insert_message as insert_postgres_message


#####################################
# Function to process a single message
#####################################

def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.

    Returns:
        dict: Processed message with proper types, or None on error.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),            
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Function to check if Kafka topic exists
#####################################

def is_topic_available(topic_name: str, bootstrap_servers: str) -> bool:
    """
    Check if a Kafka topic exists on the given broker.

    Args:
        topic_name (str): The Kafka topic to check.
        bootstrap_servers (str): Kafka broker address (e.g., "localhost:9092").

    Returns:
        bool: True if the topic exists, False otherwise.
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin_client.list_topics()
        return topic_name in topics
    except Exception as e:
        logger.error(f"Failed to verify topic '{topic_name}': {e}")
        return False


#####################################
# Consume Messages from Kafka Topic
#####################################

def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sqlite_path: pathlib.Path,
    pg_config: dict,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
        topic (str): Kafka topic to consume messages from.
        kafka_url (str): Kafka broker address.
        group (str): Consumer group ID for Kafka.
        sqlite_path (pathlib.Path): Path to the SQLite database file.
        pg_config (dict): PostgreSQL connection config.
        interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sqlite_path=}")
    logger.info(f"   {pg_config=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        if not is_topic_available(topic, kafka_url):
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer."
            )
            sys.exit(13)
        else:
            logger.info(f"Kafka topic '{topic}' is ready.")

    # Initialize databases
    init_sqlite_db(sqlite_path)
    init_postgres_db(pg_config)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_sqlite_message(processed_message, sqlite_path)
                insert_postgres_message(processed_message, pg_config)

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the consumer process.
    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        pg_config = config.get_postgres_config()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_sqlite_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, pg_config, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
