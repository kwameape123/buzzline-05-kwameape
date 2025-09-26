"""
file_consumer_case.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
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
import pathlib
import sys
import time

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger
from .sqlite_consumer_case import init_db, insert_message

#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
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
# Consume Messages from Live Data File
#####################################


def consume_messages_from_file(live_data_path, sql_path, interval_secs, last_position):
    logger.info("Called consume_messages_from_file()")
    init_db(sql_path)

    # Start at the beginning or given position
    last_position = 0

    while True:
        try:
            with open(live_data_path, "r") as file:
                # Move to last read position
                file.seek(last_position)

                while True:
                    line = file.readline()  # read a single line
                    if not line:  # no new data yet
                        time.sleep(0.1)
                        continue

                    # Process only if line has content
                    if line.strip():
                        message = json.loads(line.strip())
                        processed_message = process_message(message)
                        if processed_message:
                            insert_message(processed_message, sql_path)

                    # Update last_position
                    last_position = file.tell()

        except FileNotFoundError:
            logger.error(f"Live data file not found at {live_data_path}.")
            time.sleep(interval_secs)
        except json.JSONDecodeError as e:
            logger.warning(f"Skipped malformed JSON line: {e}")
        except Exception as e:
            logger.error(f"Error reading from live data file: {e}")
            time.sleep(interval_secs)



#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.

    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
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
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
