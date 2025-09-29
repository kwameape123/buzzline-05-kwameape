# file_consumer_arnold.py

import json
import pathlib
import sys
import time

import utils.utils_config as config
from utils.utils_logger import logger

# Import SQLite and PostgreSQL functions
from consumers.sqlite_consumer_case import init_sqlite_db as init_sqlite_db, insert_message as insert_sqlite_message
from consumers.postgres_consumer_arnold import init_pg_db as init_postgres_db, insert_message as insert_postgres_message


def process_message(message: str) -> dict:
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


def consume_messages_from_file(live_data_path, sqlite_path, pg_config, interval_secs):
    logger.info("Called consume_messages_from_file()")

    last_position = 0

    while True:
        try:
            with open(live_data_path, "r", buffering=1, encoding="utf-8", errors="ignore") as file:
                file.seek(last_position)

                while True:
                    line = file.readline()
                    if not line:
                        time.sleep(0.1)
                        continue

                    if line.strip():
                        message = json.loads(line.strip())
                        processed_message = process_message(message)
                        if processed_message:
                            insert_sqlite_message(processed_message, sqlite_path)
                            insert_postgres_message(processed_message, pg_config)

                    last_position = file.tell()

        except FileNotFoundError:
            logger.error(f"Live data file not found at {live_data_path}.")
            time.sleep(interval_secs)
        except json.JSONDecodeError as e:
            logger.warning(f"Skipped malformed JSON line: {e}")
        except Exception as e:
            logger.error(f"Error reading from live data file: {e}")
            time.sleep(interval_secs)


def main():
    logger.info("Starting Consumer to run continuously.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        pg_config: dict = config.get_postgres_config()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted SQLite database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete SQLite DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_sqlite_db(sqlite_path)
        init_postgres_db(pg_config)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    try:
        consume_messages_from_file(live_data_path, sqlite_path, pg_config, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


if __name__ == "__main__":
    main()