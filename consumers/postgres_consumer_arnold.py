#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib

# import external libraries
import psycopg2
from psycopg2 import sql

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Define Function to Initialize PostgreSQL Database
#####################################


def init_pg_db(db_config: dict):
    """
    Initialize the PostgreSQL database -
    drop 'streamed_messages' table if it exists and recreate it.

    Args:
    - db_config (dict): Dictionary containing PostgreSQL connection parameters.
      Example:
      {
          "dbname": "mydb",
          "user": "myuser",
          "password": "mypassword",
          "host": "localhost",
          "port": 5432
      }
    """
    logger.info(f"Calling Postgres init_db() with {db_config=}")
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS streamed_messages (
                id SERIAL PRIMARY KEY,
                message TEXT,
                author TEXT,
                category TEXT,
                sentiment REAL,
                keyword_mentioned TEXT,
                message_length INTEGER
            )
            """
        )
        conn.commit()
        cursor.close()
        conn.close()

        logger.info("SUCCESS: Database initialized and table ready.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize PostgreSQL database: {e}")


#####################################
# Define Function to Insert a Processed Message into the Database
#####################################


def insert_message(message: dict, db_config: dict) -> None:
    """
    Insert a single processed message into the PostgreSQL database.

    Args:
    - message (dict): Processed message to insert.
    - db_config (dict): PostgreSQL connection parameters.
    """
    logger.info("Calling Postgres insert_message() with:")
    logger.info(f"{message=}")
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO streamed_messages (
                message, author, category, sentiment, keyword_mentioned, message_length
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                message["message"],
                message["author"],
                message["category"],
                message["sentiment"],
                message["keyword_mentioned"],
                message["message_length"],
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")


#####################################
# Define Function to Delete a Message from the Database
#####################################


def delete_message(message_id: int, db_config: dict) -> None:
    """
    Delete a message from the PostgreSQL database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_config (dict): PostgreSQL connection parameters.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM streamed_messages WHERE id = %s", (message_id,))
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")


#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting Postgres db testing.")

    # Use config to get Postgres connection details
    DB_CONFIG = config.get_postgres_config()

    # Initialize the PostgreSQL database
    init_pg_db(DB_CONFIG)

    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }

    insert_message(test_message, DB_CONFIG)

    # Retrieve the ID of the inserted test message
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM streamed_messages WHERE message = %s AND author = %s",
            (test_message["message"], test_message["author"]),
        )
        row = cursor.fetchone()
        if row:
            test_message_id = row[0]
            # Delete the test message
            delete_message(test_message_id, DB_CONFIG)
        else:
            logger.warning("Test message not found; nothing to delete.")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
