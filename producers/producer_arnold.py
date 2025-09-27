"""
producer_arnold.py

Stream JSON data to any combination of sinks:
- File (JSONL)
- Kafka topic

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os
import random
import time
import pathlib
from datetime import datetime
import utils.emitters.kafka_emitter as ke

# Import external packages (must be installed in .venv first)
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 5))
    logger.info(f"Message interval: {interval} seconds")
    return interval

def get_kafka_topic()->str:
    """Fetch kafka topic from enivorment or use default."""
    topic= os.getenv("BUZZ_TOPIC","buzzline_db")
    logger.info(f"Kafka Topic:{topic}")
    return topic

#####################################
# Set up Paths - write to a file the consumer will monitor
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("project2_live.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Stub Sentiment Analysis Function
#####################################

def assess_sentiment(text: str) -> float:
    """Stub for sentiment analysis; returns a random score in [0,1]."""
    return round(random.uniform(0, 1), 2)

#####################################
# Define global variables
#####################################

# Define some lists for generating buzz messages
ADJECTIVES: list = ["amazing", "funny", "boring", "exciting", "weird"]
ACTIONS: list = ["found", "saw", "tried", "shared", "loved"]
TOPICS: list = ["a movie", "a meme", "an app", "a trick", "a story"
                "python","JavaScript","recipe","travel","game"]
AUTHORS:list = ["Alice","Bob","Charlie","Eve"]
KEYWORD_CATEGORIES = {
        "meme": "humor",
        "Python": "tech",
        "JavaScript": "tech",
        "recipe": "food",
        "travel": "travel",
        "movie": "entertainment",
        "game": "gaming",
    }


#####################################
# Define a function to generate buzz messages
#####################################


def generate_messages():
    """
    Generate a stream of buzz messages in the JSON format

    This function uses a generator, which yields one buzz at a time.
    Generators are memory-efficient because they produce items on the fly
    rather than creating a full list in memory.

    Because this function uses a while True loop, it will run continuously 
    until we close the window or hit CTRL c (CMD c on Mac/Linux).
    """
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
# Define main() function to run this producer.
#####################################


def main() -> None:
    """
    Main entry point for this producer.

    It doesn't need any outside information, so the parentheses are empty.
    It doesn't return anything, so we say the return type is None.   
    The colon at the end of the function signature is required.
    All statements inside the function must be consistently indented. 
    This is a multiline docstring - a special type of comment 
    that explains what the function does.
    """
    topic_name = get_kafka_topic()
    logger.info(f"kafka_topic is {topic_name}")
    ke.create_topic(topic_name)

    logger.info("START producer...")
    logger.info("Hit CTRL c (or CMD c) to close.")
    
    # Call the function we defined above to get the message interval
    # Assign the return value to a variable called interval_secs
    interval_secs: int = get_message_interval()
    producer = ke.get_producer()


    try:
        for message in generate_messages():
            logger.info(message)
            with DATA_FILE.open("a") as f:
                f.write(json.dumps(message) + "\n")
            ke.send_messages(producer,topic_name,message)
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Producer shutting down.")




#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
