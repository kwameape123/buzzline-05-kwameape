""" This module is used to create summary tables from
a PostgreSQL database based on criteria
For example with use the function in this module 
to select all messages from a particular author and write it to a 
csv file."""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv

def connect_db(host, database, user, password, port=5432):
    """
    Create and return a PostgreSQL database connection.
    """
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def execute_select(query, params=None, connection=None):
    """
    Execute a SELECT statement and return results as a list of dictionaries.
    
    Args:
        query (str): The SELECT SQL query.
        params (tuple, optional): Parameters for parameterized query.
        connection (psycopg2 connection, required): Existing DB connection.
        
    Returns:
        List[dict]: List of rows as dictionaries.
    """
    if connection is None:
        raise ValueError("A valid database connection must be provided.")

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error executing query: {e}")
        raise

def write_to_csv(data, file_path):
    """
    Write a list of dictionaries to a CSV file.
    
    Args:
        data (List[dict]): Data to write.
        file_path (str): CSV file path.
    """
    if not data:
        print("No data to write.")
        return

    with open(file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Data written to {file_path}")

def close_db(connection):
    """
    Close the database connection.
    """
    if connection:
        connection.close()
