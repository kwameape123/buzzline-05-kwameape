import sqlite3
import csv

def connect_db(db_file):
    """
    Create and return a SQLite database connection.
    
    Args:
        db_file (str): Path to the .sqlite database file.
        
    Returns:
        sqlite3.Connection: SQLite connection object.
    """
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row  # Fetch results as dictionaries
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
        connection (sqlite3.Connection, required): Existing DB connection.
        
    Returns:
        List[dict]: List of rows as dictionaries.
    """
    if connection is None:
        raise ValueError("A valid database connection must be provided.")
    
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        # Convert sqlite3.Row objects to dictionaries
        results = [dict(row) for row in rows]
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
