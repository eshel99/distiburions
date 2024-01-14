# code to change 1 storeno's ovesec value

import pandas as pd
import sqlite3
from contextlib import contextmanager

# Function to establish SQLite connection
@contextmanager
def sqlite_connection(db_path):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()

# Function to prepare data and update stores_ovenseconds
def update_stores_ovenseconds(conn, ovenseconds, storeno):
    # Replace this with your implementation to prepare and execute the update query
    update_query = f"UPDATE stores SET ovenseconds = {ovenseconds} WHERE StoreNo = '{storeno}';"
    conn.execute(update_query)

# Provide the path to your SQLite database
db_path = 'C:/Users/Eshel/Downloads/Algo.db_40701/Algo.db'

# Get user input for ovenseconds
ovenseconds = int(input("Enter the value for ovenseconds:"))
# Get user input for ovenseconds
stores = input("Enter the storeno, separate by comma:").split(',')

with sqlite_connection(db_path) as conn:
    for storeno in stores:
        storeno = storeno.strip()   # Remove leading/trailing whitespaces if any
        # Call the function with the user-provided ovenseconds value
        update_stores_ovenseconds(conn, ovenseconds, storeno)
        conn.commit() 

        # Execute a SELECT query to fetch the updated data
        select_query = "SELECT StoreNo, OvenSeconds FROM stores ;"
        update_ovensc_example_df = pd.read_sql_query(select_query, conn)

# Print the DataFrame
update_ovensc_example_df

