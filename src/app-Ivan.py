import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

def connect():
    global engine
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    print("Starting the connection...")
    engine = create_engine(connection_string).execution_options(isolation_level="AUTOCOMMIT")
    return engine

# Connect to the database
engine = connect()

# Helper function to execute SQL files
def execute_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()
    # Execute SQL script using a connection
    with engine.connect() as connection:
        connection.execute(text(sql_script))

# Drop tables before creating them
drop_sql_path = "./src/sql/drop.sql"
execute_sql_file(drop_sql_path)
print("Tables dropped successfully.")

# Create tables
create_sql_path = "./src/sql/create.sql"
execute_sql_file(create_sql_path)
print("Tables created successfully.")

# Insert data
insert_sql_path = "./src/sql/insert.sql"
execute_sql_file(insert_sql_path)
print("Data inserted successfully.")

# Query and display data as a DataFrame
query = "SELECT * FROM books;"
df_books = pd.read_sql(query, engine)
print("\nBooks Table (as DataFrame):")
print(df_books)
