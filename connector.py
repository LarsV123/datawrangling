import os
import psycopg2
from dotenv import load_dotenv, find_dotenv
from utils import linebreak

load_dotenv(find_dotenv())


class Connector:
    """
    Connects to the Postgres database using credentials from .env file.
    """

    def __init__(self):
        # Connect to the Postgres server
        self.connection = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            database=os.environ.get("POSTGRES_DB"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
        )

        # Create a cursor
        self.cursor = self.connection.cursor()

        # Check connection
        self.cursor.execute("SELECT version()")
        db_version = self.cursor.fetchone()
        print(f"Connected to: {db_version[0]}")

    def close(self):
        self.cursor.close()
        self.connection.close()
        print("Connection to database closed")
