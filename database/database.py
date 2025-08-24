import psycopg2
from psycopg2 import sql
import os


class FishDatabase:
    def __init__(self):
        self.connection = self.connect()

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed.")

    def connect(self):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),    
                user=os.getenv("DB_USER"),     
                password=os.getenv("DB_PASSWORD"),   
                host=os.getenv("DB_HOST", "localhost"),           
                port=os.getenv("DB_PORT", "5432")                 
            )
            print("Connected to PostgreSQL successfully")
            return conn
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None

    def test_connection(self):
        if self.connection:
            print("Connection test successful.")
            self.connection.close()
        else:
            print("Connection test failed.")