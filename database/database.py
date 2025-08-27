import psycopg2
import os
import polars as pl
from psycopg2.extras import execute_values
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

    def merge_dataframe(self, table_name: str, data: pl.DataFrame, delete_columns: list[str], primary_key_columns: list[str] = None):
        """
        Merge a Polars DataFrame into a Postgres table (UPSERT).

        Parameters
        ----------
        table_name : str
            Name of the Postgres table
        data : pl.DataFrame
            Polars DataFrame to insert/merge
        delete_columns : list[str]
            Columns to check conflicts on (usually primary keys or unique keys)
        primary_key_columns : list[str], optional

        """
        records = list(data.iter_rows())
        columns = list(data.columns)
        
        try:
            with self.connection.cursor() as cur:
                if delete_columns:
                    delete_values = data.select(delete_columns).unique().iter_rows()
                    placeholders = ", ".join(["%s"] * len(delete_columns))
                    where_clause = f"({', '.join(delete_columns)}) IN %s"

                    delete_sql = f"DELETE FROM {table_name} WHERE {where_clause}"
                    cur.execute(delete_sql, (tuple(delete_values),))
                    print(f"Deleted {cur.rowcount} existing rows from {table_name}")

                    insert_sql = f"""
                        INSERT INTO {table_name} ({", ".join(columns)})
                        VALUES %s
                    """
                    
                    execute_values(cur, insert_sql, records)
                    print(f"Inserted {len(records)} rows into {table_name}")

                self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            print(f"Error merging DataFrame: {e}")