import psycopg2
import pandas as pd


class pg_admin:

    def __init__(self, database, credentials):
        self.database = database
        self.conn = psycopg2.connect(
            host='localhost',
            database=self.database,
            user=credentials[0],
            password=credentials[1]
        )

    def write(self, query_string):
        cursor = self.conn.cursor()
        cursor.execute(query_string)
        self.conn.commit()
        cursor.close()

    def query(self, query_string):
        cursor = self.conn.cursor()
        cursor.execute(query_string)
        return cursor.fetchall()

    def get_tables(self):
        cursor = self.conn.cursor()
        sql = """SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name"""
        cursor.execute(sql)
        return cursor.fetchall()


