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


    # TODO: create table or update if exists
    # TODO: add update data from csvs using copy
    # TODO: create schema for tables
    #  * use timestamp

