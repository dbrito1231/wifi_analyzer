"""
    Name: Postgres Connect
    Author: Damian Brito
    Date: July 2021
    Description: Connects to postgres database using custom class

    Modules:
        psycopg2 - PostgreSQL database adapter
"""
import psycopg2


class PgAdmin:
    """
    Database class used for connecting to postgreSQL database.
    """

    def __init__(self, database, credentials):
        """
        Constructor to connect to postgreSQL database.
        :param database: database name
        :param credentials: database credentials for access
        """
        self.database = database
        self.conn = psycopg2.connect(
            host='localhost',
            database=self.database,
            user=credentials[0],
            password=credentials[1]
        )

    def close(self):
        """
        Wrapper to close database connection.
        :return: connection closed
        """
        self.conn.close()

    def write(self, query_string):
        """
        Takes in queries and commits to database.
        :param query_string: query
        :return: None
        """
        cursor = self.conn.cursor()
        cursor.execute(query_string)
        self.conn.commit()
        cursor.close()

    def query(self, query_string):
        """
        Used to perform queries on database.  Only read queries
        are allowed.  Write queries will be sent but not committed.
        :param query_string: query
        :return: None
        """
        cursor = self.conn.cursor()
        cursor.execute(query_string)
        return cursor.fetchall()

    def get_tables(self):
        """
        Retrieves a list of existing tables form database.
        :return: tuple of database tables found.
        """
        cursor = self.conn.cursor()
        sql = """SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name"""
        cursor.execute(sql)
        return cursor.fetchall()
