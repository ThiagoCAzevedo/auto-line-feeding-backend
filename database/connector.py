import mysql.connector, os
from dotenv import load_dotenv

load_dotenv()


class MySQL_Connector:
    def __init__(self):
        self.host = os.getenv("MYSQL_HOST")
        self.user = os.getenv("MYSQL_USER")
        self.password = os.getenv("MYSQL_PSWD")
        self.database = os.getenv("MYSQL_DATABASE")
        self.connection = self.get_connection()

    def get_connection(self):
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )