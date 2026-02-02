import mysql.connector, os
from dotenv import load_dotenv

load_dotenv()

def mysql_connector():
    mydb = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PSWD"),
        database=os.getenv("MYSQL_DATABASE")
    )
    return mydb