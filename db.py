import oracledb
import os
from dotenv import load_dotenv

oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_19")

load_dotenv()

def get_connection():
    return oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=os.getenv("ORACLE_DSN")
    )
