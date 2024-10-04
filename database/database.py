import psycopg
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from database.query import CHECK_DB
# from tabulate import tabulate

load_dotenv()

API_MODE = os.getenv("API_MODE")
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

CONNECTION_STRING = f"postgres://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def db_connection_check():
    try:
        conn = psycopg.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute(CHECK_DB)

        return True
    except Exception as e:
        logging.error(f"PostgreSQL 연결 실패 : {str(e)}")
        return False

def execute_query(query):
    try:
        with psycopg.connect(CONNECTION_STRING) as con:
            with con.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
                
                column_names = [desc[0] for desc in cur.description]

                df = pd.DataFrame(result, columns=column_names)

                pd.set_option('display.max_columns', None)
                pd.set_option('display.max_rows', None)

                return df
    except Exception as e:
        logging.error(f"데이터베이스 연결 또는 쿼리 실패: {e}")
        return None

# sql = "select * from users;"
# print("===============================")
# print(execute_query(sql))

