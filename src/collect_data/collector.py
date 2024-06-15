import os
import sys
import pandas as pd
from dotenv import load_dotenv
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.Postgres import Postgres

load_dotenv()

def connect_to_postgres():
    '''Connect to the postgres db'''
    pg = Postgres(user=os.getenv("POSTGRES_USER"),
                  password=os.getenv("POSTGRES_PASSWORD"),
                  host=os.getenv("POSTGRES_HOST"),
                  port=os.getenv("POSTGRES_PORT"),
                  db=os.getenv("POSTGRES_DB"))
    pg.set_table("flow")
    return pg

def collect_data(debug=False):
    '''Collect data from the postgres db'''
    connection = connect_to_postgres()
    connection.connect()
    data = connection.get_all_data(columns="value, timestamp")
    if debug:
        print(data)
    return data

if __name__ == "__main__":
    collect_data(debug=True)