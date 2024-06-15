import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.prophet.model import main as forecast
from utils.Postgres import Postgres

load_dotenv()

def datetime_to_timestamp(date):
    '''Convert a datetime object to a timestamp'''
    element = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    ts = datetime.timestamp(element)
    return ts

def connect_to_postgres():
    '''Connect to the postgres db'''
    pg = Postgres(user=os.getenv("POSTGRES_USER"),
                  password=os.getenv("POSTGRES_PASSWORD"),
                  host=os.getenv("POSTGRES_HOST"),
                  port=os.getenv("POSTGRES_PORT"),
                  db=os.getenv("POSTGRES_DB"))
    pg.set_table("forecasting")
    pg.connect()
    return pg

def train_model():
    '''Train the model'''
    data_model = forecast()
    return data_model

def insert_data(dataframe, debug=False):
    '''Insert data into the postgres db'''
    connection = connect_to_postgres()
    dataframe = dataframe[['ds', 'yhat']]
    dataframe['ds'] = dataframe['ds'].apply(datetime_to_timestamp)
    data_tuple = list(dataframe.itertuples(index=False, name=None))
    connection.insert_many_data(collumns="timestamp, value", data_tuples=data_tuple)
    if debug:
        data_debug = connection.get_all_data(columns="timestamp, value")
        print(data_debug)
    # TODO: Limit dataframe para pegar apenas os dados preditos pelo modelo (os que são maiores que o maior valor de timestamp da tabela flow)

if __name__ == "__main__":
    data = train_model()
    insert_data(data)
