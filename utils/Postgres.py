'''This module is used to connect to a postgres db and insert data into it and collect data from it'''
import psycopg2 as pg
from urllib.parse import urlparse

class Postgres():

    def __init__(self, url):
        self.__url = urlparse(url)
        self.__db = ''
        self.__table = ''
    
    def set_db(self, db):
        self.__db = db

    def set_table(self, table):
            self.__table = table

    def connect(self):
        if self.__url != '' and self.__db != '':
            self.username = self.__url.username
            self.password = self.__url.password
            self.host = self.__url.hostname
            self.port = self.__url.port
            try: 
                connection = pg.connect(
                    dbname=self.__db,
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                self.cursor = connection.cursor()
                return self.cursor
            except Exception as e:
                raise e
    
    def get_all_data(self, columns : str = ''):
        if columns == '':
            query = f"SELECT * FROM {self.__table}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        else:
            query = f"SELECT {columns} FROM {self.__table}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
    
    def get_data_with_condition(self, condition : str, columns : str = ''):
        if columns == '':
            query = f"SELECT * FROM {self.__table} WHERE {condition}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        else:
            query = f"SELECT {columns} FROM {self.__table} WHERE {condition}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
    
    def insert_data(self, data : dict):
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{value}'" for value in data.values()])
        query = f"INSERT INTO {self.__table} ({columns}) VALUES ({values})"
        self.cursor.execute(query)