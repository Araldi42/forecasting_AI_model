'''This module is used to connect to a postgres db and insert data into it and collect data from it'''
from urllib.parse import urlparse
import psycopg2 as pg

class Postgres():
    '''CLASS TO HANDLE POSTGRES DB CONNECTIONS AND INSERTIONS

    Attributes
    ----------
    __url : urllib.parse.ParseResult
        URL to connect to the postgres db
    __db : str
        Database to be used
    __table : str
        Table to be used
    username : str
        Username to connect to the postgres db
    password : str
        Password to connect to the postgres db
    host : str
        Host to connect to the postgres db
    port : str
        Port to connect to the postgres db
    cursor : psycopg2.extensions.cursor
        Cursor to interact with the postgres db
    '''

    def __init__(self, url):
        self.__url = urlparse(url)
        self.__db = ''
        self.__table = ''
        self.username = ''
        self.password = ''
        self.host = ''
        self.port = ''
        self.cursor = ''

    def set_db(self, db):
        '''Set the database to be used'''
        self.__db = db

    def set_table(self, table):
        '''Set the table to be used'''
        self.__table = table

    def connect(self):
        '''Connect to the postgres db'''
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
        '''Get all data from the table'''
        if columns == '':
            query = f"SELECT * FROM {self.__table}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        query = f"SELECT {columns} FROM {self.__table}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_data_with_condition(self, condition : str, columns : str = ''):
        '''Get data from the table with a condition'''
        if columns == '':
            query = f"SELECT * FROM {self.__table} WHERE {condition}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        query = f"SELECT {columns} FROM {self.__table} WHERE {condition}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_data(self, data : dict):
        '''Insert data into the table'''
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{value}'" for value in data.values()])
        query = f"INSERT INTO {self.__table} ({columns}) VALUES ({values})"
        self.cursor.execute(query)