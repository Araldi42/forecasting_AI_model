'''This module is used to connect to a mongo db and insert data into it'''
from pymongo import MongoClient

class Mongo():
    """CLASS TO HANDLE MONGO DB CONNECTIONS AND INSERTIONS

    Attributes
    ----------
    port : int
        Port to connect to the mongo db
    host : str
        Host to connect to the mongo db
    mongo_con : MongoClient
        MongoClient object to connect to the mongo db
    database : str
        Database to be used
    collection : str
        Collection to be used
    verbose : bool
        Verbose parameter
    """
    def __init__(self, port, host):
        """CONSTRUCTOR
        
        Parameters
        ----------
        port : int
            Port to connect to the mongo db
        host : str
            Host to connect to the mongo db
        Returns
        -------
        None
            Create a new Mongo object
        """
        self.__port = port
        self.__host = host
        self.__mongo_con = ''
        self.__database = ''
        self.__collection = ''
        self.__verbose = False

    def set_verbose(self, verbose: bool):
        """SET THE VERBOSE PARAMETER
        
        Parameters
        ----------
        verbose : bool
            Verbose parameter
        Returns
        -------
        None
            Set the verbose parameter
        """
        self.__verbose = verbose

    def connect(self):
        """CONNECT TO MONGO DB

        Parameters
        ----------
        None
        Returns
        -------
        MongoClient
            MongoClient object to connect to the mongo db
        """
        self.__mongo_con = MongoClient(self.__host,
                                       self.__port)
        return self.__mongo_con
    
    def end_connection(self):
        """END THE MONGO DB CONNECTION

        Parameters
        ----------
        None
        Returns
        -------
        None
            End the mongo db connection
        """
        self.__mongo_con.close()
        if self.__verbose:
            print("Connection closed")

    def set_database(self, database):
        """SET THE DATABASE TO BE USED

        Parameters
        ----------
        database : str
            Database to be used
        Returns
        -------
        None
            Set the database to be used
        """
        database = str(database)
        self.__database = database
        if self.__verbose:
            print(f"Database set to: {self.__database}")

    def set_collection(self, collection):
        """SET THE COLLECTION TO BE USED

        Parameters
        ----------
        collection : str
            Collection to be used
        Returns
        -------
        None
            Set the collection to be used
        """
        collection = str(collection)
        self.__collection = collection
        if self.__verbose:
            print(f"Collection set to: {self.__collection}")

    def get_specific_data(self, data:str):
        """GET SPECIFIC DATA FROM MONGODB ACCORDING TO THE DATA PARAMETER

        Parameters
        ----------
        data : str
            Data to be retrieved from the mongo db
        Returns
        -------
        list
            List of data retrieved from the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        if self.__verbose:
            print(f"Getting data from {self.__database}.{self.__collection}")
        return collection.find({}, {data: 1})

    def find_data(self, data, sort_query=None):
        """FIND DATA IN MONGODB

        Parameters
        ----------
        data : dict
            Data to be found in the mongo db
        Returns
        -------
        dict
            Data found in the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        if self.__verbose:
            print(f"Finding data in {self.__database}.{self.__collection}")
        if sort_query:
            return collection.find(data).sort(sort_query).next()
        return collection.find_one(data)
    
    def find_all_data(self, data=None):
        """FIND ALL DATA IN MONGODB

        Parameters
        ----------
        None
        Returns
        -------
        list
            List of all data found in the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        if self.__verbose:
            print(f"Finding all data in {self.__database}.{self.__collection}")
        if data is None:
            return collection.find()
        return collection.find(data)

    def insert_data(self, data):
        """INSERT SINGLE DATA INTO MONGODB

        Parameters
        ----------
        data : dict
            Data to be inserted into the mongo db
        Returns
        -------
        None
            Insert data into the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        collection.insert_one(data)
        if self.__verbose:
            print(f"inserted into {self.__database}.{self.__collection}: {data}")

    def insert_many_data(self, data):
        """INSERT MANY DATA INTO MONGODB

        Parameters
        ----------
        data : list
            List of data to be inserted into the mongo db
        Returns
        -------
        None
            Insert data into the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        collection.insert_many(data)
        if self.__verbose:
            print(f"inserted many into {self.__database}.{self.__collection}: {len(data)} data")
    
    def update(self, filter_data: dict, data: dict):
        """UPDATE DATA IN MONGODB

        Parameters
        ----------
        filter : dict
            Filter to find the data to be updated
        data : dict
            Data to be updated
        Returns
        -------
        None
            Update data in the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        collection.update_one(filter_data, {"$set": data})
        if self.__verbose:
            print(f"updated {self.__database}.{self.__collection}: {data}")
    
    def update_or_insert(self, filter_data: dict, data: dict):
        """UPDATE OR INSERT DATA IN MONGODB

        Parameters
        ----------
        filter : dict
            Filter to find the data to be updated
        data : dict
            Data to be updated
        Returns
        -------
        None
            Update data in the mongo db
        """
        db = self.__mongo_con[self.__database]
        collection = db[self.__collection]
        collection.update_one(filter_data, {"$set": data}, upsert=True)
        if self.__verbose:
            print(f"updated {self.__database}.{self.__collection}: {data}")
