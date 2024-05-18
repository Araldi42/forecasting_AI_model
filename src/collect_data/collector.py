import os
import sys
import pandas as pd
from dotenv import load_dotenv
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.Mongo import Mongo

load_dotenv()

def connect_to_mongo():
    mongo = Mongo(port=int(os.getenv("MONGO_PORT")),
                  host=os.getenv("MONGO_HOST"))
    mongo.connect()
    return mongo

def get_data(node, period):
    mongo = connect_to_mongo()
    mongo.set_database(os.getenv("MONGO_DATABASE"))
    mongo.set_collection(os.getenv("MONGO_COLLECTION"))
    data = mongo.find_data({'info.id_node': node,
                         'timestamp': {'$gte': period}})
    lista = []
    for value in data:
        print(value)
        list.append(value)
    df = pd.DataFrame(lista)
    return df

if __name__ == "__main__":
    mongo = connect_to_mongo()
    df = get_data("DMC012", 1672542000)
    print(df.head())
    #mongo.end_connection()