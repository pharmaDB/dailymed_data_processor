from dotenv import dotenv_values
import os
import pymongo

from utils.logging import getLogger

_logger = getLogger(__name__)

_config = dict(
    dotenv_values(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".env")
    )
)


class MongoClient:
    def __init__(self, db_client):
        self.db_client = db_client

    def find(self, collection_name, query):
        collection = self.__get_collection(collection_name)
        results = None
        if type(query) == dict:
            query = [query]
        if not len(query):
            results = collection.find()
        elif len(query) == 1:
            results = collection.find(query[0])
        else:
            results = collection.find({"$and": query})
        return results

    def insert(self, collection_name, document):
        collection = self.__get_collection(collection_name)
        collection.insert(document)

    def upsert(self, collection_name, query, document):
        collection = self.__get_collection(collection_name)
        collection.update(query, document, upsert=True)

    def __get_collection(self, collection_name):
        return self.db_client[collection_name]


def connect_mongo():
    try:
        host, port, db_name = (
            _config["MONGODB_HOST"],
            int(_config["MONGODB_PORT"]),
            _config["MONGODB_NAME"],
        )
        mongo = pymongo.MongoClient(host, port=port)
        _logger.info(f"Successfully connected to mongodb at {host}:{port}")
        return mongo[db_name]
    except Exception as e:
        _logger.error(f"Error occured {e}")
        return
