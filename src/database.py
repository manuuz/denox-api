import pymongo

## conexão com mongodb via pymongo
def connection_mongodb(host, port):
    connection = pymongo.MongoClient(host, port)
    return connection

## conexão com o banco de dados denox
def connection_database():
    connection = connection_mongodb("localhost", 27017)
    return connection.denox
