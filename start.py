#!/usr/bin/env python

import json
import pymongo
import requests
from datetime import datetime

import settings


API_URL = "https://covidapi.naxa.com.np/api/v1/stats/"
MONGO_URI = "mongodb://{}:{}@ds229909.mlab.com:29909/covid19-nepal?retryWrites=false ".format(
    settings.DB_USER, settings.DB_PASSWORD)

collection_name = 'mohp-data'
client = pymongo.MongoClient(MONGO_URI)
db = client.get_default_database()


def get_collection(collection_name):
    return db[collection_name]


def is_redundent_data(collection, data):
    '''
    Check if data extracted from API has not been updated from last date entry.
    @return True if data is already present else return False
    '''
    query = {
        'data_update_date': {
            '$eq': data['data_update_date']
        }
    }
    count = collection.count_documents(query)
    return True if count > 0 else False


def start():
    response = requests.get(API_URL)

    if response.status_code is not 200:
        return "Cannot connect to website {}".format(API_URL)

    mohp_data = get_collection(collection_name)
    data = json.loads(response.text)
    update_date = data.get('update_date').replace('Z', '')
    data['data_update_date'] = datetime.fromisoformat(update_date)
    data['created_date'] = datetime.utcnow()

    if not is_redundent_data(mohp_data, data):
        print("Inserted new data")
        mohp_data.insert_one(data)
        client.close()
    else:
        print("Data already present")


def get_data():
    mohp_data = get_collection(collection_name)
    cursor = mohp_data.find()
    for data in cursor:
        print(data)


if __name__ == "__main__":
    start()
    get_data()
