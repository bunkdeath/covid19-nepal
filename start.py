#!/usr/bin/env python
import os
import json
import logging
import pathlib
import pymongo
import requests
from datetime import datetime

import settings

BASE_DIR = pathlib.Path(__file__).parent.absolute()
LOG_PATH = os.path.join(BASE_DIR, 'covid19-nepal.log')
logger = logging.getLogger('covid19-nepal')


API_URL = "https://covidapi.naxa.com.np/api/v1/stats/"
MONGO_URI = "mongodb://{}:{}@ds229909.mlab.com:29909/covid19-nepal?retryWrites=false ".format(
    settings.DB_USER, settings.DB_PASSWORD)

collection_name = 'mohp-data'
client = pymongo.MongoClient(MONGO_URI)
db = client.get_default_database()


def setup_logger():
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler = logging.FileHandler(LOG_PATH)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)


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
    logger.info("Starting extraction process...")
    response = requests.get(API_URL)

    if response.status_code is not 200:
        msg = "Cannot connect to website {}".format(API_URL)
        logger.info(msg)
        return msg

    mohp_data = get_collection(collection_name)
    data = json.loads(response.text)
    update_date = data.get('update_date').replace('Z', '')
    data['data_update_date'] = datetime.fromisoformat(update_date)
    data['created_date'] = datetime.utcnow()

    if not is_redundent_data(mohp_data, data):
        logger.info("Found new data.")
        mohp_data.insert_one(data)
        client.close()
    else:
        logger.info("Data already present.")


def get_data():
    mohp_data = get_collection(collection_name)
    cursor = mohp_data.find()
    for data in cursor:
        print(data)


if __name__ == "__main__":
    setup_logger()
    start()
    get_data()
