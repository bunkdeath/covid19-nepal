#!/usr/bin/env python

import json
import pymongo
import requests
from datetime import datetime

import settings


API_URL = "https://covidapi.naxa.com.np/api/v1/stats/"
MONGO_URI = "mongodb://{}:{}@ds229909.mlab.com:29909/covid19-nepal?retryWrites=false ".format(
    settings.DB_USER, settings.DB_PASSWORD)


def start():
    response = requests.get(API_URL)

    if response.status_code is not 200:
        return "Cannot connect to website {}".format(API_URL)

    client = pymongo.MongoClient(MONGO_URI)
    db = client.get_default_database()
    mohp_data = db['mohp-data']
    data = json.loads(response.text)
    update_date = data.get('update_date').replace('Z', '')
    data['data_update_date'] = datetime.fromisoformat(update_date)
    data['created_date'] = datetime.utcnow()
    mohp_data.insert_one(data)
    client.close()


if __name__ == "__main__":
    start()
