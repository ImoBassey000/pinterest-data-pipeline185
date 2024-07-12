import os
import yaml
import requests
from time import sleep
import random
from sqlalchemy import create_engine, text
from datetime import datetime
import json

random.seed(100)
config_dir = "/Users/imobassey/Desktop/DevOps/pinterest-data-pipeline185/"

class AWSDBConnector:
    def __init__(self, config_dir=""):
        self.config_dir = config_dir

    def my_yaml_file(self, filename):
        filepath = os.path.join(self.config_dir, filename)
        with open(filepath, "r") as file:
            return yaml.safe_load(file)
        
    def read_my_db_creds(self):
        return self.my_yaml_file("db_creds.yaml")

    def create_db_connector(self):
        db_creds = self.read_my_db_creds()
        engine = create_engine(
            f"mysql+pymysql://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}?charset=utf8mb4"
        )
        return engine

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def send_data_to_topic(api_url, topic, data):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    }, default=json_serializer)
    response = requests.request("POST", api_url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Failed to send data to {topic}. Status code: {response.status_code}")
    else:
        print(f"Successfully sent data to {topic}")

def run_infinite_post_data_loop():
    new_connector = AWSDBConnector(config_dir=config_dir)
    db_creds = new_connector.read_my_db_creds()
    user_id = db_creds['UserId']
    pin_url = db_creds['PIN_TOPIC']
    geo_url = db_creds['GEO_TOPIC']
    user_url = db_creds['USER_TOPIC']

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                send_data_to_topic(pin_url, f"{user_id}.pin", pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                send_data_to_topic(geo_url, f"{user_id}.geo", geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                send_data_to_topic(user_url, f"{user_id}.user", user_result)

            print(pin_result)
            print(geo_result)
            print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
