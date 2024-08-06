import os
import json
import requests
import random
from time import sleep
from sqlalchemy import text
from db_utils import AWSDBConnector

random.seed(100)

config_dir = "/Users/imobassey/Desktop/DevOps/pinterest-data-pipeline185/"

def send_data_to_topic(api_url: str, topic: str, data: dict):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    }, default=str)
    response = requests.post(api_url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Failed to send data to {topic}. Status code: {response.status_code}")
    else:
        print(f"Successfully sent data to {topic}")

def fetch_random_row(connection, table_name, random_row):
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    result = connection.execute(query)
    for row in result:
        return dict(row._mapping)
    return None

def run_infinite_post_data_loop():
    connector = AWSDBConnector(config_dir=config_dir)
    db_creds = connector.read_my_db_creds()

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = connector.create_db_connector()

        with engine.connect() as connection:
            pin_result = fetch_random_row(connection, "pinterest_data", random_row)
            if pin_result:
                send_data_to_topic(db_creds['PIN_TOPIC'], db_creds['UserId'].pin", pin_result)

            geo_result = fetch_random_row(connection, "geolocation_data", random_row)
            if geo_result:
                send_data_to_topic(db_creds['GEO_TOPIC'], db_creds['UserId'].geo", geo_result)

            user_result = fetch_random_row(connection, "user_data", random_row)
            if user_result:
                send_data_to_topic(db_creds['USER_TOPIC'], db_creds['UserId'].user", user_result)

            print(pin_result)
            print(geo_result)
            print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
