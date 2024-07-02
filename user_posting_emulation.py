
import os
import yaml
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import create_engine, text

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
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    
