import os
import yaml
from sqlalchemy import create_engine

class AWSDBConnector:
    def __init__(self, config_dir: str = ""):
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
