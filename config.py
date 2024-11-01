import json
from typing import IO

class CONFIG:
    def __init__(self, file_name: str):
        try:
            with open(file_name) as file:
                conf = json.load(file)
                self.db_host = conf.get("db_host", "localhost")
                self.db_port = conf.get("db_port", "5432")
                self.db_username = conf.get("db_username", "postgres")
                self.db_password = conf.get("db_password", "postgres")
                self.db_name = conf.get("db_name", "postgres")
        except Exception as e:
            print(e)
            import sys
            sys.exit(1)


