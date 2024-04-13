import json

def read_local_config(config_file_path: str= "code/config.json"):
    with open(config_file_path, "r") as file:
        config = json.load(file)
    return config