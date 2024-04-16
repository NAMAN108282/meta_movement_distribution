import json

def read_config(path: str= "code/config.json"):
    with open(path, "r") as file:
        config = json.load(file)
    return config
