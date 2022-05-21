import yaml
class Loader:
    def __init__(self, config_path:str):
        self.config_path = config_path
    
    def loader(self):
        with open(self.config_path, "r") as f:
            data = yaml.safe_load(f)
        return data