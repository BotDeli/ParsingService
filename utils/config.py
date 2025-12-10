import yaml


def load_config_data(config_path: str) -> dict: 
    with open(config_path, 'r') as file:
        config_data = yaml.safe_load(file)
        return config_data