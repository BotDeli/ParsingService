from service.api.server import ServiceServer
from service.core import ServiceCore
from utils.config import load_config_data

import loggers

YAML_CONFIG_PATH = "./config.yaml"

def main():
    cfg = load_config_data(YAML_CONFIG_PATH)
    loggers._init_logger_config(cfg["logger"]["filepath"], cfg["logger"]["is_debug"])

    core = ServiceCore(cfg)

    try:
        server = ServiceServer(core, cfg["server"])
        server.start_listener()
    except Exception as error:
        print(error)
    finally:
        print("close core service!")
        core.close()

if __name__ == '__main__':
    main()