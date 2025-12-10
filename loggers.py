import logging


metrics = logging.getLogger("metrics.client")
ozon = logging.getLogger("parser.ozon")
detmir = logging.getLogger("parser.detmir")
wildberries = logging.getLogger("parser.wildberries")
yandex_market = logging.getLogger("parser.yandex_market")

def _init_logger_config(log_filename: str, is_debug: bool):
    file_log = logging.FileHandler(log_filename)
    console_out = logging.StreamHandler()

    if is_debug:
        logging.basicConfig(handlers=(file_log, console_out), level=logging.DEBUG)
    else:
        logging.basicConfig(handlers=(file_log, console_out), level=logging.WARNING)