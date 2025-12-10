from parser.source.yandex_market import YandexMarketParser
from parser.source.wildberries import WildberriesParser
from parser.source.detmir import DetmirParser
from parser.source.ozon import OzonParser
from utils.url import parse_url_source
from structs.task import ParsingTask

import threading


class ParserMaster():
    def __init__(self, cfg: dict):
        self.max_timeout_thread_determinate = cfg["master"]["max_timeout_thread_determinate_sec"]

        self.ozon = OzonParser(cfg)
        self.ozon_stop_event = threading.Event()
        self.ozon_thread = threading.Thread(target=self.ozon.start_listen_parsing_queue, args=[self.ozon_stop_event,], daemon=False)
        self.ozon_thread.start()

        self.wildberries = WildberriesParser(cfg)
        self.wildberries_stop_event = threading.Event()
        self.wildberries_thread = threading.Thread(target=self.wildberries.start_listen_parsing_queue, args=[self.wildberries_stop_event,], daemon=False)
        self.wildberries_thread.start()

        self.detmir = DetmirParser(cfg)
        self.detmir_stop_event = threading.Event()
        self.detmir_thread = threading.Thread(target=self.detmir.start_listen_parsing_queue, args=[self.detmir_stop_event,], daemon=False)
        self.detmir_thread.start()

        self.yandex_market = YandexMarketParser(cfg)
        self.yandex_market_stop_event = threading.Event()
        self.yandex_market_thread = threading.Thread(target=self.yandex_market.start_listen_parsing_queue, args=[self.yandex_market_stop_event,], daemon=False)
        self.yandex_market_thread.start()
            
    def execute_product_task(self, task: ParsingTask):
        source = parse_url_source(task.url)
        match source:
            case "ozon":
                self.ozon.add_to_parsing_queue(task)
                return True
            case "detmir":
                self.detmir.add_to_parsing_queue(task)
                return True
            case "wildberries":
                self.wildberries.add_to_parsing_queue(task)
                return True
            case "yandex_market":
                self.yandex_market.add_to_parsing_queue(task)
                return True
            
        return False
    
    def close(self):
        self.ozon_stop_event.set()
        self.detmir_stop_event.set()
        self.wildberries_stop_event.set()
        self.yandex_market_stop_event.set()
        
        self.ozon_thread.join()
        self.detmir_thread.join()
        self.wildberries_thread.join()
        self.yandex_market_thread.join()
        
