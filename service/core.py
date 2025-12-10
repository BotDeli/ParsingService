from storage.postgresql import MainStorage
from metrics.client import ClientMetrics
from structs.product import ProductInfo
from parser.master import ParserMaster
from structs.task import ParsingTask

from time import sleep
import threading

class ServiceCore():
    def __init__(self, cfg: dict):
        self.max_timeout_thread_determinate = cfg["parsers"]["master"]["max_timeout_thread_determinate_sec"]
        self.task_tracking = {}

        self.metrics = ClientMetrics(cfg["metrics"]["client_port"])
        self.metrics_thread = threading.Thread(target=self.metrics.start_tracking_system_metrics, daemon=True)
        self.metrics_thread.start()

        self.storage = MainStorage(cfg["storage"]["postgresql"])
        self.storage.init_tables()

        self.parser = ParserMaster(cfg["parsers"])

        self.thread_everyday_parser = threading.Thread(target=self.start_everyday_product_parser, daemon=True)
        self.thread_everyday_parser.start()

    def start_everyday_product_parser(self):
        while True:
            required_update = self.storage.get_required_update_geo_params()
            for (url, geo_params) in required_update:
                self.add_parsing_task_with_url(url, geo_params)

            sleep(86400)

    def add_parsing_task_with_product_id(self, product_id: int, geo_params: list[str]) -> str | None:
        try:
            url = self.storage.product.get_product_url(product_id)
            return self.add_parsing_task_with_url(url, geo_params)
        except:
            return None

    def add_parsing_task_with_url(self, url: str, geo_params: list[str]) -> str | None:
        task = ParsingTask(
            url=url, 
            geo_params=geo_params, 
            on_callback=self._save_callback_products,
            on_error=self._on_error,
        )
        
        if self.parser.execute_product_task(task):
            self.task_tracking[task.id] = True
            return task.id

    def _save_callback_products(self, task_id: str, products: list[ProductInfo]):
        if products is None or len(products) == 0:
            del self.task_tracking[task_id]
            return

        source = products[0].source
        self.metrics.total_tasks.labels(source=source).inc()
        self.metrics.total_products.labels(source=source).inc(len(products))

        err_geo_params = []
        for product in products:
            product_id = self.storage.product.save_product(product.url, product.article, product.source)
            if product.is_error():
                self.metrics.error_products.labels(source=source).inc()
                self.storage.parsed_logs.save_parsed_log(product_id, product)
                err_geo_params.append(product.location)
                continue

            self.metrics.product_process_time.labels(source=source).observe(product.process_time)
            self.storage.product_info.save_product_info(product_id, product)
            self.storage.parsed_logs.save_parsed_log(product_id, product)

        if 0 < len(err_geo_params):
            self.add_parsing_task_with_url(products[0].url, err_geo_params)
        else:
            del self.task_tracking[task_id]

    def _on_error(self, source: str):
        self.metrics.total_tasks.labels(source=source).inc()
        self.metrics.error_tasks.labels(source=source).inc()      

    def is_working_task(self, task_id: str) -> bool:
        if task_id in self.task_tracking:
            return self.task_tracking[task_id]
        return False
        
    def close(self):
        self.parser.close()
        self.storage.close()
