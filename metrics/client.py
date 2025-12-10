import loggers

from prometheus_client import start_http_server, Counter, Histogram, Gauge
from time import sleep
import psutil


class ClientMetrics():
    def __init__(self, port: int):
        start_http_server(port)
        loggers.metrics.info(f"Prometheus metrics server started on port {port}.")

        self.cpu_usage = Gauge(
            "system_cpu_percent",
            "Average CPU usage %"
        )

        self.ram_usage = Gauge(
            "system_ram_percent",
            "RAM usage %"
        )

        self.total_tasks = Counter(
            "parser_total_tasks", 
            "Count total executed tasks to parser",
            ["source"],
        )

        self.error_tasks = Counter(
            "parser_error_tasks", 
            "Count raised errors for parsing tasks",
            ["source"],
        )

        self.total_products = Counter(
            "parser_total_products", 
            "Count total parsed products",
            ["source"],
        )

        self.error_products = Counter(
            "parser_error_products", 
            "Count raised errors for parsing product",
            ["source"],
        )

        self.product_process_time = Histogram(
            "parser_product_process_time",
            "Time spent on product parsing - seconds",
            ["source"],
        )

        self.server_total_requests = Counter(
            "server_total_requests",
            "Count total server requests",
            ["status_code"],
        )

        self.server_request_duration = Histogram(
            "server_request_duration_sec",
            "HTTP request duration - seconds",
            ["status_code"],
        )

        self.update_system_metrics()

    def start_tracking_system_metrics(self):
        while True:
            self.update_system_metrics()
            sleep(5)

    def update_system_metrics(self):
        self._update_cpu_metrics()
        self._update_ram_metrics()

    def _update_cpu_metrics(self):
        try:
            cpu_percent = psutil.cpu_percent()
            self.cpu_usage.set(cpu_percent)
        except Exception as error:
            loggers.metrics.warning(f"Fail update cpu metrics: {error}")
    
    def _update_ram_metrics(self):
        try:
            memory = psutil.virtual_memory()
            self.ram_usage.set(memory.percent)
        except Exception as error:
            loggers.metrics.warning(f"Fail update ram metrics: {error}")