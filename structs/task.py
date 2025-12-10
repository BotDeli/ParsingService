from structs.product import ProductInfo
from uuid import uuid4


class ParsingTask():
    def __init__(self, url: str, geo_params: list[str], on_callback: function = None, on_error: function = None):
        self.id = str(uuid4())

        self.url = url
        self.geo_params = geo_params
        self.on_callback = on_callback
        self.on_error = on_error

    def callback(self, products: list[ProductInfo]):
        if self.on_callback:
            self.on_callback(self.id, products)

    def error(self, error: Exception):
        if self.on_error:
            self.on_error(error)