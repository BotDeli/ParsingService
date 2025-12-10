import time

# First step - init product info
# Last step - set status
# Process time = timestamp_set_status - timestamp_init


class ProductInfo():
    def __init__(self, url: str, source: str):
        self.process_time = None
        self.start_at = time.perf_counter()

        # Service details
        self.is_err = False
        self.err_message = ""
        self.err_details = ""
        
        self.status = "undefined"
        self.http_status = None

        # Base product info
        self.url = url
        self.source = source
        self.article = ""

        self.price = None
        self.original_price = None
        self.currency = None
        
        self.available = False
        self.location = "none"

    def set_status(self, status: str, http_status: int):
        self.status = status
        self.http_status = http_status
        self.process_time = time.perf_counter() - self.start_at

    def set_error(self, err_message: str, err_details: str):
        self.is_err = True
        self.err_message = err_message
        self.err_details = err_details

    def is_error(self) -> bool:
        return self.is_err
    
    def set_article(self, article: str):
        self.article = str(article)

    def set_price_data(self, price: int, original_price: int, currency: str):
        self.price = price
        self.original_price = original_price
        self.currency = currency

    def set_aviable(self, available: bool):
        self.available = available

    def set_location(self, location: str):
        self.location = location

    def string(self) -> str:
        if self.is_error():
            return f"{self.url}: {self.err_message} - {self.err_details}"
        
        return f"{self.url}: {self.price} {self.original_price} {self.currency} {self.available} {self.location}"