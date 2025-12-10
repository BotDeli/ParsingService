from utils.article import parse_yandex_market_article
from utils.price import format_currency
from structs.product import ProductInfo
from structs.queue import LinkedQueue
from structs.task import ParsingTask
import structs.status as status
import loggers

from fake_useragent import UserAgent
from random import randrange
from threading import Event
from time import sleep
import requests
import json


class YandexMarketParser():
    def __init__(self, cfg: dict):
        all_cfg = cfg["all"]
        self.cycle_min_cooldown = all_cfg["cycle_min_cooldown_ms"]
        self.cycle_max_cooldown = all_cfg["cycle_max_cooldown_ms"]

        self.parsing_queue = LinkedQueue()
       
        self.ua = UserAgent()
        self._init_session()
        
    def _init_session(self):
        self.session = requests.session()
        self.session.headers.update({
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": self.ua.random,
            "Upgrade-Insecure-Requests": "1"
        })

        self._set_adult_cookie()

    def _set_adult_cookie(self):
        r = self.session.get("https://market.yandex.ru/")
        for cookie in r.cookies:
            if cookie.name == "yandexuid":
                self.session.cookies.set("adult", f":{cookie.value}:ADULT")
                break

    def start_listen_parsing_queue(self, stop_event: Event):
        while not stop_event.is_set():
            if self.parsing_queue.is_next():
                task = None
                try:
                    task = self.parsing_queue.pop()
                    product = self.parse_product(task.url)
                    if not stop_event.is_set():
                        task.callback([product])

                except Exception as error:
                    if not task is None:
                        task.on_error("yandex_market")

                    loggers.yandex_market.warning(f"Skip panic parsing task: {error}")

            self._cycle_cooldown()

        self.session.close()

    def _cycle_cooldown(self):
        cooldown_ms = randrange(self.cycle_min_cooldown, self.cycle_max_cooldown)
        sleep(cooldown_ms / 1000)

    def add_to_parsing_queue(self, task: ParsingTask):
        self.parsing_queue.push(task)

    def parse_product(self, url: str) -> ProductInfo:
        product = ProductInfo(url, "yandex_market")

        article = parse_yandex_market_article(url)
        product.set_article(article)

        try:
            r = self.session.get(url)
        except requests.Timeout as err:
            product.set_status(status.ERR_TIMEOUT, -1)
            product.set_error("fail parse product", err)
            return product
        except Exception as err:
            product.set_status(status.ERR_FAIL, -1)
            product.set_error("fail parse product", err)
            return product

        try:
            info = self._parse_info(r.text)
            if 0 < info["quantity"]:
                product.set_aviable(True)
                product.set_price_data(info["price"], info["original_price"], info["currency"])
            
            product.set_status(status.PARSING_SUCCESS, r.status_code)
        except Exception as err:
            product.set_status(status.ERR_FAIL, r.status_code)
            product.set_error("fail fetch product info", err)

        return product
    
    def _parse_info(self, html: str) -> str:
        s_ind = html.index("""{"widgets":{"@marketfront/ProductCartButton":""")
        e_ind = html.index("""</noframes>""", s_ind)
        text = html[s_ind:e_ind]

        data = json.loads(text)
        collections = data["collections"]

        buy_opts = collections["buyOption"]
        if len(buy_opts) == 0:
            return {
            "quantity": 0,
            "price": None,
            "original_price": None,
            "currency": None,
        }

        bi = buy_opts[list(buy_opts.keys())[0]]
        
        quantity = bi["maximum"]
        price = int(bi["price"]["value"])
        original_price = int(bi["basePrice"]["value"])
        
        currency = bi["price"]["currency"]
        currency = format_currency(currency)

        return {
            "quantity": quantity,
            "price": price,
            "original_price": original_price,
            "currency": currency,
        }
