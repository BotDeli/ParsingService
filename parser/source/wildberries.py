from utils.article import parse_wildberries_article
from utils.url import parse_url_params
from structs.product import ProductInfo
from structs.queue import LinkedQueue
from structs.task import ParsingTask
import structs.status as status
import loggers

import undetected_chromedriver as uc
from fake_useragent import UserAgent
from random import randrange
from threading import Event
from time import sleep
import requests
import json




# many nm
# https://www.wildberries.ru/__internal/u-card/cards/v4/detail?appType=1&curr=rub&dest=123585633&spp=30&hide_dtype=11&ab_testing=false&lang=ru&nm=211380438;211380442;211380447;214017611;211380440
# geo endpoint
# https://user-geo-data.wildberries.ru/get-geo-info?currency=RUB&latitude=55.034523&longitude=82.922891&locale=ru&address=%D0%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8%D0%B1%D0%B8%D1%80%D1%81%D0%BA%2C+%D0%A3%D0%BB%D0%B8%D1%86%D0%B0+%D0%9C%D0%B8%D1%87%D1%83%D1%80%D0%B8%D0%BD%D0%B0+9&dt=1763644292&currentLocale=ru&b2bMode=false&addressId=50042972&addressType=self

geo_dest_list = {
    "Горно-Алтайск": 123585493,
    "Благовещенск": 123585533,
    "Архангельск": 123589924,
    "Астрахань": -10106464,
    "Майкоп": 123589323,
    "Барнаул": -3224888,
    "Уфа": 12358517,
    "Белгород": 123585474,
    "Брянск": -5518648,
    "Улан-Удэ": -1979947,
    "Владимир": -3856723,
    "Волгоград": -4039473,
    "Вологда": 123586880,
    "Воронеж": 12358283,
    "Махачкала": -5921132,
    "Биробиджан": 123586043,
    "Чита": -5551586,
    "Иваново": -1586361,
    "Магас": 123585806,
    "Иркутск": -5827722,
    "Нальчик": 123586045,
    "Калининград": 123587335,
    "Элиста": 123585888,
    "Калуга": 123585487,
    "Петропавловск-Камчатский": 123585749,
    "Черкесск": 123585541,
    "Петрозаводск": 123585633,
    "Кемерово": -1172839,
    "Киров": 123589347,
    "Сыктывкар": 123585595,
    "Кострома": 123585693,
    "Краснодар": 12358062,
    "Красноярск": 12356481,
    "Курган": 123585948,
    "Курск": -2307160,
    "Липецк": -4734876,
    "Москва": -1257786,
    "Йошкар-Ола": 123586143,
    "Саранск": 123586028,
    "Магадан": 123585707,
    "Мурманск": -1965487,
    "Нижний Новгород": 12358536,
    "Великий Новгород": 123585586,
    "Новосибирск": -364763,
    "Омск": -3902910,
    "Оренбург": 12358312,
    "Орёл": 123585628,
    "Пенза": -5892277,
    "Пермь": 12358361,
    "Псков": 123585943,
    "Владивосток": 123587791,
    "Ростов-на-Дону": -2227685,
    "Рязань": -5817686,
    "Самара": -283781,
    "Санкт-Петербург": -1198059,
    "Саратов": 12358459,
    "Якутск": 123585755,
    "Южно-Сахалинск": 123586091,
    "Владикавказ": -4638357,
    "Екатеринбург": 123589409,
    "Смоленск": -3339991,
    "Ставрополь": -5856842,
    "Тамбов": 123587520,
    "Тверь": -1784077,
    "Томск": -2688922,
    "Тула": 123586853,
    "Кызыл": 123585918,
    "Тюмень": 12358470,
    "Казань": -2133462,
    "Ижевск": -971646,
    "Ульяновск": -3115289,
    "Хабаровск": -1785058,
    "Абакан": 123587564,
    "Ханты-Мансийск": 123585931,
    "Челябинск": -1581689,
    "Грозный": -3888337,
    "Чебоксары": -3889738,
    "Салехард": 123585947,
    "Ярославль": -3351953,
}

class WildberriesParser():
    def __init__(self, cfg: dict):
        all_cfg = cfg["all"]
        self.cycle_min_cooldown = all_cfg["cycle_min_cooldown_ms"]
        self.cycle_max_cooldown = all_cfg["cycle_max_cooldown_ms"]
        self.request_min_cooldown = all_cfg["request_min_cooldown_ms"]
        self.request_max_cooldown = all_cfg["request_max_cooldown_ms"]
        
        wildberries_cfg = cfg["wildberries"]
        self.chrome_version = wildberries_cfg["chrome_version"]

        self.parsing_queue = LinkedQueue()
       
        self.ua = UserAgent()
        self._init_session()
        self.update_session_token()
        
    def update_session_token(self):
        self.driver = uc.Chrome(
            version_main=142,
            user_multi_procs=True,
        )

        self.driver.get("https://www.wildberries.ru/")
        sleep(2)
        self.driver.get("https://www.wildberries.ru/")
        sleep(3)

        cookies = self.driver.get_cookies()
        for cookie in cookies:
            self.session.cookies.set(cookie["name"], cookie["value"])
        
        self.driver.quit()

    def _init_session(self):
        self.session = requests.session()
        self.session.headers.update({
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": self.ua.random,
            "Upgrade-Insecure-Requests": "1"
        })

    def start_listen_parsing_queue(self, stop_event: Event):
        while not stop_event.is_set():
            if self.parsing_queue.is_next():
                task = None
                try:
                    task = self.parsing_queue.pop()
                    self._handle_parsing_task(task, stop_event)
                except Exception as error:
                    if not task is None:
                        task.on_error("wildberries")

                    loggers.wildberries.warning(f"Skip panic parsing task: {error}")

            self._cycle_cooldown()
        
        self.session.close()

    def _handle_parsing_task(self, task: ParsingTask, stop_event: Event):
        geo_params = task.geo_params
        url = task.url
        products = []

        for location in geo_params:
            if stop_event.is_set():
                return

            try:
                product = self.parse_product_with_location(url, location)
                products.append(product)
                self._request_cooldown()
            except:
                pass

        if not stop_event.is_set():
            task.callback(products)

    def _request_cooldown(self):
        cooldown_ms = randrange(self.request_min_cooldown, self.request_max_cooldown)
        sleep(cooldown_ms / 1000)

    def _cycle_cooldown(self):
        cooldown_ms = randrange(self.cycle_min_cooldown, self.cycle_max_cooldown)
        sleep(cooldown_ms / 1000)

    def add_to_parsing_queue(self, task: ParsingTask):
        self.parsing_queue.push(task)

    def parse_product_with_location(self, url: str, location: str) -> ProductInfo:      
        product = ProductInfo(url, "wildberries")
        product.set_location(location)

        article = parse_wildberries_article(url)
        product.set_article(article)

        try: 
            api_url = self._construct_api_url(article, location)
            r = self.session.get(api_url)
            
            if r.status_code == 498:
                self.update_session_token()
                product.set_error("invalid session token", "session token is updated, retry request")
                product.set_status(status.ERR_FAIL, r.status_code)
                return product

        except requests.Timeout as err:
            product.set_status(status.ERR_TIMEOUT, -1)
            product.set_error("fail parse product", err)
            return product
        except Exception as err:
            product.set_status(status.ERR_FAIL, -1)
            product.set_error("fail parse product", err)
            return product

        try:
            data = json.loads(r.text)
            product_data = data["products"][0]
            info = {}

            params = parse_url_params(url)
            if "size" in params:
                size = params["size"][0]
                info = self._parse_product_info_with_size(product_data, size)
            else:
                info = self._parse_product_info(product_data)

            if not info["aviable"]:
                product.set_status(status.PARSING_SUCCESS, r.status_code)
                return product
            
            product.set_aviable(True)
            product.set_price_data(info["price"], info["original_price"], info["currency"])
        except Exception as err:
            product.set_status(status.ERR_FAIL, r.status_code)
            product.set_error("fail parse response data", err)
            return product

        product.set_status(status.PARSING_SUCCESS, r.status_code)
        return product
    
    def _construct_api_url(self, article: str, location: str) -> str:
        if location not in geo_dest_list.keys():
            raise ValueError("location not found")

        dest = geo_dest_list[location]
        api_url = f"https://www.wildberries.ru/__internal/u-card/cards/v4/detail?appType=1&curr=rub&dest={dest}&spp=30&hide_dtype=11&ab_testing=false&lang=ru&nm={article}"
        return api_url
    
    def _parse_product_info_with_size(self, product_data: dict, size: str) -> dict | None:
        for size_data in product_data["sizes"]:
            if str(size_data["optionId"]) == str(size):
                aviable = 0 < len(size_data["stocks"])
                if not aviable:
                    return {
                        "aviable": False,
                        "price": -1,
                        "original_price": -1,
                        "currency": '?'
                    }
                
                price_data = size_data["price"]
                price = int(int(price_data["product"]) / 100)
                original_price = int(int(price_data["basic"]) / 100)
                return {
                    "aviable": True,
                    "price": price,
                    "original_price": original_price,
                    "currency": "RUB",
                }
        
        return self._parse_product_info(product_data)

    def _parse_product_info(self, product_data: dict) -> dict:
        aviable = 0 < product_data["totalQuantity"]
        if not aviable:
            return {
                "aviable": False,
                "price": -1,
                "original_price": -1,
                "currency": '?'
            }
            
            
        for size_data in product_data["sizes"]:
            aviable = 0 < len(size_data["stocks"])
            if aviable:
                price_data = size_data["price"]
                price = int(int(price_data["product"]) / 100)
                original_price = int(int(price_data["basic"]) / 100)
                return {
                    "aviable": True,
                    "price": price,
                    "original_price": original_price,
                    "currency": "RUB",
                }

        return {
                "aviable": False,
                "price": -1,
                "original_price": -1,
                "currency": '?'
            }
    

