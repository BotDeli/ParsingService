from utils.price import parse_price, parse_currency, format_currency
from utils.article import parse_ozon_article
from structs.product import ProductInfo
from structs.queue import LinkedQueue
from structs.task import ParsingTask
import structs.status as status
import loggers

from undetected_chromedriver.webelement import By
import undetected_chromedriver as uc
from random import randrange
from threading import Event
from time import sleep
import json


all_geo_params = ['Горно-Алтайск', 'Благовещенск', 'Архангельск', 'Астрахань', 'Майкоп', 'Барнаул', 'Уфа', 'Белгород', 'Брянск', 'Улан-Удэ', 'Владимир', 'Волгоград', 'Вологда', 'Воронеж', 'Махачкала', 'Биробиджан', 'Чита', 'Иваново', 'Магас', 'Иркутск', 'Нальчик', 'Калининград', 'Элиста', 'Калуга', 'Петропавловск-Камчатский', 'Черкесск', 'Петрозаводск', 'Кемерово', 'Киров', 'Сыктывкар', 'Кострома', 'Краснодар', 'Красноярск', 'Курган', 'Курск', 'Липецк', 'Москва', 'Йошкар-Ола', 'Саранск', 'Магадан', 'Мурманск', 'Нижний Новгород', 'Великий Новгород', 'Новосибирск', 'Омск', 'Оренбург', 'Орёл', 'Пенза', 'Пермь', 'Псков', 'Владивосток', 'Ростов-на-Дону', 'Рязань', 'Самара', 'Санкт-Петербург', 'Саратов', 'Якутск', 'Южно-Сахалинск', 'Владикавказ', 'Екатеринбург', 'Смоленск', 'Ставрополь', 'Тамбов', 'Тверь', 'Томск', 'Тула', 'Кызыл', 'Тюмень', 'Казань', 'Ижевск', 'Ульяновск', 'Хабаровск', 'Абакан', 'Ханты-Мансийск', 'Челябинск', 'Грозный', 'Чебоксары', 'Салехард', 'Ярославль']


class OzonParser():
    def __init__(self, cfg: dict):
        all_cfg = cfg["all"]
        self.cycle_min_cooldown = all_cfg["cycle_min_cooldown_ms"]
        self.cycle_max_cooldown = all_cfg["cycle_max_cooldown_ms"]
        self.request_min_cooldown = all_cfg["request_min_cooldown_ms"]
        self.request_max_cooldown = all_cfg["request_max_cooldown_ms"]

        ozon_cfg = cfg["ozon"]
        self.chrome_version = ozon_cfg["chrome_version"]
        self.waiting_element_loading = ozon_cfg["waiting_element_loading_sec"]
        self.page_load_timeout = ozon_cfg["page_load_timeout_sec"]
        self.page_load_cooldown = ozon_cfg["page_load_cooldown_sec"]

        self.parsing_queue = LinkedQueue()
        self.geo_urls = {}
        self.geo_cookies = {}

        self._init_driver()
        self._init_all_geo_urls()
        self._init_all_geo_cookies()

    def _init_driver(self):
        self.driver = uc.Chrome(
            version_main=self.chrome_version, 
            user_multi_procs=True,
        )
        
        self.driver.implicitly_wait(self.waiting_element_loading)
        self.driver.set_page_load_timeout(self.page_load_timeout)
        self._init_get()

    def _init_get(self):
        self.driver.get("https://ozon.ru")
        self._set_adult_cookie()
        sleep(2)

    def _init_all_geo_urls(self):
        try:
            with open("ozon_geo_urls.json", 'r') as file:
                self.geo_urls = json.load(file)
        except:
            pass
        
        for location in all_geo_params:
            if location not in self.geo_urls:
                try:
                    self._parse_geo_url(location)
                except Exception as error:
                    loggers.ozon.warning(f"fail parse geo url '{location}': {error}")

        with open("ozon_geo_urls.json", 'w') as file:
            json.dump(self.geo_urls, file, indent=4, ensure_ascii=False)

    def _parse_geo_url(self, location: str):
        self.driver.delete_all_cookies()
        self.driver.get("https://www.ozon.ru/")
        sleep(1)

        el = self.driver.find_element(By.CSS_SELECTOR, "[data-widget='addressBookBarWeb']")
        el.click()
        sleep(1)

        btn_els = self.driver.find_elements(By.TAG_NAME, "button")
        for _el in btn_els:
            if _el.text.strip().startswith("Выбрать на карте"):
                _el.click()
                break

        sleep(2)

        textarea_el = self.driver.find_element(By.TAG_NAME, "textarea")
        for i in range(10):
            try:
                textarea_el.click()
                sleep(1)
                textarea_el.send_keys(location)
                sleep(1)
                break
            except:
                sleep(0.5)
        
        sleep(2)
        el = self.driver.find_element(By.XPATH, f'//*[contains(@role, "option")][contains(@title, "{location}")]')
        el.click()
        sleep(2)

        try:
            pvz_el = self.driver.find_element(By.CSS_SELECTOR, "[data-widget='pvzList']")

        except:
            ctr_plus = self.driver.find_element(By.CSS_SELECTOR, "[data-tid=om-zoom-control-plus]")
            ctr_minus = self.driver.find_element(By.CSS_SELECTOR, "[data-tid=om-zoom-control-minus]")

            for i in range(10):
                ctr_plus.click()
                sleep(0.1)

            for i in range(5):
                ctr_minus.click()
                try:
                    pvz_el = self.driver.find_element(By.CSS_SELECTOR, "[data-widget='pvzList']")
                    break
                except:
                    continue


        el = pvz_el.find_element(By.TAG_NAME, 'a')
        geo_url = el.get_attribute("href")
        self.geo_urls[location] = geo_url

    def _init_all_geo_cookies(self):
        try:
            with open("ozon_geo_cookies.json", 'r') as file:
                self.geo_cookies = json.load(file)
        except:
            pass

        for location in all_geo_params:
            if location not in self.geo_cookies:
                self._update_geo_cookies(location)

    def _update_geo_cookies(self, location: str):
        try:
            self._parse_location_cookies(location)
            with open("ozon_geo_cookies.json", 'w') as file:
                json.dump(self.geo_cookies, file, indent=4, ensure_ascii=False)
        except Exception as error:
            loggers.ozon.warning(f"fail parse geo cookies '{location}': {error}")

    def _parse_location_cookies(self, location: str):
        self.driver.delete_all_cookies()
        geo_url = self.geo_urls[location]
        self.driver.get(geo_url)
        sleep(1)

        btn_els = self.driver.find_elements(By.TAG_NAME, "button")
        for _el in btn_els:
            if _el.text.strip() == "Заберу отсюда":
                for i in range(5):
                    try:
                        _el.click()
                        break
                    except:
                        sleep(1)

        self.geo_cookies[location] = [
            {"name": "__Secure-ab-group", "value": self.driver.get_cookie("__Secure-ab-group")["value"]},
            {"name": "__Secure-access-token", "value": self.driver.get_cookie("__Secure-access-token")["value"]},
            {"name": "__Secure-ETC", "value": self.driver.get_cookie("__Secure-ETC")["value"]},
            {"name": "__Secure-ext_xcid", "value": self.driver.get_cookie("__Secure-ext_xcid")["value"]},
            {"name": "__Secure-refresh-token", "value": self.driver.get_cookie("__Secure-refresh-token")["value"]},
            {"name": "__Secure-user-id", "value": self.driver.get_cookie("__Secure-user-id")["value"]},
            {"name": "_Location", "value": location},
        ]

    def start_listen_parsing_queue(self, stop_event: Event):
        while not stop_event.is_set():
            if self.parsing_queue.is_next():
                task = None
                try:
                    task = self.parsing_queue.pop()
                    self._handle_parsing_task(task, stop_event)
                except Exception as error:
                    if not task is None:
                        task.on_error("ozon")

                    loggers.ozon.warning(f"Skip panic parsing task: {error}")

            self._cycle_cooldown()
        
        self._close()

    def _close(self):
        self.driver.quit()
        with open("ozon_geo_cookies.json", "w") as file:
            json.dump(self.geo_cookies, file, indent=4, ensure_ascii=False)

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
        product = ProductInfo(url, "ozon")

        article = parse_ozon_article(url)
        product.set_article(article)
        product.set_location(location)
        
        try:
            self._set_location(location)
            self._set_adult_cookie()
            self.driver.refresh()
            sleep(self.page_load_cooldown)

            api_url = self._construct_api_url(url)
            self.driver.get(api_url)
            sleep(self.page_load_cooldown)

        except Exception as err:
            product.set_status(status.ERR_FAIL, -1)
            product.set_error("fail parse product", err)
            return product
        
        try:
            html = self.driver.page_source
            text = html[html.index('{'):len(html)-64]
            data = json.loads(text)

            if location != data["location"]["current"]["city"]:
                self.driver.refresh()
                sleep(self.page_load_cooldown)
                html = self.driver.page_source
                text = html[html.index('{'):len(html)-64]
                data = json.loads(text)

        except Exception as err:
            if self._is_anti_bot_defender():
                product.set_status(status.ERR_CAPTCHA, 403)
                product.set_error("anti bot defender", "")
            
            else:
                product.set_status(status.ERR_CHANGED_MARKUP, 200)
                product.set_error("fail parse json data", err)

            return product

        if location != data["location"]["current"]["city"]:
            del self.geo_cookies[location]
            product.set_status(status.ERR_FAIL, 200)
            product.set_error("fail parse product from location", "server location params is dont correct. Location params is started update")
            return product

        widgets = data["widgetStates"]
        webprice_data = {}
        for w in widgets.keys():
            if w.startswith("webOutOfStock"):
                product.set_status(status.PARSING_SUCCESS, 200)
                return product
            
            if w.startswith("webPrice-"):
                webprice_text = widgets[w]
                webprice_data = json.loads(webprice_text)

        try:
            price = parse_price(webprice_data["price"])

            original_price = price
            if "originalPrice" in webprice_data:
                original_price = parse_price(webprice_data["originalPrice"])

            currency = parse_currency(webprice_data["price"])
            currency = format_currency(currency)
        except Exception as err:
            product.set_status(status.ERR_CHANGED_MARKUP, 200)
            product.set_error("fail parse produt price data", err)
            return product

        product.set_aviable(True)
        product.set_price_data(price, original_price, currency)
        product.set_status(status.PARSING_SUCCESS, 200)
        return product

    def _construct_api_url(self, url: str) -> str:
        ind = url.find('/product')
        if ind == -1:
            raise ValueError("undefined product format url")
        
        url = url[ind:]

        ind = url.find('?')
        if ind != -1:
            url = url[:ind]

        return f"https://www.ozon.ru/api/composer-api.bx/page/json/v2?url={url}"
    
    # Для парсинга товаров 18+
    def _set_adult_cookie(self):
        self.driver.add_cookie({'name': 'is_adult_confirmed','value': 'true'})
        self.driver.add_cookie({'name': 'adult_user_birthdate','value': '2000-12-22'})

    def _set_location(self, location: str):
        self.driver.delete_all_cookies()
        if location not in self.geo_cookies:
            self._update_geo_cookies(location)

        self._set_location_cookies(location)

    def _set_location_cookies(self, location: str):
        self.driver.delete_all_cookies()
        cookies = self.geo_cookies[location]
        for cookie in cookies:
            self.driver.add_cookie(cookie)    

    def _is_anti_bot_defender(self) -> bool:
        html = self.driver.page_source
        return html.find("id=\"captcha-input\"") != -1 or html.find("id=\"challenge\"") != -1 or html.find("<meta name=\"robots\"") != -1 or html.find("����") != -1

