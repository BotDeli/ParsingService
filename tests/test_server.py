import unittest
from requests import session
from uuid import uuid4
from time import sleep
import json

TEST_SERVER_PROTOCOL = "http"
TEST_SERVER_ADDR = "127.0.0.1:8000"
YAML_CONFIG_PATH = "./config.yaml"

class TestServerAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session()
        cls.product_id = 0

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def setUp(self):
        self.base_url = f"{TEST_SERVER_PROTOCOL}://{TEST_SERVER_ADDR}/"
        self.create_account_endpoint = self.base_url + "/api/v1/account/create"
        self.start_tracking_endpoint = self.base_url + "/api/v1/products/add"
        self.prices_export_endpoint = self.base_url + "/api/v1/prices/export"

    def test_account_create(self):
        # test_account_empty_body
        r = self.session.post(self.create_account_endpoint)
        self.assertEqual(422, r.status_code)       

        # test_account_bad_email 
        data = {
            "email": "fail-test@email",
            "name": "username",
        }

        body = json.dumps(data)
        r = self.session.post(self.create_account_endpoint, body)
        self.assertEqual(400, r.status_code)

        # test_account_bad_name
        data = {
            "email": "test@mail.ru",
            "name": "_",
        }

        body = json.dumps(data)
        r = self.session.post(self.create_account_endpoint, body)
        self.assertEqual(400, r.status_code)

        # test_account_success_create_account
        id = str(uuid4())
        data = {
            "email": f"test_{id}@mail.ru",
            "name": f"test_{id}",
        }

        body = json.dumps(data)
        r = self.session.post(self.create_account_endpoint, body)
        self.assertEqual(201, r.status_code)
        
        data = json.loads(r.text)
        api_key = data["api_key"]
        self.assertNotEqual('', api_key)
        
        self.session.headers.update({
            "Authorization": api_key,
        })

    def test_tracking_parsing(self):
        # test_start_tracking
        data = {
            "url": "https://www.ozon.ru/product/kofe-v-zernah-tasty-coffee-natti-1-kg-715106535/",
            "geo_params": [
                "Москва",
                "Уфа",
                "Новосибирск"
            ]
        }

        body = json.dumps(data)
        r = self.session.post(self.start_tracking_endpoint, body)
        self.assertEqual(200, r.status_code)

        data = json.loads(r.text)
        self.product_id = int(data["product_id"])
        self.assertNotEqual(self.product_id, 0)

        # test_force_products
        print(f"Product ID: {self.product_id}")
        url = f"{self.base_url}/api/v1/products/{self.product_id}/force"
        r = self.session.post(url, None)
        self.assertEqual(202, r.status_code)
        data = json.loads(r.text)
        task_id = data["task_id"]

        for i in range(10):
            sleep(3)
            url = f"{self.base_url}/api/v1/tasks/{task_id}/"
            r = self.session.get(url)
            self.assertEqual(200, r.status_code)
            data = json.loads(r.text)
            if not data["is_working"]:
                break

        # test_content_products_from_price_export
        r = self.session.get(self.prices_export_endpoint)
        self.assertEqual(200, r.status_code)

        data = json.loads(r.text)
        products = data["prices_data"]
        self.assertEqual(3, len(products))

        geo_params = []
        for product in products:
            geo_params.append(product["geo"])
           
            self.assertEqual(self.product_id, int(product["product_id"]))
            self.assertNotEqual('', product["url"])
            self.assertNotEqual('', product["geo"])
            self.assertEqual("ozon", product["source"])

        self.assertEqual(len(geo_params), len(set(geo_params)))

        # test_stop_tracking
        url = self.base_url + f"/api/v1/products/{self.product_id}"
        r = self.session.delete(url)
        self.assertEqual(200, r.status_code)

        # test_empty_price_export
        r = self.session.get(self.prices_export_endpoint)
        self.assertEqual(200, r.status_code)

        data = json.loads(r.text)
        products = data["prices_data"]
        self.assertEqual(0, len(products))

    def test_rps_limiter(self):
        url = self.base_url + "/api/v1/tasks/test-tasks"
        responses = []
        for i in range(30):
            try:
                resp = self.session.get(url)
                responses.append(resp.status_code)
            except Exception as e:
                print(f"Запрос {i+1}: Ошибка - {e}")
            sleep(0.05)

        rate_limited = len([r for r in responses if r == 429])
        self.assertNotEqual(0, rate_limited)


if __name__ == "__main__":
    unittest.main()