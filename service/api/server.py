from fastapi import FastAPI, status, HTTPException, Header, Depends, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded
from starlette.responses import JSONResponse
from slowapi.util import get_remote_address
from typing import Callable
import uvicorn
import time

from utils.url import format_url, parse_url_source
from utils.article import parse_product_article
from service.core import ServiceCore
import service.api.models as models


REDIS_HOST = "localhost"
REDIS_PORT = 6379

all_geo_params = ['Горно-Алтайск', 'Благовещенск', 'Архангельск', 'Астрахань', 'Майкоп', 'Барнаул', 'Уфа', 'Белгород', 'Брянск', 'Улан-Удэ', 'Владимир', 'Волгоград', 'Вологда', 'Воронеж', 'Махачкала', 'Биробиджан', 'Чита', 'Иваново', 'Магас', 'Иркутск', 'Нальчик', 'Калининград', 'Элиста', 'Калуга', 'Петропавловск-Камчатский', 'Черкесск', 'Петрозаводск', 'Кемерово', 'Киров', 'Сыктывкар', 'Кострома', 'Краснодар', 'Красноярск', 'Курган', 'Курск', 'Липецк', 'Москва', 'Йошкар-Ола', 'Саранск', 'Магадан', 'Мурманск', 'Нижний Новгород', 'Великий Новгород', 'Новосибирск', 'Омск', 'Оренбург', 'Орёл', 'Пенза', 'Пермь', 'Псков', 'Владивосток', 'Ростов-на-Дону', 'Рязань', 'Самара', 'Санкт-Петербург', 'Саратов', 'Якутск', 'Южно-Сахалинск', 'Владикавказ', 'Екатеринбург', 'Смоленск', 'Ставрополь', 'Тамбов', 'Тверь', 'Томск', 'Тула', 'Кызыл', 'Тюмень', 'Казань', 'Ижевск', 'Ульяновск', 'Хабаровск', 'Абакан', 'Ханты-Мансийск', 'Челябинск', 'Грозный', 'Чебоксары', 'Салехард', 'Ярославль']


class ServiceServer():
    def __init__(self, core: ServiceCore, cfg: dict):
        self.host = cfg["host"]
        self.port = cfg["port"]

        self.core = core
        self.app = FastAPI()

        self._init_limiter()
        self._init_middleware()
        self._init_endpoints()

    def _init_limiter(self):
        self.limiter = Limiter(
            key_func=get_remote_address,
            storage_uri=f"redis://{REDIS_HOST}:{REDIS_PORT}/1",
            default_limits=["10/second"]
        )

        self.app.state.limiter = self.limiter
        self.app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        self.app.add_middleware(SlowAPIMiddleware)

    def _get_request_key(self, r: Request) -> str:
        authorization = r.headers.get("Authorization")
        if authorization is None:
            return get_remote_address(r)

        return extract_api_key(authorization)

    def _init_middleware(self):
        @self.app.middleware("http")
        async def http_middleware(request: Request, call_next: Callable):
            addr = get_remote_address(request)
            if self.core.storage.ban_list.is_banned_ip(addr):
                self.core.metrics.server_total_requests.labels(status_code=403).inc()

                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={
                        "detail": "ip address is banned",
                    }
                )
            
            start_at = time.perf_counter()
            r = await call_next(request)
            process_time = time.perf_counter() - start_at

            self.core.metrics.server_total_requests.labels(status_code=str(r.status_code)).inc()
            self.core.metrics.server_request_duration.labels(status_code=str(r.status_code)).observe(process_time)            
            return r

    def start_listener(self):
        uvicorn.run(
            app=self.app,
            host=self.host,
            port=self.port,
        )

    def _init_endpoints(self):
        @self.app.post("/api/v1/account/create", response_model=models.AccountCreateResponse, status_code=status.HTTP_201_CREATED)
        async def create_account(r: models.AccountCreateRequest):
            if len(r.name) < 3:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "min name length = 3",
                    }
                )
            
            
            try:
                api_key = self.core.storage.account.create_account(r.email, r.name)
                return models.AccountCreateResponse(
                    api_key=api_key,
                )

            except Exception as error:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": str(error),
                    }
                )

        @self.app.post("/api/v1/products/add",  response_model=models.StartTrackingResponse)
        async def start_tracking_url(r: models.StartTrackingRequest, account_id: int = Depends(self.verify_account)):
            url = r.url.strip().strip()
            if not url.startswith("https://"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "dont supported url protocol",
                    }
                )

            url = format_url(url)
            if url == '':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "dont correct url format",
                    }
                )
            
            source = parse_url_source(url)
            if source == "":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "undefined url domain",
                    }
                )

            article = parse_product_article(url)
            if article == "":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "undefined product article",
                    }
                )
            

            geo_params = r.geo_params
            if source == "yandex_market":
                geo_params = ["none"]
            else:
                is_correct, err_location = is_correct_geo_params(geo_params)            
                if not is_correct:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": err_location,
                        }
                    )
            
            product_id = self.core.storage.product.save_product(url, article, source)
            self.core.storage.tracking.start_tracking(account_id, product_id, geo_params)
            return models.StartTrackingResponse(
                product_id=product_id,
            )
        
        @self.app.delete("/api/v1/products/{product_id}")
        async def stop_tracking_url(product_id: int, account_id: int = Depends(self.verify_account)):
            self.core.storage.tracking.stop_tracking(account_id, product_id)

        @self.app.post("/api/v1/products/{product_id}/force", response_model=models.ForceResponse, status_code=status.HTTP_202_ACCEPTED)
        async def force_product_data(product_id: int, account_id: int = Depends(self.verify_account)):
            geo_params = self.core.storage.tracking.get_tracking_geo_params(account_id, product_id)
            if len(geo_params) == 0:
                raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "geo_params is empty",
                        }
                    )
                
            task_id = self.core.add_parsing_task_with_product_id(product_id, geo_params)
            if task_id is None:
                raise HTTPException(
                        status_code=status.HTTP_406_NOT_ACCEPTABLE,
                        detail={
                            "error": "fail execute parsing task to queue",
                        }
                    )
            
            return models.ForceResponse(
                product_id=product_id,
                task_id=task_id,
            )

        @self.app.post("/api/v1/force_all", response_model=models.AllForceResponse, status_code=status.HTTP_202_ACCEPTED)
        async def force_all_product_data(account_id: int = Depends(self.verify_account)):
            tracking_data = self.core.storage.tracking.get_tracking_data(account_id)

            forces = []
            for (product_id, geo_params) in tracking_data:
                if len(geo_params) == 0:
                    continue

                task_id = self.core.add_parsing_task_with_product_id(product_id, geo_params)
                if not task_id is None:
                    forces.append(models.ForceResponse(
                        product_id=product_id,
                        task_id=task_id,
                    ))

            return models.AllForceResponse(
                forces=forces,
            )

        @self.app.get("/api/v1/prices/export", response_model=models.PricesExportResponse)
        async def export_products_data(account_id: int = Depends(self.verify_account)):
            prices_data = self.core.storage.export_prices_data(account_id)
            return models.PricesExportResponse(
                prices_data=prices_data
            )
        
        @self.app.get("/api/v1/tasks/{task_id}", response_model=models.TaskTrackingResponse)
        async def get_task_tracking_status(task_id: str):
            is_working = self.core.is_working_task(task_id)
            return models.TaskTrackingResponse(
                task_id=task_id,
                is_working=is_working,
            )
        
    async def verify_account(self, authorization: str = Header(...)) -> int:
        if not authorization:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

        api_key = extract_api_key(authorization)
        if api_key == '':
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
        
        account_id = self.core.storage.account.get_account_id(api_key)
        if account_id is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
        
        return account_id


def extract_api_key(authorization: str) -> str:
    if authorization.startswith("Bearer "):
        return authorization[7:]
    return authorization

def is_correct_geo_params(geo_params: list[str]) -> tuple[bool, str]:
    if geo_params is None or len(geo_params) == 0:
        return False, "geo_params is not set"

    for geo in geo_params:
        if not geo in all_geo_params:
            return False, f"undefined {geo} location"
        
    return True, ""