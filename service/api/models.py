from pydantic import BaseModel


class AccountCreateRequest(BaseModel):
    email: str
    name: str

class AccountCreateResponse(BaseModel):
    api_key: str

class StartTrackingRequest(BaseModel):
    url: str
    geo_params: list[str]

class StartTrackingResponse(BaseModel):
    product_id: int

class ForceResponse(BaseModel):
    product_id: int
    task_id: str

class AllForceResponse(BaseModel):
    forces: list[ForceResponse]

class PricesExportResponse(BaseModel):
    prices_data: list

class TaskTrackingResponse(BaseModel):
    task_id: str
    is_working: bool