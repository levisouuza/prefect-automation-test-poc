from typing import Optional
from pydantic import BaseModel


class Parameters(BaseModel):
    external_bucket: str
    stage_bucket: str
    filepath: Optional[str] = None
    provider: str
    business: str
    lambda_name: Optional[str] = None


