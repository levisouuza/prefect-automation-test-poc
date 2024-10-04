from typing import Optional
from pydantic import BaseModel


class Parameters(BaseModel):
    bucket_external: str
    bucket_stage: str
    bucket_sor: str
    filepath: Optional[str] = None
    provider: str
    business: str
    lambda_name_start_task: Optional[str] = None
    lambda_name_start_job: Optional[str] = None
    database_sor: str
    table_name: str


