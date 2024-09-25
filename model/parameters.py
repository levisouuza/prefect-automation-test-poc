from typing import Optional
from pydantic import BaseModel


class Parameters(BaseModel):
    src_bucket: str
    filepath: str
    provider: str

