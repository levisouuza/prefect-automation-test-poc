from pydantic import BaseModel


class Config(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str

