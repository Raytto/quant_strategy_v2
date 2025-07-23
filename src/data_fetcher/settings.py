from pydantic_settings import BaseSettings
from pydantic import SecretStr


class Settings(BaseSettings):
    tushare_api_token: SecretStr

    class Config:
        env_file = ".env"


settings = Settings()
