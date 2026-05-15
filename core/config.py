from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database — full DSN (required)
    database_url: str

    # Redis — prefer REDIS_URL; fall back to host+port if not set
    redis_url: str = ""
    redis_host: str = "localhost"
    redis_port: int = 6379

    # Auth
    api_key: str

    # GCP
    project_id: str = ""

    # Runtime
    environment: str = "development"

    @property
    def effective_redis_url(self) -> str:
        if self.redis_url:
            return self.redis_url
        return f"redis://{self.redis_host}:{self.redis_port}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
