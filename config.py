from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    HOST: str
    PORT: int
    ENVIRONMENT: str

    # MongoDB
    MONGO_HOST: str
    MONGO_PORT: int
    MONGO_USERNAME: str
    MONGO_PASSWORD: str
    DATABASE: str
    COLLECTION: str

    # Reuters
    REUTERS_USERNAME: str
    REUTERS_PASSWORD: str

    class Config:
        env_file = ".env"


settings = Settings()
