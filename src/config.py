from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_brokers: str = "localhost:9092"
    kafka_topic: str = "rejections"
    s3_bucket: str = "mock-casesheet-bucket"
    llm_model: str = "llama3"
    llm_base_url: str = "http://localhost:11434"
    
    class Config:
        env_file = ".env"

settings = Settings()
