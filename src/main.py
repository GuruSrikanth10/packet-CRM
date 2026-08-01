import threading
from fastapi import FastAPI
from contextlib import asynccontextmanager

from src.utils.kafkaConsumer import consume_forever
from src.api.routes import router as api_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the Kafka consumer in a background daemon thread
    consumer_thread = threading.Thread(target=consume_forever, daemon=True)
    consumer_thread.start()
    yield
    # Any teardown logic here (like stopping the consumer if needed)

app = FastAPI(lifespan=lifespan)
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
