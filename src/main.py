import os
import sys
import threading
from fastapi import FastAPI
from contextlib import asynccontextmanager

# Add the project root to sys.path so 'src' module can be resolved 
# when running `python src/main.py` directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.kafkaConsumer import consume_forever
from src.api.routes import router as api_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the Kafka consumer in a background daemon thread
    consumer_thread = threading.Thread(target=consume_forever, daemon=True)
    consumer_thread.start()
    yield
    # Any teardown logic here (like stopping the consumer if needed)

app = FastAPI(
    title="Packet-CRM API",
    description="AI-driven, self-learning service to ingest, analyze, and resolve rejected biometric packets within the UIDAI ecosystem.",
    version="1.0.0",
    lifespan=lifespan
)
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
