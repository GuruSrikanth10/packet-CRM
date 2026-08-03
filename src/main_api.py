import os
import sys
from fastapi import FastAPI
from contextlib import asynccontextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.routes import router as api_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

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
