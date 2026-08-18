import asyncio
import os
import sys
from fastapi import FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.routes import drain_and_shutdown
from src.api.routes import router as api_router
from src.utils.config_validator import validate_config

validate_config()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup, then a bounded drain on the way down.

    The shutdown half was an empty yield: uvicorn stopped accepting
    connections while agent.invoke() kept running on non-daemon threads until
    SIGKILL, leaving an IN_PROGRESS status.json behind for every interrupted
    packet and blocking its reprocessing for MAX_IN_PROGRESS_AGE_SECONDS (G9).
    """
    yield
    # Run the drain off the event loop: it blocks for up to
    # API_SHUTDOWN_DRAIN_SECONDS and would otherwise stall the loop that the
    # in-flight investigations are still being awaited on.
    await asyncio.to_thread(drain_and_shutdown)

app = FastAPI(
    title="Packet-CRM API",
    description="AI-driven, self-learning service to ingest, analyze, and resolve rejected biometric packets within the UIDAI ecosystem.",
    version="1.0.0",
    lifespan=lifespan
)
app.include_router(api_router)

# DLT analysis (DLT_PLAN.md). A separate router on the same app: the flow is
# parallel to the rejection pipeline, not part of it.
from src.api.dlt_routes import router as dlt_router  # noqa: E402
app.include_router(dlt_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
