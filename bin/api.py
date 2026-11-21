import uvicorn

from src.fastapi import create_app
from src.trace_context import configure_trace_logging, configure_uvicorn_logging

if __name__ == "__main__":
    uvicorn.run("bin.api:app", host="0.0.0.0", port=8000)
else:
    configure_trace_logging()
    configure_uvicorn_logging()
    app = create_app()
