from fastapi import FastAPI

from app.api.routes import router
from app.core.logging_config import configure_logging
from app.db.session import create_db_and_tables

configure_logging()
app = FastAPI(title="Data Engineering Challenge API", version="1.0.0")
app.include_router(router)


@app.on_event("startup")
def on_startup() -> None:
    create_db_and_tables()
