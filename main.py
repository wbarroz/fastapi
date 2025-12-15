from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
import logging

class Event(BaseModel):
    source:str
    host:str
    severity:str
    message:str
    timestamp:datetime

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger("ingestion")

app=FastAPI()

@app.post("/events")
async def ingest_event(event:Event):
    logger.info(
        "event_received",
        extra={
            "source":event.source,
            "host":event.host,
            "severity":event.severity,
            },
    )
    return {"status":"accepted"}

@app.get("/ping")
async def ping():
    return {"status":"ok"}
