from fastapi import FastAPI,HTTPException
from pydantic import BaseModel
from datetime import datetime
import logging

from elasticsearch import Elasticsearch,exceptions as es_exceptions


# --------------------
# Models
# --------------------
class Event(BaseModel):
    source:str
    host:str
    severity:str
    message:str
    timestamp:datetime

# --------------------
# Logging
# --------------------
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger("ingestion")

# --------------------
# App
# --------------------
app=FastAPI(title="Event Ingestion API")


# --------------------
# ES Client
# --------------------
es=Elasticsearch("http://localhost:9200")
INDEX_NAME="events"


# --------------------
# Ingestion Endpoint
# --------------------
@app.post("/events")
async def ingest_event(event:Event):
    try:
        logger.info(
            "event_received",
            extra={
                "source":event.source,
                "host":event.host,
                "severity":event.severity,
                },
        )

        # Index document into Elasticsearch
        es.index(index=INDEX_NAME,document=event.dict())

        return {"status":"accepted"}

    except es_exceptions.ConnectionError:
        logger.error("elasticsearch_unavailable")
        raise HTTPException(
            status_code=503,
            detail="Search backend unavailable",
        )
    except Exception as exc:
        logger.exception("unexpected error")
        raise HTTPException(
            status_code=500,
            detail="Internal server error",
        )
                        
# --------------------
# Health Check
# --------------------
@app.get("/ping")
async def ping():
    return {"status":"ok"}
