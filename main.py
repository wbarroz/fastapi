from fastapi import FastAPI,HTTPException,BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import logging
import time

from elasticsearch import Elasticsearch,exceptions as es_exceptions

from typing import Optional

from pymongo import MongoClient

from bson import ObjectId

mongo_client=MongoClient("mongodb://localhost:27017")
mongo_db=mongo_client["security"]
events_collection=mongo_db["events"]


def index_with_retry(es_client,index:str,document:dict,attempts:int=3):
    for attempt in range(1,attempts+1):
        try:
            es_client.index(index=index,document=document)
            return
        except es_exceptions.ConnectionError as exc:
            logger.warning(
                    "es_index_retry",
                    extra={"attempt":attempt+1,"max_attempts":attempts},
            )
            if attempt==attempts:
                raise
            time.sleep(attempt) # simple linear backoff


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

@app.get("/mongo/events")
async def list_events(limit:int=5):
    docs=events_collection.find().limit(limit)
    events=[]
    for doc in docs:
        doc["_id"]=str(doc["_id"])
        events.append(doc)

    return events

def enrich_event(event:dict):
    # Placeholder for enrichment logic
    logger.info("event_enriched",extra={"host":event["host"]})

# --------------------
# Ingestion Endpoint
# --------------------
@app.post("/events")
async def ingest_event(event:Event,background_tasks:BackgroundTasks):
    try:
        event_dict=event.dict()

        # store in MongoDB(source of "truth")
        events_collection.insert_one(event_dict)

        # Index into Elasticsearch(analytics)
        #es.index(index=INDEX_NAME,document=event_dict) ### This one is wrong!!!
        #es.index(index=INDEX_NAME,document=event.dict())
        # Elasticsearch: analytics (with retry)
        #index_with_retry(es, INDEX_NAME, event_dict)
        index_with_retry(es, INDEX_NAME, event.dict())

        # Async enrichment
        # background_tasks.add_task(enrich_event, event_dict)
        background_tasks.add_task(enrich_event, event.dict())

        logger.info(
            "event_ingested",
            extra={
                "source":event.source,
                "host":event.host,
                "severity":event.severity,
                },
        )

        # Index document into Elasticsearch
        #es.index(index=INDEX_NAME,document=event.dict())
        #es.index(index=INDEX_NAME,document=event_dict)

        return {"status":"accepted"}

        '''
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
        '''
    except es_exceptions.ConnectionError:
        logger.error("elasticsearch_unavailable_after_retries")
        raise HTTPException(
            status_code=503,
            detail="Search backend unavailable",
        )

    except Exception:
        logger.exception("ingestion_failed")
        raise HTTPException(
            status_code=500,
            detail="Ingestion failed",
        )


# --------------------
# Query Endpoint
# --------------------
@app.get("/events")
async def query_events(severity:Optional[str]=None):
    try:
        query={"match_all":{}}

        if severity:
            query={
                    "term":{
                    "severity":severity
                    }
            }
        response=es.search(
                index=INDEX_NAME,
                query=query,
                size=10
        )

        hits=[
            hit["_source"]
            for hit in response["hits"]["hits"]
        ]

        return{
            "count":len(hits),
            "events":hits,
        }

    except es_exceptions.ConnectionError:
        logger.error("elasticsearch_unavailable"),
        raise HTTPException(
            status_code=503,
            detail="Search backend unavailable",
        )
    except Exception:
        logger.exception("query_failed")
        raise HTTPException(
            status_code=500,
            detail="Query failed",
        )
                        
# --------------------
# Health Check
# --------------------
@app.get("/ping")
async def ping():
    return {"status":"ok"}
