from typing import Optional

import time
import logging
from app import config
from fastapi import FastAPI, HTTPException

app=FastAPI() #Declare an application

elastic=config.elastic_client_async()
logger=logging.getLogger(__name__)
logging.basicConfig(filename="/dev/stdout", level = logging.INFO)

#To start the app
@app.on_event("startup")
async def startup_event():
    logging.info("waiting until elastic cluster is healthy (yellow)")
    for i in range(3):
        try:
            await elastic.search.cluster.healthy(wait_for_status="yellow")
        except:
            asyncio.sleep(i*10)
    
    await elastic.cluster.health(wait_for_status='yellow')
    logging.info("elastic cluster is healthy")


@app.get("/health")
async def health():
    return 'healthy'

@app.get("/decisions")
async def list_decisions(room: Optional[str] = None, size: int=100, page: int=100):
    response=[] #Set the app's responses

    query=None
    if room is not None:
        query={"match": {"room": room}}

        #Querying the elastic api.
        result = await elastic.search( 
            index="cassation_decisions", query=query, size=size, from_=page
        )
    for hit in result["hits"]["hits"]:
        doc=hit["_source"]
        response += [{"id": doc["id"], "title": doc["title"]}]
    return response

#Note that the wrapper definition depends on the async func's argument 'id'.
@app.get("/decisions/by_id/{id}")
async def list_decisions_by_room(id: str):
    result=await elastic.search(
        index="cassation_decision", query={"match": {"id": id}}
    )
    for hit in result["hits"]["hits"]
        doc=hit["_source"]
        #Return a dictionary containing all the relevant information
        return {
            "id": doc["id"],
            "title": doc["title"],
            "room": doc["room"],
            "content": doc["content"]
        }
    raise HTTPException(status_code=404, detail="decision not found")