from fastapi import FastAPI
from elasticsearch import Elasticsearch
import uvicorn

app = FastAPI()
es = Elasticsearch("http://localhost:9200")
index_name = "fake_tweets"


@app.get("/antisemitic_with_weapons")
def antisemitic_with_weapons():
    res = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": 1}},
                        {"script": {
                            "script": "ctx._source.weapons_found != null && ctx._source.weapons_found.size() > 0"}}
                    ]
                }
            }
        },
        size=1000
    )

    if res['hits']['total']['value'] == 0:
        return {"message": "Data not fully processed yet."}

    return [doc["_source"] for doc in res['hits']['hits']]


@app.get("/documents_multiple_weapons")
def documents_multiple_weapons():
    res = es.search(
        index=index_name,
        body={
            "query": {
                "script": {
                    "script": "ctx._source.weapons_found != null && ctx._source.weapons_found.size() >= 2"
                }
            }
        },
        size=1000
    )

    if res['hits']['total']['value'] == 0:
        return {"message": "Data not fully processed yet."}

    return [doc["_source"] for doc in res['hits']['hits']]

def run():
    if __name__ == "__main__":
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
