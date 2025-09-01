from elasticsearch import Elasticsearch, helpers
import pandas as pd

data = pd.read_csv("data/tweets_injected 3.csv")

es = Elasticsearch("http://localhost:9200")

index_name = "fake_tweets"

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

mapping = {
    "mappings": {
        "properties": {
            "text": {"type": "text"},
            "CreateDate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "TweetID": {"type": "keyword"},
            "Antisemitic": {"type": "integer"}
        }
    }
}

es.indices.create(index=index_name, body=mapping)
print("Index created with custom mapping")

actions = (
    {
        "_index": index_name,
        "_source": {
            "text": row.text,
            "CreateDate": row.CreateDate,
            "TweetID": row.TweetID,
            "Antisemitic": row.Antisemitic
        }
    }
    for row in data.itertuples()
)

helpers.bulk(es, actions)
print("Data indexed successfully")
