from elasticsearch import Elasticsearch, helpers
from es_connection import ESConnection
import pandas as pd

class Loader:
    def __init__(self):

        self.data = pd.read_csv("../data/tweets_injected 3.csv")
        self.es = ESConnection().get_client()
        self.index_name = "fake_tweets"

    def index(self):

        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)

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

        self.es.indices.create(index=self.index_name, body=mapping)
        print("Index created with custom mapping")

        actions = (
            {
                "_index": self.index_name,
                "_source": {
                    "text": row.text,
                    "CreateDate": row.CreateDate,
                    "TweetID": row.TweetID,
                    "Antisemitic": row.Antisemitic
                }
            }
            for row in self.data.itertuples()
        )

        helpers.bulk(self.es, actions)
        print("Data indexed successfully")
