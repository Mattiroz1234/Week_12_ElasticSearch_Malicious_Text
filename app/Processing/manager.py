from app.es.es_connection import ESConnection
from processer import TextFeatures
from elasticsearch.helpers import bulk

class ESUpdater:
    def __init__(self, index_name: str):
        self.index_name = index_name
        self.es = ESConnection().get_client()
        self.features = TextFeatures()

    def update_weapons(self):
        script = """
        List found = new ArrayList();
        if (ctx._source.containsKey('text') && ctx._source.text != null) {
            try {
                String t = ctx._source.text.toString();
                for (w in params.weapons) {
                    if (t.toLowerCase().contains(w.toLowerCase())) {
                        found.add(w);
                    }
                }
            } catch (Exception e) {

            }
        }
        ctx._source.weapons_found = found;
        """

        body = {
            "script": {
                "source": script,
                "lang": "painless",
                "params": {"weapons": ["knife", "gun", "bomb", "rifle", "grenade"]}
            },
            "query": {"match_all": {}},
            "conflicts": "proceed"
        }

        self.es.update_by_query(index="fake_tweets", body=body, refresh=True)

    def update_sentiment(self, batch_size=500):
        res = self.es.search(
            index=self.index_name,
            query={"match_all": {}},
            size=batch_size,
            scroll='2m'
        )
        scroll_id = res['_scroll_id']
        total_docs = res['hits']['total']['value']

        print(f"Updating sentiment for {total_docs} documents...")
        actions = []

        while True:
            hits = res['hits']['hits']
            if not hits:
                break

            for doc in hits:
                text = doc['_source'].get("text", "")
                sentiment = self.features.find_text_sentiment(text)
                actions.append({
                    "_op_type": "update",
                    "_index": self.index_name,
                    "_id": doc['_id'],
                    "doc": {"sentiment": sentiment}
                })

            bulk(self.es, actions)
            actions = []

            res = self.es.scroll(scroll_id=scroll_id, scroll='2m')

        print("Sentiment updated")

    def delete(self):

        query = {
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {"Antisemitic": 1}}
                    ],
                    "filter": [
                        {"script": {
                            "script": "ctx._source.weapons_found == null || ctx._source.weapons_found.size() == 0"}},
                        {"terms": {"sentiment.keyword": ["neutral", "positive"]}}
                    ]
                }
            }
        }

        self.es.delete_by_query(index=self.index_name, body=query)
        print("Non-relevant documents deleted")



