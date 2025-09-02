from app.es.data_loader import Loader
from app.Processing.manager import ESUpdater

def main():
    loader = Loader()
    loader.index()

    index_name = "fake_tweets"
    updater = ESUpdater(index_name)
    updater.update_weapons()
    updater.update_sentiment()
    updater.delete()