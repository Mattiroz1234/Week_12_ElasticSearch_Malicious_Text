import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import os

class TextFeatures:
    def __init__(self):
        nltk_dir = "/tmp/nltk_data"
        os.makedirs(nltk_dir, exist_ok=True)
        nltk.data.path.append(nltk_dir)
        nltk.download('vader_lexicon', download_dir=nltk_dir, quiet=True)
        self.analyzer = SentimentIntensityAnalyzer()
        self.weapons = None
        self.name_file_weapons = "../data/weapon_list.txt"


    def find_text_sentiment(self, text: str):
        score = self.analyzer.polarity_scores(text)["compound"]
        if score > 0.5:
            return "positive"
        elif score > -0.5:
            return "neutral"
        else:
            return "negative"

    def find_weapons_in_text(self, text: str):
        found = []
        if text:
            for weapon in self.weapons:
                if weapon.lower() in text.lower():
                    found.append(weapon)
        return found

    def read_weapons(self):
        weapons = []
        with open(self.name_file_weapons, "r", encoding='utf-8-sig') as f:
            while True:
                line = f.readline().strip("\n")
                if line == "":
                    break
                weapons.append(line)
        self.weapons = weapons