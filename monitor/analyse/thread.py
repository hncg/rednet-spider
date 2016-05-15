# coding=utf-8
import threading
from .mood import Mood


class AThread(threading.Thread):

    def __init__(self, articles, key_mood_map):
        threading.Thread.__init__(self)
        self.articles = articles
        self.key_mood_map = key_mood_map

    def run(self):
            Mood.start(self.articles, self.key_mood_map)
