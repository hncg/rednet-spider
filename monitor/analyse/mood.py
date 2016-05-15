# coding=utf-8
import jieba
import re
import time
from .mood_map import mood_map
from handler.model import Mood as DbMood
from handler.model import HotWords as DBHotWords

filter_chars = [
    u',', u'！', u'_', u'\s', u'，', u'&quot',
    u';', u'；', u'▼', u'”', u"“", u'\.', u'。',
    u'：', u'…', u'、', u'/', u'[a-z]+', u'\d+',
    u':', u'-', u'？', u'\'', u'《', u'【', u'】',
    u'》', u'丶', u'@', u'）', u'（', u'\(', u'\)'
]


class Mood(object):

    @classmethod
    def start(cls, articles, key_mood_map):
        if not articles:
            print "空"
            return
        mood_time_map = {}
        city_id = articles[0].city_id
        hot_words_map = {}
        start_time = time.time()
        print "城市{}开始分析{}".format(city_id, start_time)
        for article in articles:
            content = re.compile(u"|".join(filter_chars)).sub(u'', article.content) # noqa
            seg_list = jieba.cut(content)
            time_at = article.time_at.strftime("%Y-%m-%d %H:%M:%S")[:10]
            if not mood_time_map.get(time_at):
                mood_time_map[time_at] = {}
            if not hot_words_map.get(time_at):
                hot_words_map[time_at] = {}
            article_mood = {
                "happy": 0, "good": 0, "anger": 0, "sorrow": 0,
                "fear": 0, "evil": 0, "surprise": 0
            }
            for key in seg_list:
                mood_strength = key_mood_map.get(key)
                if not mood_strength:
                    continue
                if not hot_words_map[time_at].get(key):
                    hot_words_map[time_at][key] = 1
                else:
                    hot_words_map[time_at][key] += 1
                if mood_strength['_class'] in mood_map[u'乐']:
                    article_mood['happy'] += mood_strength['strength']
                if mood_strength['_class'] in mood_map[u'好']:
                    article_mood['good'] += mood_strength['strength']
                if mood_strength['_class'] in mood_map[u'怒']:
                    article_mood['anger'] += mood_strength['strength']
                if mood_strength['_class'] in mood_map[u'哀']:
                    article_mood['sorrow'] += mood_strength['strength']
                if mood_strength['_class'] in mood_map[u'惧']:
                    article_mood['fear'] += mood_strength['strength']
                if mood_strength['_class'] in mood_map[u'恶']:
                    article_mood['evil'] += mood_strength['strength']
                if mood_strength['_class'] in mood_map[u'惊']:
                    article_mood['surprise'] += mood_strength['strength']
            for k, v in article_mood.items():
                if not mood_time_map[time_at].get(k):
                    mood_time_map[time_at][k] = v
                else:
                    mood_time_map[time_at][k] += v
        db_moods = DbMood.mget_by_city_id(city_id)
        db_mood_times = [_.time_at.strftime("%Y-%m-%d") for _ in db_moods]
        db_mood_map = {_.time_at.strftime("%Y-%m-%d"): _ for _ in db_moods}
        moods = []
        update_moods = []

        for time_at, v in hot_words_map.items():
            word_timeses = sorted(v.items(), key=lambda v: v[1], reverse=True)
            hot_words = []
            for word_times in word_timeses[:30]:
                hot_word = DBHotWords.get_by_city_id_and_time_and_word(city_id, time_at, word_times[0]) # noqa
                if hot_word:
                    DBHotWords.update(city_id, time_at, word_times[0], word_times[1] + hot_word.times) # noqa
                else:
                    hot_words.append(
                        DBHotWords(
                            city_id=city_id,
                            time_at=time_at,
                            word=word_times[0],
                            times=word_times[1]
                        )
                    )
            DBHotWords.add_all(hot_words)
        for time_at, v in mood_time_map.items():
            happy = v.get('happy') or 0
            good = v.get('good') or 0
            anger = v.get('anger') or 0
            sorrow = v.get('sorrow') or 0
            fear = v.get('fear') or 0
            evil = v.get('evil') or 0
            surprise = v.get('surprise') or 0
            cps = 2 * happy + 2 * good - 3 * anger - sorrow - 2 * fear - 3 * evil - surprise # noqa
            if time_at in db_mood_times:
                update_moods.append(
                    DbMood(
                        time_at=time_at,
                        city_id=city_id,
                        happy=happy + db_mood_map[time_at].happy,
                        good=good + db_mood_map[time_at].good,
                        anger=anger + db_mood_map[time_at].anger,
                        sorrow=sorrow + db_mood_map[time_at].sorrow,
                        fear=fear + db_mood_map[time_at].fear,
                        evil=evil + db_mood_map[time_at].evil,
                        surprise=surprise + db_mood_map[time_at].surprise,
                        cps=cps + db_mood_map[time_at].cps,
                    )
                )
            else:
                moods.append(
                    DbMood(
                        time_at=time_at,
                        city_id=city_id,
                        happy=happy,
                        good=good,
                        anger=anger,
                        sorrow=sorrow,
                        fear=fear,
                        evil=evil,
                        surprise=surprise,
                        cps=cps,
                    )
                )
        DbMood.add_all(moods)
        DbMood.update_all(update_moods)
        cost_time = int(time.time() - start_time)
        print "城市{}分析完毕,耗时{}分钟{}秒". \
            format(city_id, cost_time // 60, cost_time % 60)
