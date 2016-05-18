# coding=utf-8
import threading
import time
from utils import cg_core
from spider.thread import MyThread
from analyse.thread import AThread
from handler.model import (
    City,
    Article
)

thread_limit = 4    # 最大爬虫线程数
thread_limit_time = 60 * 1    # 爬虫限制等待时间等待抓取

if __name__ == '__main__':
    key_mood_map = cg_core.get_key_mood_map("../doc/", "feel.xlsx")
    citys = City.mget()
    articles = Article.mget_latest_time_at()
    latest_time_at_map = {articles[1]: articles[0] for
                          articles in articles}
    for city in citys:
        if threading.activeCount() > thread_limit:
            print "最大爬虫线程数只能为{}个, 线程休眠{}秒". \
                format(thread_limit, thread_limit_time)
            time.sleep(thread_limit_time)
        tids = [int(tid[0]) for tid in Article.mget_tids_by_city_id(city.id)]
        MyThread(city, latest_time_at_map.get(city.id), tids).start()

    while (True):
        print "准备分析数据，当前线程数：{}，等待爬虫线程抓取完毕".format(threading.activeCount())
        if threading.activeCount() != 1:
            time.sleep(60 * 5)
            continue
        else:
            for city in citys:
                articles = Article.mget_by_city_id_and_time(
                    city.id, latest_time_at_map.get(city.id))
                AThread(articles, key_mood_map).run()
            break
