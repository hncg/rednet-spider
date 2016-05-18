# coding=utf-8
from sqlalchemy import Column, String, Integer, DateTime, Text, func, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from handler import DBSession
from utils import (
    log,
    do_time,
)
Base = declarative_base()


class City(Base):

    __tablename__ = 'city'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    fid = Column(Integer, index=True)

    @classmethod
    def mget(cls):
        return DBSession().query(cls).all()

    @classmethod
    def add(cls, **kwds):
        session = DBSession()
        session.add(cls(**kwds))
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise(SQLAlchemyError)
        finally:
            session.close()


class Article(Base):

    __tablename__ = 'article'

    id = Column(Integer, primary_key=True)
    city_id = Column(Integer, index=True)
    tid = Column(Integer, index=True)
    type = Column(String(32), index=True)
    title = Column(String(128))
    time_at = Column(DateTime)
    content = Column(Text)
    author = Column(String(128))
    reply_number = Column(Integer)
    read_number = Column(Integer)
    url = Column(String(256))

    @classmethod
    def mget_by_city_id_and_time(cls, city_id, time_at):
        time_at = do_time.str2datetime('1994-01-01 22:22:22') if not time_at \
            else time_at
        return DBSession().query(cls). \
            filter(cls.city_id == city_id). \
            filter(cls.time_at > time_at).all()

    @classmethod
    def mget_tids_by_city_id(cls, city_id):
        return DBSession().query(cls.tid). \
            filter(cls.city_id == city_id).all()

    @classmethod
    def mget_latest_time_at(cls):
        return DBSession().query(func.max(cls.time_at), cls.city_id). \
            group_by(cls.city_id).all()

    @classmethod
    def add(cls, **kwds):
        session = DBSession()
        session.add(cls(**kwds))
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise(SQLAlchemyError)
        finally:
            session.close()

    @classmethod
    def add_all(cls, articles):
        if not articles:
            return
        session = DBSession()
        session.add_all(articles)
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            log.log(message=articles[0].url)
            # raise(SQLAlchemyError)
        finally:
            session.close()


class Mood(Base):

    __tablename__ = 'mood'

    id = Column(Integer, primary_key=True)
    time_at = Column(DateTime)
    city_id = Column(Integer, index=True)
    happy = Column(Integer)
    good = Column(Integer)
    anger = Column(Integer)
    sorrow = Column(Integer)
    fear = Column(Integer)
    evil = Column(Integer)
    surprise = Column(Integer)
    cps = Column(Float)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)

    @classmethod
    def mget_by_city_id(cls, city_id):
        return DBSession().query(cls).filter(cls.city_id == city_id).all()

    @classmethod
    def add_all(cls, moods):
        session = DBSession()
        session.add_all(moods)
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise(SQLAlchemyError)
        finally:
            session.close()

    @classmethod
    def update_all(cls, moods):
        session = DBSession()
        for mood in moods:
            session.query(cls). \
                filter(cls.time_at == mood.time_at). \
                filter(cls.city_id == mood.city_id). \
                update({
                    cls.happy: mood.happy,
                    cls.good: mood.good,
                    cls.anger: mood.anger,
                    cls.sorrow: mood.sorrow,
                    cls.fear: mood.fear,
                    cls.evil: mood.evil,
                    cls.surprise: mood.surprise,
                    cls.cps: mood.cps,
                })
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise(SQLAlchemyError)
        finally:
            session.close()


class HotWords(Base):

    __tablename__ = 'hot_words'

    id = Column(Integer, primary_key=True)
    time_at = Column(DateTime)
    word = Column(Integer)
    times = Column(Integer)
    city_id = Column(Integer, index=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)

    @classmethod
    def get_by_city_id_and_time_and_word(cls, city_id, time_at, word):
        return DBSession().query(cls). \
            filter(cls.city_id == city_id). \
            filter(cls.time_at == time_at). \
            filter(cls.word == word). \
            first()

    @classmethod
    def add_all(cls, hot_words):
        if not hot_words:
            return
        session = DBSession()
        session.add_all(hot_words)
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            # raise(SQLAlchemyError)
        finally:
            session.close()

    @classmethod
    def update(cls, city_id, time_at, word, times):
        session = DBSession()
        session.query(cls). \
            filter(cls.time_at == time_at). \
            filter(cls.city_id == city_id). \
            filter(cls.word == word). \
            update({
                cls.times: times,
            })
        try:
            session.flush()
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise(SQLAlchemyError)
        finally:
            session.close()
