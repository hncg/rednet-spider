# coding=utf-8
import sys
sys.path.append('../')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import env
engine = create_engine('mysql://cg:123456@localhost:3306/rednet?charset=utf8&unix_socket=' + env.socket, echo=False, pool_size=2000) # noqa
DBSession = sessionmaker(bind=engine)
