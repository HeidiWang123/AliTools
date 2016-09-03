# -*- coding: utf-8 -*-

from datetime import date
from sqlalchemy import Integer, String, Date, DateTime, Boolean, Column
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

class Product(BASE):
    """docstring for Product."""

    __tablename__ = "products"

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    style_no = Column('style_no', String)
    title = Column('title', String)
    keywords = Column('keywords', String)
    owner = Column('owner', String)
    modify_time = Column('modify_time', Date)
    update = Column('update', Date, default=date.today)
    is_trade_product = Column('is_trade_product', Boolean)
    is_window_product = Column('is_window_product', Boolean)

class Keyword(BASE):
    """docstring for Keyword."""

    __tablename__ = "keywords"

    value = Column('value', String, primary_key=True)
    repeat_keyword = Column('repeat_keyword', String)
    company_cnt = Column('company_cnt', Integer)
    showwin_cnt = Column('showwin_cnt', Integer)
    srh_pv = Column('srh_pv', String)
    update = Column('update', DateTime)
    is_p4p_keyword = Column('is_p4p_keyword', Boolean)

class Rank(BASE):
    """docstring for Rank."""

    __tablename__ = "rank"

    keyword = Column('keyword', String, primary_key=True)
    ranking = Column('ranking', String)
    update = Column('update', Date, default=date.today)

class P4P(BASE):
    __tablename__ = "p4p"

    keyword = Column('keyword', String, primary_key=True)
    qs_star = Column('qs_star', Integer)
    is_start = Column('is_start', Boolean)
    tag = Column('tag', String)
