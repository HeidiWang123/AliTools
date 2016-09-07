# -*- coding: utf-8 -*-

import json
import ast
from datetime import date
from sqlalchemy import Integer, String, Date, DateTime, Boolean, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict

class JSONEncodedDict(TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage::
        JSONEncodedDict(255)
    """

    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

class Array(TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage::
        JSONEncodedDict(255)
    """

    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = str(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = ast.literal_eval(value)
        return value

ARRAY_TYPE = Array()
JSON_TYPE = MutableDict.as_mutable(JSONEncodedDict)

BASE = declarative_base()

class Product(BASE):
    """docstring for Product."""

    __tablename__ = "products"

    id = Column('id', Integer, primary_key=True, autoincrement=False)
    style_no = Column('style_no', String)
    title = Column('title', String)
    keywords = Column('keywords', ARRAY_TYPE)
    owner = Column('owner', String)
    modify_time = Column('modify_time', Date)
    update = Column('update', Date, default=date.today)
    is_trade_product = Column('is_trade_product', Boolean)
    is_window_product = Column('is_window_product', Boolean)

class Keyword(BASE):
    """docstring for Keyword."""

    __tablename__ = "keywords"

    value = Column('value', String, primary_key=True, autoincrement=False)
    repeat_keyword = Column('repeat_keyword', String)
    company_cnt = Column('company_cnt', Integer)
    showwin_cnt = Column('showwin_cnt', Integer)
    srh_pv = Column('srh_pv', JSON_TYPE)
    update = Column('update', DateTime)
    is_p4p_keyword = Column('is_p4p_keyword', Boolean)
    category = Column('category', ARRAY_TYPE)

class Rank(BASE):
    """docstring for Rank."""

    __tablename__ = "rank"

    keyword = Column('keyword', String, primary_key=True, autoincrement=False)
    ranking = Column('ranking', ARRAY_TYPE)
    update = Column('update', Date, default=date.today)

class P4P(BASE):
    __tablename__ = "p4p"

    keyword = Column('keyword', String, primary_key=True, autoincrement=False)
    qs_star = Column('qs_star', Integer)
    is_start = Column('is_start', Boolean)
    tag = Column('tag', ARRAY_TYPE)
