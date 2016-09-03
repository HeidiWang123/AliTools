import json
import re
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import Integer, String, Date, DateTime, Boolean
from sqlalchemy import Column, create_engine, exists
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import and_
from tzlocal import get_localzone
from pytz import timezone

Base = declarative_base()

class Product(Base):
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

class Keyword(Base):
    """docstring for Keyword."""

    __tablename__ = "keywords"

    value = Column('value', String, primary_key=True)
    repeat_keyword = Column('repeat_keyword', String)
    company_cnt = Column('company_cnt', Integer)
    showwin_cnt = Column('showwin_cnt', Integer)
    srh_pv = Column('srh_pv', String)
    update = Column('update', DateTime)
    is_p4p_keyword = Column('is_p4p_keyword', Boolean)

class Rank(Base):
    """docstring for Rank."""

    __tablename__ = "rank"

    keyword = Column('keyword', String, primary_key=True)
    ranking = Column('ranking', String)
    update = Column('update', Date, default=date.today)

class P4P(Base):
    __tablename__ = "p4p"

    keyword = Column('keyword', String, primary_key=True)
    qs_star = Column('qs_star', Integer)
    is_start = Column('is_start', Boolean)
    tag = Column('tag', String)

class Database():
    session = None

    base_keywords_file = "./config/base_keywords.txt"
    extend_keywords_file = "./config/extend_keywords.txt"
    negative_keywords_file = "./config/negative_keywords.txt"

    def __init__(self):
        engine = create_engine('sqlite:///./database/data.db', echo=False)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def upsert_products(self, products):
        if products is None or len(products) == 0:
            return
        self.session.add_all(products)
        self.session.commit()

    def upsert_rank(self, rank):
        if rank is None:
            return
        record = self.session.query(Rank).filter_by(keyword=rank.keyword).first()
        if record is not None:
            record.keyword = rank.keyword
            record.ranking = rank.ranking
            record.update = rank.update
        else:
            self.session.add(rank)
        self.session.commit()

    def upsert_keywords(self, keywords):
        if keywords is None or len(keywords) == 0:
            return
        for keyword in keywords:
            record = self.session.query(Keyword).filter_by(value=keyword.value).first()
            if record is not None:
                record.value = keyword.value
                record.repeat_keyword = keyword.repeat_keyword
                record.company_cnt = keyword.company_cnt
                record.showwin_cnt = keyword.showwin_cnt
                record.srh_pv = keyword.srh_pv
                record.update = keyword.update
                record.is_p4p_keyword = keyword.is_p4p_keyword
            else:
                self.session.add(keyword)
        self.session.commit()

    def add_p4ps(self, p4ps):
        if p4ps is None or len(p4ps) == 0:
            return
        self.session.add_all(p4ps)
        self.session.commit()

    def get_p4ps(self):
        return self.session.query(P4P).all()

    def rank_exsit_unneed_update(self, keyword):
        record = self.session.query(Rank).filter_by(keyword=keyword).first()
        if record is None:
            return False

        is_day2update = False
        if record.update is not None:
            is_day2update = date.today() > record.update
        return record is not None and not is_day2update

    def is_keyword_need_update(self, keyword):
        keyword = re.sub(" +", " ", keyword.lower())
        current_pst = datetime.now(get_localzone()).astimezone(timezone('US/Pacific'))
        month_ago = current_pst + relativedelta(months=-1)
        return self.session.query(exists().where(and_(Keyword.update<month_ago, Keyword.value==keyword))).scalar()

    def is_products_need_update(self):
        return self.session.query(exists().where(Product.update < date.today())).scalar()

    def get_product_keywords(self):
        keywords = set()
        for keyword in self.session.query(Product.keywords):
            keywords.update(map(str.strip, keyword[0].split(',')))
        return sorted(list(keywords))

    def get_products(self):
        return self.session.query(Product).all()

    def get_product_modify_time(self, product_id=None, style_no=None):
        return self.session.query(Product.modify_time).filter_by(id=product_id,
                                                                 style_no=style_no).scalar()

    def get_keyword_rank_info(self, keyword):
        keyword = re.sub(" +", " ", keyword.lower())
        rank = self.session.query(Rank).filter_by(keyword=keyword).first()
        if rank is None or rank.ranking is None:
            return None
        return json.loads(rank.ranking)

    def get_style_no_by_id(self, product_id):
        return self.session.query(Product.style_no).filter_by(id=product_id).scalar()

    def get_keyword(self, value):
        keyword = self.session.query(Keyword).filter_by(value=value).first()
        return keyword

    def get_keywords(self):
        return self.session.query(Keyword).all()

    def clear_products(self):
        self.session.query(Product).delete()
        self.session.commit()

    def clear_p4p(self):
        self.session.query(P4P).delete()
        self.session.commit()

    def close(self):
        self.session.commit()
        self.session.close()

    def get_all_keywords(self, extend_keywords_only=False, products_only=False):
        products_keywords = self.db.get_product_keywords()
        if products_only:
            return sorted(set(products_keywords))

        extend_keywords = self.db.get_extend_file_keywords()
        if extend_keywords_only:
            return sorted(set(extend_keywords))

        base_keywords = self.db.get_base_file_keywords()
        negative_keywords = self.db.get_negative_file_keywords()
        keywords = []
        keywords.extend(base_keywords)
        keywords.extend(products_keywords)
        if negative_keywords is not None and len(negative_keywords) > 0:
            keywords = [x for x in keywords if x not in negative_keywords]
        return sorted(set(keywords))

    def get_base_file_keywords(self):
        extend_keywords = None
        with open(self.base_keywords_file, 'r') as f:
            extend_keywords = f.read().splitlines()
        return extend_keywords

    def get_extend_file_keywords(self):
        extend_keywords = None
        with open(self.extend_keywords_file, 'r') as f:
            extend_keywords = f.read().splitlines()
        return extend_keywords

    def get_negative_file_keywords(self):
        negative_keywords = None
        with open(self.negative_keywords_file, 'r') as f:
            negative_keywords = f.read().splitlines()
        return negative_keywords
