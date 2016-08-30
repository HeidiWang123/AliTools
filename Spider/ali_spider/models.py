import json
from datetime import datetime, date, timedelta
from sqlalchemy import Integer, String, Date, DateTime, Boolean
from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
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

    keyword = Column('keyword_value', String, primary_key=True)
    ranking = Column('ranking', String)
    update = Column('update', Date)

class Database():
    session = None
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

    def rank_exsit_unneed_update(self, keyword):
        record = self.session.query(Rank).filter_by(keyword=keyword).first()
        if record is not None and record.update == date.today():
            # 如果是同一天的，就不用更新了
            return True
        else:
            return False

    def keyword_exsit_unneed_update(self, keyword):
        record = self.session.query(Keyword).filter_by(value=keyword).first()
        tzpdt = timezone('US/Pacific')
        next_month = record.update.replace(month=record.update.month+1).replace(tzinfo=tzpdt)
        if record is not None and next_month > datetime.now(get_localzone()):
            return True
        else:
            return False

    def get_product_keywords(self):
        keywords = set()
        for keyword in self.session.query(Product.keywords):
            keywords.update(map(str.strip, keyword[0].split(',')))
        return sorted(list(keywords))

    def get_products(self):
        return self.session.query(Product).all()

    def get_keyword_rank_info(self, keyword):
        rank = self.session.query(Rank).filter_by(keyword=keyword.lower()).first()
        if rank is None:
            return None
        return json.loads(rank.ranking)

    def get_keyword(self, value):
        keyword = self.session.query(Keyword).filter_by(value=value).first()
        return keyword

    def close(self):
        self.session.commit()
        self.session.close()
