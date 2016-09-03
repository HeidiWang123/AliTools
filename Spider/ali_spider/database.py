# -*- coding: utf-8 -*-
"""database

此模块主要包含与数据库操作相关的类 Database
"""

import json
import re
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pytz import timezone
from tzlocal import get_localzone
from sqlalchemy import create_engine, exists
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_
from models import BASE, Product, Keyword, Rank, P4P
import settings

class Database():
    """Database 包含所有的数据库操作方法。

    初始化时会创建一个 session 并再次对象生命周期中公用。
    注意不需要使用此对象时请调用 close 方法关闭数据库连接
    """


    def __init__(self):
        engine = create_engine(settings.DATABASE_URL, echo=settings.DATABASE_ECHO)
        BASE.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()

    def add_products(self, products):
        """insert Product object list to database

        Args:
            products (list): The list of Product objects will be added.
        """
        if products is None or len(products) == 0:
            return
        self.session.add_all(products)
        self.session.commit()

    def upsert_rank(self, rank):
        """update or insert a Rank object to database

        Args:
            rank (Rank): a rank object will be update or insert.
        """
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
        """update or insert Keyword object list to database

        Args:
            keywords (list): Keyword list will be added.
        """
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
        """insert P4P object list to database

        Args:
            p4ps (list): a list of P4P objects will be inserted.
        """
        if p4ps is None or len(p4ps) == 0:
            return
        self.session.add_all(p4ps)
        self.session.commit()

    def get_p4ps(self):
        """query P4P records from database and return P4P object list.

        Returns:
            list: a list contain all P4P objects from database.
        """
        return self.session.query(P4P).all()

    def rank_exsit_unneed_update(self, keyword):
        """measure if a keyword record should be exsit and need update.

        Args:
            keyword (str): The keyword should be measured.

        Returns:
            bool: True for keyword rank info is exist and unneed update,
                False for others.
        """
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
        return self.session.query(exists().where(
            and_(Keyword.update<month_ago, Keyword.value==keyword))).scalar()

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
        products_keywords = self.get_product_keywords()
        if products_only:
            return sorted(set(products_keywords))

        extend_keywords = self.get_extend_file_keywords()
        if extend_keywords_only:
            return sorted(set(extend_keywords))

        base_keywords = self.get_base_file_keywords()
        negative_keywords = self.get_negative_file_keywords()
        keywords = []
        keywords.extend(base_keywords)
        keywords.extend(products_keywords)
        if negative_keywords is not None and len(negative_keywords) > 0:
            keywords = [x for x in keywords if x not in negative_keywords]
        return sorted(set(keywords))

    def get_base_file_keywords(self):
        extend_keywords = None
        with open(settings.BASE_KEYWORDS_FILE, 'r') as f:
            extend_keywords = f.read().splitlines()
        return extend_keywords

    def get_extend_file_keywords(self):
        extend_keywords = None
        with open(settings.EXTEND_KEYWORDS_FILE, 'r') as f:
            extend_keywords = f.read().splitlines()
        return extend_keywords

    def get_negative_file_keywords(self):
        negative_keywords = None
        with open(settings.NEGATIVE_KEYWORDS_FILE, 'r') as f:
            negative_keywords = f.read().splitlines()
        return negative_keywords
