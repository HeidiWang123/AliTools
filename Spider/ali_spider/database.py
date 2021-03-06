# -*- coding: utf-8 -*-

"""database

此模块主要包含与数据库操作相关的类 Database
"""

import os
import re
from operator import is_not
from functools import partial
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pytz import timezone
from tzlocal import get_localzone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
from models import BASE, Product, Keyword, Rank, P4P
import settings

class Database():
    """Database 包含所有的数据库操作方法。

    初始化时会创建一个 session 并再次对象生命周期中公用。
    注意不需要使用此对象时请调用 close 方法关闭数据库连接
    """

    def __init__(self):
        url = make_url(settings.DATABASE_URL)
        os.makedirs(os.path.dirname(url.database), exist_ok=True)
        engine = create_engine(settings.DATABASE_URL, echo=settings.DATABASE_ECHO)
        BASE.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()
        self.products = None

    def get_all_products(self, cache=True):
        if self.products is None or not cache:
            self.products = self.session.query(Product).all()
        return self.products

    def get_keyword_products(self, keyword):
        keyword = re.sub(" +", " ", keyword.lower())
        products = self.get_all_products()
        keyword_products = list()
        for item in products:
            keywords = [re.sub(" +", " ", x.lower()) for x in item.keywords]
            keyword_products.extend([item for k in keywords if k==keyword])
        return keyword_products

    def get_product_by_id(self, product_id):
        products = self.get_all_products()
        for product in products:
            if product.id == product_id:
                return product
        return None

    def get_rank_info(self, keyword, product_id):
        ranking, top1_product_id, top1_ranking = (None, None, None)

        rank_info = self.get_keyword_rank_info(keyword)
        if rank_info is None:
            return ranking, top1_product_id, top1_ranking

        for item in rank_info:
            if item['product_id'] == product_id:
                ranking = item['ranking']
                break
        top1_rank_info = sorted(rank_info, key=lambda x: x['ranking'])[0]
        top1_product_id = top1_rank_info['product_id']
        top1_ranking = top1_rank_info['ranking']
        return ranking, top1_product_id, top1_ranking

    def get_keyword(self, value):
        value = re.sub(" +", " ", value.lower())
        keyword = self.session.query(Keyword).filter_by(value=value).first()
        return keyword

    def get_all_keywords(self):
        return self.session.query(Keyword).all()

    def get_p4ps(self):
        """query P4P records from database and return P4P object list.

        Returns:
            list: a list contain all P4P objects from database.
        """
        return self.session.query(P4P).all()

    def get_product_keywords(self):
        """Get all products used keywords.

        Returns:
            list: a sorted keywords list and without duplicate keywords.
        """
        keywords = list()
        query_result = self.session.query(Product.keywords).all()
        products_keywords = [q for q, in query_result]
        for item in products_keywords:
            keyword_list = [x for x in item]
            keywords.extend(keyword_list)
        return list(set(keywords))

    def get_product_modify_time(self, product_id=None, style_no=None):
        return self.session.query(
            Product.modify_time).filter_by(id=product_id, style_no=style_no
        ).scalar()

    def delete_all_products(self):
        """Delete all products in datebase."""

        self.session.query(Product).delete()
        self.session.commit()

    def delete_all_p4p(self):
        self.session.query(P4P).delete()
        self.session.commit()

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
            record.keyword = re.sub(" +", " ", rank.keyword.lower())
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
            self.upsert_keyword(keyword=keyword, commit=False)
        self.session.commit()
    
    def upsert_keyword(self, keyword, commit=True):
        if keyword is None:
            return
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
        if commit:
            self.session.commit()
    
    def insert_none_keyword(self, keyword):
        if self.is_keyword_need_upsert(keyword):
            self.upsert_keyword(
                Keyword(
                    value=keyword,
                    update=datetime.strptime(datetime.today().strftime('%Y%m0309'), '%Y%m%d%H') + relativedelta(months=+1)
                ))
    
    def update_keyword_category(self, keyword, category):
        keyword.category = category
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

    def is_products_need_update(self):
        """measure if a product record need update

        If the product update date is befor today, then it should update.

        Returns:
            bool: if need update, return True, else return False
        """
        products_count = self.session.query(Product).count()
        need_update_count = self.session.query(Product).filter(Product.update < date.today()).count()
        return products_count == 0 or need_update_count > 0

    def is_keyword_need_upsert(self, keyword):
        """measure if a keyword is need update or insert.

        Args:
            keyword (str): the keyword should to be measured

        Returns:
            bool: if a keyword is exist and need update return True, else return False.
        """
        keyword = re.sub(" +", " ", keyword.lower())
        month_ago = datetime.now(get_localzone()).astimezone(timezone('US/Pacific')) + relativedelta(months=-1)
        record = self.session.query(Keyword).filter(Keyword.value == keyword).first()
        return record is None or record.update < month_ago.replace(tzinfo=None)

    def is_keyword_category_need_update(self, keyword):
        """measure a keyword's category information is need update or not.

        If a keyword category is None, return True, else False

        Args:
            keyword (str): the keyword string.

        Returns:
            bool: True if need update, else False.
        """
        return keyword.category is None or len(keyword.category) == 0

    def is_rank_need_upsert(self, keyword):
        """measure if a keyword is need update or insert.

        Args:
            keyword (str): The keyword should be measured.

        Returns:
            bool: if the rank information of the keyword is exist and need update, return True,
                else return False.
        """
        keyword = re.sub(" +", " ", keyword.lower())
        record = self.session.query(Rank).filter_by(keyword=keyword).first()
        if record is None or record.update is None:
            return True
        return date.today() > record.update

    def get_keyword_rank_info(self, keyword):
        keyword = re.sub(" +", " ", keyword.lower())
        rank = self.session.query(Rank).filter_by(keyword=keyword).first()
        if rank is None or rank.ranking is None:
            return None
        return rank.ranking

    def get_valid_keywords(self):
        valid_keywords = self.get_product_keywords()
        
        category_regex = re.compile(settings.REG_CATEGORIES)
        all_keywords = self.get_all_keywords()
        for keyword in all_keywords:
            if self.is_negative_keyword(keyword.value) or \
               not category_regex.search(str(keyword.category)):
               continue
            valid_keywords.append(keyword.value)

        return sorted(set(valid_keywords))

    def get_base_file_keywords(self):
        base_keywords = None
        with open(settings.BASE_KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            base_keywords = f.read().splitlines()
            base_keywords = filter(partial(is_not, ""), base_keywords)
            base_keywords = filter(partial(is_not, None), base_keywords)
            return base_keywords

    def close(self):
        self.session.commit()
        self.session.close()

    def get_negative_keyword_rules(self):
        negative_keywords = list()
        with open(settings.NEGATIVE_KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                line = line.strip()
                line = line[1:-1] if line.startswith('/') and line.endswith('/') else '^%s$'%line
                regex = re.compile(line)
                negative_keywords.append(regex)
        return negative_keywords

    def is_negative_keyword(self, keyword):
        if keyword is None or keyword == '':
            return True
        for rule in self.get_negative_keyword_rules():
            if rule.search(keyword.strip()):
                return True
        return False
