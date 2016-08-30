#! /usr/bin/env python

"""
"""

import os.path
import json
import csv
from datetime import datetime
from spider import Spider
from models import Database

webdriver_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'webdriver'))
os.environ["PATH"] += os.pathsep + webdriver_path

class SpiderMain():

    def __init__(self, database):
        self.database = database
        self.spider = Spider(self.database)

    def craw(self, craw_products=True, craw_keywords=True, craw_rank=True,
             extend_keywords_only=False, products_only=False):
        if craw_products and not extend_keywords_only:
            self.spider.craw_products()
        if craw_keywords:
            self.spider.craw_keywords(extend_keywords_only=extend_keywords_only,
                                      products_only=products_only)
        if craw_rank:
            self.spider.craw_rank(extend_keywords_only=extend_keywords_only,
                                  products_only=products_only)
        self._generate_csv(extend_keywords_only=extend_keywords_only,
                           products_only=products_only)

    def _generate_csv(self, extend_keywords_only=False, products_only=False):
        csv_header = ["关键词", "负责人", "产品编号", "产品排名", "第一位排名", "第一产品", "贸易表现",
                      "橱窗", "P4P", "供应商竞争度", "橱窗数", "热搜度"]

        csv_file = "./csv/alibab" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            products = self.database.get_products()
            keywords = self.spider.get_keywords(extend_keywords_only)
            if extend_keywords_only:
                self._write_keywords(writer=writer, keywords=keywords, products=products)
                return
            if products_only:
                self._write_products(writer=writer, products=products)
                return
            self._write_keywords(writer=writer, keywords=keywords, products=products)
            self._write_products(writer=writer, products=products)

    def _write_keywords(self, writer, keywords, products):
        for keyword in keywords:
            rank_info = self.database.get_keyword_rank_info(keyword=keyword)
            t_top1_style_no, t_top1_ranking = self._get_top_rank_info(rank_info=rank_info,
                                                                      products=products)
            t_is_p4p_keyword = None
            t_company_cnt = None
            t_showwin_cnt = None
            t_srh_pv = None
            keyword_info = self.database.get_keyword(value=keyword)
            if keyword_info is not None:
                srh_pv = json.loads(keyword_info.srh_pv)
                t_is_p4p_keyword = keyword_info.is_p4p_keyword
                t_company_cnt = keyword_info.company_cnt
                t_showwin_cnt = keyword_info.showwin_cnt
                t_srh_pv = srh_pv['srh_pv_this_mon']

            self._writerow(writer=writer,
                     t_keyword=keyword,
                     t_top1_style_no=t_top1_style_no,
                     t_top1_ranking=t_top1_ranking,
                     t_is_p4p_keyword=t_is_p4p_keyword,
                     t_company_cnt=t_company_cnt,
                     t_showwin_cnt=t_showwin_cnt,
                     t_srh_pv=t_srh_pv
                    )

    def _write_products(self, writer, products):
        for product in products:
            product_keywords = map(str.strip, product.keywords.split(','))
            for product_keyword in product_keywords:
                t_product_ranking, t_top1_style_no, t_top1_ranking = self._get_rank_info(
                    rank_info=self.database.get_keyword_rank_info(keyword=product_keyword),
                    product_id=product.id, products=products)
                keyword_info = self.database.get_keyword(value=product_keyword)
                t_is_p4p_keyword = None
                t_company_cnt = None
                t_showwin_cnt = None
                t_srh_pv = None
                if keyword_info is not None:
                    srh_pv = json.loads(keyword_info.srh_pv)
                    t_is_p4p_keyword = keyword_info.is_p4p_keyword
                    t_company_cnt = keyword_info.company_cnt
                    t_showwin_cnt = keyword_info.showwin_cnt
                    t_srh_pv = srh_pv['srh_pv_this_mon']

                self._writerow(writer=writer,
                         t_keyword=product_keyword,
                         t_owner=product.owner,
                         t_style_no=product.style_no,
                         t_product_ranking=t_product_ranking,
                         t_is_trade_product=product.is_trade_product,
                         t_is_window_product=product.is_window_product,
                         t_top1_style_no=t_top1_style_no,
                         t_top1_ranking=t_top1_ranking,
                         t_is_p4p_keyword=t_is_p4p_keyword,
                         t_company_cnt=t_company_cnt,
                         t_showwin_cnt=t_showwin_cnt,
                         t_srh_pv=t_srh_pv
                        )

    def _writerow(self, writer, **info):
        t_keyword = info.get('t_keyword', None)
        if t_keyword is None:
            print("keyword is None, ignored.")
            return
        if t_keyword != 'a4 size file folder':
            return
        t_owner = info.get('t_owner', None)
        t_style_no = info.get('t_style_no', None)
        t_product_ranking = info.get('t_product_ranking', None if t_style_no is None else '-')
        t_top1_ranking = info.get('t_top1_ranking', '-')
        t_top1_style_no = info.get('t_top1_style_no', '-')
        t_is_trade_product = info.get('t_is_trade_product', None)
        t_is_window_product = info.get('t_is_window_product', None)
        t_is_p4p_keyword = info.get('t_is_p4p_keyword', None)
        t_company_cnt = info.get('t_company_cnt', '-')
        t_showwin_cnt = info.get('t_showwin_cnt', '-')
        t_srh_pv = info.get('t_srh_pv', '-')

        if t_company_cnt is None:
            t_company_cnt = "-"
        if t_showwin_cnt is None:
            t_showwin_cnt = "-"
        if t_srh_pv is None:
            t_srh_pv = "-"

        writer.writerow([t_keyword, t_owner, t_style_no, t_product_ranking, t_top1_ranking,
                         t_top1_style_no, t_is_trade_product, t_is_window_product, t_is_p4p_keyword,
                         t_company_cnt, t_showwin_cnt, t_srh_pv])

    def _get_rank_info(self, rank_info, products, product_id):
        product_ranking = '-'
        if rank_info is not None and len(rank_info) != 0:
            for item in rank_info:
                if item['product_id'] == str(product_id):
                    product_ranking = item['ranking']
        top1_style_no, top1_ranking = self._get_top_rank_info(rank_info=rank_info, products=products)
        return product_ranking, top1_style_no, top1_ranking

    def _get_top_rank_info(self, rank_info, products):
        top1_style_no = '-'
        top1_ranking = '-'
        if rank_info is None:
            return top1_style_no, top1_ranking

        top1_rank_info = sorted(rank_info, key=lambda x: x['ranking'])[0]
        top1_ranking = top1_rank_info['ranking']
        for p in products:
            if str(p.id) == top1_rank_info['product_id']:
                top1_style_no = p.style_no
        return top1_style_no, top1_ranking

    def craw_rank(self):
        self.craw(craw_products=False, craw_keywords=False, craw_rank=True)

    def craw_products_rank(self):
        self.craw(craw_products=False, craw_keywords=False, craw_rank=True,
                  products_only=True)

    def craw_exntend_keywords_rank(self):
        self.craw(craw_products=True, craw_keywords=False, craw_rank=True,
                  extend_keywords_only=True)

    def craw_products(self):
        self.craw(craw_products=True, craw_keywords=False, craw_rank=False)

    def craw_keywords(self):
        self.craw(craw_products=False, craw_keywords=True, craw_rank=False)

    def craw_products_keywords(self):
        self.craw(craw_products=False, craw_keywords=True, craw_rank=False,
                  products_only=True)

    def craw_exntend_keywords(self):
        self.craw(craw_products=False, craw_keywords=True, craw_rank=False,
                  products_only=True)


if __name__ == "__main__":
    db = Database()
    spider_main = SpiderMain(db)
    spider_main.craw_products_rank()
    db.close()
