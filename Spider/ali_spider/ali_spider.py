#! /usr/bin/env python

"""
"""

import os.path
import json
import csv
import datetime
from spider import Spider
from models import Database
from parser import OverRequestCountError
from pytz import timezone
from tzlocal import get_localzone

webdriver_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'webdriver'))
os.environ["PATH"] += os.pathsep + webdriver_path

class SpiderMain():

    def __init__(self, database, init_spider=True):
        self.database = database
        self.spider = Spider(self.database) if init_spider else None

    def craw(self, craw_products=False, craw_keywords=False, craw_rank=False, craw_p4p=False,
             extend_keywords_only=False, products_only=False, generate_csv=False):
        try:
            if craw_products and not extend_keywords_only:
                self.spider.craw_products()
            if craw_keywords:
                self.spider.craw_keywords(extend_keywords_only=extend_keywords_only,
                                          products_only=products_only)
            if craw_rank:
                self.spider.craw_rank(extend_keywords_only=extend_keywords_only,
                                      products_only=products_only)
            if craw_p4p:
                self.spider.craw_p4p()
                self.generate_p4p_csv()

            if generate_csv:
                self.generate_csv(extend_keywords_only=extend_keywords_only,
                                  products_only=products_only)
        except OverRequestCountError:
            tzpdt = timezone('US/Pacific')
            next_datetime = (datetime.datetime.now(tzpdt) + datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            local_datetime = next_datetime.astimezone(get_localzone())
            print('查询太频繁，请%s再试' % local_datetime.strftime(' %d 号 %H:%M:%S '))

    def generate_csv(self, extend_keywords_only=False, products_only=False):
        csv_header = ["关键词", "负责人", "产品编号", "产品排名", "第一位排名", "第一产品",
                      "最后更新日期", "贸易表现", "橱窗", "P4P", "供应商竞争度", "橱窗数", "热搜度"]

        csv_file = "./csv/alibaba" + datetime.datetime.now().strftime("%Y%m%d") + ".csv"
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            products = self.database.get_products()
            extend_keywords = self.spider.get_keywords(extend_keywords_only=True)
            if extend_keywords_only:
                self._write_keywords(writer=writer, keywords=extend_keywords)
                return
            if products_only:
                self._write_products(writer=writer, products=products)
                return
            self._write_products(writer=writer, products=products)
            self._write_keywords(writer=writer, keywords=extend_keywords)

    def _write_keywords(self, writer, keywords):
        for keyword in keywords:
            t_top1_style_no, t_top1_ranking = self._get_top1_rank_info(keyword=keyword)
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
                     t_top1_modify_time=self.database.get_product_modify_time(style_no=t_top1_style_no),
                     t_is_p4p_keyword=t_is_p4p_keyword,
                     t_company_cnt=t_company_cnt,
                     t_showwin_cnt=t_showwin_cnt,
                     t_srh_pv=t_srh_pv,
                    )

    def _write_products(self, writer, products):
        for product in products:
            product_keywords = map(str.strip, product.keywords.split(','))
            for product_keyword in product_keywords:
                t_product_ranking, t_top1_style_no, t_top1_ranking = self._get_rank_info(
                    keyword=product_keyword, product_id=product.id)

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
                               t_top1_style_no=t_top1_style_no,
                               t_top1_ranking=t_top1_ranking,
                               t_top1_modify_time=self._get_product_modify_time(
                                                        style_no=t_top1_style_no,
                                                        products=products),
                               t_is_trade_product=product.is_trade_product,
                               t_is_window_product=product.is_window_product,
                               t_is_p4p_keyword=t_is_p4p_keyword,
                               t_company_cnt=t_company_cnt,
                               t_showwin_cnt=t_showwin_cnt,
                               t_srh_pv=t_srh_pv
                              )

    def _get_product_modify_time(self, style_no, products):
        for p in products:
            if p.style_no == style_no:
                return p.modify_time
        return None

    def _writerow(self, writer, **info):
        t_keyword = info.get('t_keyword', None)
        t_owner = info.get('t_owner', None)
        t_style_no = info.get('t_style_no', None)

        t_product_ranking = info.get('t_product_ranking', None)
        if t_style_no is None:
            t_product_ranking = None
        elif t_product_ranking is None:
            t_product_ranking = '-'

        t_top1_style_no = info.get('t_top1_style_no', None)
        t_top1_ranking = info.get('t_top1_ranking', None)
        t_top1_modify_time = info.get('t_top1_modify_time', None)
        t_top1_style_no = '-' if t_top1_style_no is None else t_top1_style_no
        t_top1_ranking = '-' if t_top1_ranking is None else t_top1_ranking
        t_top1_modify_time = '-' if t_top1_modify_time is None else t_top1_modify_time

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
                         t_top1_style_no, t_top1_modify_time, t_is_trade_product,
                         t_is_window_product, t_is_p4p_keyword, t_company_cnt,
                         t_showwin_cnt, t_srh_pv])

    def _get_rank_info(self, keyword, product_id):
        ranking, top1_style_no, top1_ranking = (None, None, None)

        rank_info = self.database.get_keyword_rank_info(keyword)
        if rank_info is None:
            return ranking, top1_style_no, top1_ranking

        for item in rank_info:
            if item['product_id'] == product_id:
                ranking = item['ranking']
                break

        top1_style_no, top1_ranking = self._get_top1_rank_info(rank_info=rank_info)
        return ranking, top1_style_no, top1_ranking

    def _get_top1_rank_info(self, keyword=None, rank_info=None):
        top1_style_no, top1_ranking = (None, None)
        if rank_info is None and keyword is not None:
            rank_info = self.database.get_keyword_rank_info(keyword)
        if rank_info is None:
            return top1_style_no, top1_ranking
        top1_rank_info = sorted(rank_info, key=lambda x: x['ranking'])[0]
        top1_style_no = self.database.get_style_no_by_id(top1_rank_info['product_id'])
        top1_ranking = top1_rank_info['ranking']
        return top1_style_no, top1_ranking

    def generate_unused_keywords_csv(self):
        csv_header = ["关键词", "第一位排名", "第一产品", "供应商竞争度", "橱窗数", "热搜度"]
        csv_file = "./csv/未使用关键词-" + datetime.datetime.now().strftime("%Y%m%d") + ".csv"
        keywords = self.database.get_keywords()
        used_keywords = self.spider.get_keywords()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in keywords:
                keyword = item.value
                if keyword not in used_keywords:

                    t_keyword = keyword
                    t_top1_style_no, t_top1_ranking = self._get_top1_rank_info(keyword=keyword)
                    t_company_cnt = item.company_cnt
                    t_showwin_cnt = item.showwin_cnt
                    t_srh_pv = json.loads(item.srh_pv)['srh_pv_this_mon']

                    none_value = {
                        't_top1_ranking': '-',
                        't_top1_style_no': '-',
                        't_company_cnt': '-',
                        't_showwin_cnt': '-',
                        't_srh_pv': '-',
                    }
                    t_top1_ranking = none_value['t_top1_ranking'] if t_top1_ranking is None else t_top1_ranking
                    t_top1_style_no = none_value['t_top1_style_no'] if t_top1_style_no is None else t_top1_style_no
                    t_company_cnt = none_value['t_company_cnt'] if t_company_cnt is None else t_company_cnt
                    t_showwin_cnt = none_value['t_showwin_cnt'] if t_showwin_cnt is None else t_showwin_cnt
                    t_srh_pv = none_value['t_srh_pv'] if t_srh_pv is None else t_srh_pv

                    writer.writerow([t_keyword, t_top1_ranking, t_top1_style_no, t_company_cnt,
                                     t_showwin_cnt, t_srh_pv])

    def generate_month_new_keywords_csv(self):
        csv_header = ["关键词", "第一位排名", "第一产品", "供应商竞争度", "橱窗数", "热搜度"]
        csv_file = "./csv/月新增关键词-" + datetime.datetime.now().strftime("%Y%m%d") + ".csv"
        keywords = self.database.get_keywords()
        used_keywords = self.spider.get_keywords()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in keywords:
                keyword = item.value
                if keyword not in used_keywords:
                    t_srh_pv = json.loads(item.srh_pv)
                    srh_pv_this_mon = t_srh_pv['srh_pv_this_mon']
                    if int(t_srh_pv['srh_pv_last_1mon']) > 0:
                        continue
                    t_keyword = keyword
                    t_top1_style_no, t_top1_ranking = self._get_top1_rank_info(keyword=keyword)
                    t_company_cnt = item.company_cnt
                    t_showwin_cnt = item.showwin_cnt
                    none_value = {
                        't_top1_ranking': '-',
                        't_top1_style_no': '-',
                        't_company_cnt': '-',
                        't_showwin_cnt': '-',
                        'srh_pv_this_mon': '-',
                    }
                    t_top1_ranking = none_value['t_top1_ranking'] if t_top1_ranking is None else t_top1_ranking
                    t_top1_style_no = none_value['t_top1_style_no'] if t_top1_style_no is None else t_top1_style_no
                    t_company_cnt = none_value['t_company_cnt'] if t_company_cnt is None else t_company_cnt
                    t_showwin_cnt = none_value['t_showwin_cnt'] if t_showwin_cnt is None else t_showwin_cnt
                    srh_pv_this_mon = none_value['t_srh_pv'] if srh_pv_this_mon is None else srh_pv_this_mon

                    writer.writerow([t_keyword, t_top1_ranking, t_top1_style_no, t_company_cnt,
                                     t_showwin_cnt, srh_pv_this_mon])

    def generate_p4p_csv(self):
        csv_header = ["关键词", "推广评分", "关键词组", "状态"]
        csv_file = "./csv/p4p-" + datetime.datetime.now().strftime("%Y%m%d") + ".csv"
        p4ps = self.database.get_p4ps()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in p4ps:
                writer.writerow([item.keyword, item.qs_star, item.tag, item.is_start])

    def generate_month_keywords_csv(self):
        csv_header = ["关键词", "供应商竞争度", "橱窗数", "热搜度"]
        csv_file = "./csv/关键词-" + datetime.datetime.now().strftime("%Y%m") + ".csv"
        keywords = self.database.get_keywords()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in keywords:
                t_keyword = item.value
                t_company_cnt = item.company_cnt
                t_showwin_cnt = item.showwin_cnt
                srh_pv_this_mon = json.loads(item.srh_pv)['srh_pv_this_mon']
                writer.writerow([t_keyword, t_company_cnt, t_showwin_cnt, srh_pv_this_mon])

if __name__ == "__main__":
    db = Database()
    spider_main = SpiderMain(db)
    # spider_main.craw(craw_products=True, craw_keywords=True, craw_rank=True, craw_p4p=True)
    spider_main.spider.craw_keywords()
    spider_main.generate_month_keywords_csv()
    # spider_main.spider.craw_rank()
    # spider_main.generate_csv()
    # spider_main.generate_unused_keywords_csv()
    # spider_main.generate_month_new_keywords_csv()
    db.close()
