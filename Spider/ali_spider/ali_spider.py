#! /usr/bin/env python

"""
"""

import os.path
import json
import csv
from spider import Spider
from models import Database

webdriver_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'webdriver'))
os.environ["PATH"] += os.pathsep + webdriver_path

def generate_csv(db):
    with open("alibaba.csv", "w", encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow([
            "关键词",
            "负责人",
            "产品编号",
            "产品排名",
            "第一位排名",
            "第一产品",
            "贸易表现",
            "橱窗",
            "P4P",
            "供应商竞争度",
            "橱窗数",
            "热搜度"
        ])
        products = db.get_products()
        for product in products:
            product_keywords = map(str.strip, product.keywords.split(','))
            for product_keyword in product_keywords:
                record = list()
                t_keyword = product_keyword
                t_owner = product.owner
                t_style_no = product.style_no
                t_product_ranking = '-'
                t_top1_ranking = '-'
                t_top1_style_no = '-'
                t_is_trade_product = product.is_trade_product
                t_is_window_product = product.is_window_product
                t_is_p4p_keyword = '-'
                t_company_cnt = '-'
                t_showwin_cnt = '-'
                t_srh_pv = '-'

                keyword_ranking = db.get_keyword_ranking(keyword=t_keyword)
                t_product_ranking = get_product_ranking(
                    ranking=keyword_ranking, product_id=product.id)
                t_top1_style_no, t_top1_ranking = get_top_ranking(
                    ranking=keyword_ranking, products=products)
                keyword_info = db.get_keyword(value=product_keyword)
                if keyword_info is not None:
                    srh_pv = json.loads(keyword_info.srh_pv)
                    t_is_p4p_keyword = keyword_info.is_p4p_keyword
                    t_company_cnt = keyword_info.company_cnt
                    t_showwin_cnt = None
                    t_srh_pv = srh_pv['srh_pv_this_mon']

                record.append(t_keyword)
                record.append(t_owner)
                record.append(t_style_no)
                record.append(t_product_ranking)
                record.append(t_top1_ranking)
                record.append(t_top1_style_no)
                record.append(t_is_trade_product)
                record.append(t_is_window_product)
                record.append(t_is_p4p_keyword)
                record.append(t_company_cnt)
                record.append(t_showwin_cnt)
                record.append(t_srh_pv)

                writer.writerow(record)

def get_product_ranking(ranking, product_id):
    result = '-'
    if ranking is not None and len(ranking) != 0:
        for item in ranking:
            if item['product_id'] == str(product_id):
                result = item['ranking']
    return result

def get_top_ranking(ranking, products):
    style_no = '-'
    position = '-'
    if ranking is None or len(ranking) == 0:
        return style_no, position
    top_ranking = sorted(ranking, key=lambda x: x['ranking'])[0]
    position = top_ranking['ranking']
    for p in products:
        if str(p.id) == top_ranking['product_id']:
            style_no = p.style_no
    return style_no, position

if __name__ == "__main__":
    database = Database()
    # spider = Spider(db)
    # spider.craw_products()
    # spider.craw_keywords()
    # spider.craw_rank()
    generate_csv(db=database)
    database.close()
