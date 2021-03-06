# -*- coding: utf-8 -*-

"""data generator module
"""

import re
import csv
import os
from datetime import date
from dateutil.relativedelta import relativedelta
import settings

class CSV_Generator():

    def __init__(self, database):
        self.database = database

    def generate_overview_csv(self, keywords=None):
        if keywords is None:
            keywords = self.database.get_valid_keywords()
            
        csv_header = ["关键词", "负责人", "产品编号", "产品排名", "最后更新日期", "第一位排名", "第一产品",
                      "最后更新日期", "贸易表现", "橱窗", "P4P", "供应商竞争度", "橱窗数",
                      "热搜度", "数据更新时间"]

        csv_file = "./csv/overview-" + date.today().strftime("%Y%m%d") + ".csv"
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        with open(csv_file, "w", encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            t_generate_date = date.today()
            for keyword in keywords:
                t_keyword = keyword.strip()

                t_is_p4p_keyword = None
                t_company_cnt = t_showwin_cnt = t_srh_pv = "-"
                keyword_info = self.database.get_keyword(t_keyword)
                if keyword_info is not None:
                    if keyword_info.is_p4p_keyword is not None:
                        t_is_p4p_keyword = keyword_info.is_p4p_keyword
                    if keyword_info.company_cnt is not None:
                        t_company_cnt = keyword_info.company_cnt
                    if keyword_info.showwin_cnt is not None:
                        t_showwin_cnt = keyword_info.showwin_cnt
                    if keyword_info.srh_pv is not None:
                        t_srh_pv = keyword_info.srh_pv['srh_pv_this_mon']
                
                rank_info = self.database.get_keyword_rank_info(t_keyword)
                
                t_top1_ranking = t_top1_style_no = t_top1_modify_time = "-"
                if rank_info is not None:
                    top1_rank = sorted(rank_info, key=lambda x: x['ranking'])[0]
                    top1_product = self.database.get_product_by_id(top1_rank['product_id'])
                    
                    t_top1_ranking = top1_rank['ranking']
                    t_top1_style_no = top1_product.style_no
                    t_top1_modify_time = top1_product.modify_time

                products = self.database.get_keyword_products(t_keyword)
                if len(products) == 0:
                    t_owner = t_style_no = t_product_ranking = t_product_modify_time = t_is_trade_product = t_is_window_product = None
                    writer.writerow([
                        t_keyword, t_owner, t_style_no, t_product_ranking, t_product_modify_time, t_top1_ranking,
                        t_top1_style_no, t_top1_modify_time, t_is_trade_product,
                        t_is_window_product, t_is_p4p_keyword, t_company_cnt,
                        t_showwin_cnt, t_srh_pv, t_generate_date
                    ])
                    continue
                
                for product in products:
                    t_owner = t_style_no = t_product_ranking = t_product_modify_time = t_is_trade_product = t_is_window_product = None
                    t_owner = product.owner
                    t_style_no = product.style_no
                    t_product_modify_time = product.modify_time
                    t_is_trade_product = product.is_trade_product
                    t_is_window_product = product.is_window_product
                    if rank_info is not None:
                        rank_dict = { x["product_id"]: x["ranking"] for x in rank_info }
                        if product.id in rank_dict.keys():
                            t_product_ranking = rank_dict.get(product.id)
                    writer.writerow([
                        t_keyword, t_owner, t_style_no, t_product_ranking, t_product_modify_time, t_top1_ranking,
                        t_top1_style_no, t_top1_modify_time, t_is_trade_product,
                        t_is_window_product, t_is_p4p_keyword, t_company_cnt,
                        t_showwin_cnt, t_srh_pv, t_generate_date
                    ])
                    
    def generate_p4p_csv(self):
        csv_header = ["关键词", "推广评分", "关键词组", "状态"]
        csv_file = "./csv/p4p-" + date.today().strftime("%Y%m%d") + ".csv"
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        p4ps = self.database.get_p4ps()
        with open(csv_file, "w", encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in p4ps:
                writer.writerow([item.keyword, item.qs_star, item.tag, item.is_start])

    def generate_keywords_csv(self):
        csv_header = ["关键词",  "供应商竞争度", "橱窗数",
                      date.today().strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-1)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-2)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-3)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-4)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-5)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-6)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-7)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-8)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-9)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-10)).strftime("%Y/%m{}").format('热搜度'),
                      (date.today() + relativedelta(months=-11)).strftime("%Y/%m{}").format('热搜度'),
                      "类目"]
        csv_file = "./csv/keywords-" + date.today().strftime("%Y%m") + ".csv"
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        keywords = self.database.get_all_keywords()
        with open(csv_file, "w", encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            regex = re.compile(settings.REG_CATEGORIES)
            for item in keywords:
                if self.database.is_negative_keyword(item.value):
                    continue
                if not regex.search(str(item.category)):
                    continue
                t_keyword = item.value
                t_category = item.category
                t_company_cnt = item.company_cnt
                t_showwin_cnt = item.showwin_cnt
                t_srh_pv_this_mon = item.srh_pv['srh_pv_this_mon']
                t_srh_pv_last_1mon = item.srh_pv['srh_pv_last_1mon']
                t_srh_pv_last_2mon = item.srh_pv['srh_pv_last_2mon']
                t_srh_pv_last_3mon = item.srh_pv['srh_pv_last_3mon']
                t_srh_pv_last_4mon = item.srh_pv['srh_pv_last_4mon']
                t_srh_pv_last_5mon = item.srh_pv['srh_pv_last_5mon']
                t_srh_pv_last_6mon = item.srh_pv['srh_pv_last_6mon']
                t_srh_pv_last_7mon = item.srh_pv['srh_pv_last_7mon']
                t_srh_pv_last_8mon = item.srh_pv['srh_pv_last_8mon']
                t_srh_pv_last_9mon = item.srh_pv['srh_pv_last_9mon']
                t_srh_pv_last_10mon = item.srh_pv['srh_pv_last_10mon']
                t_srh_pv_last_11mon = item.srh_pv['srh_pv_last_11mon']
                writer.writerow([
                    t_keyword, t_company_cnt, t_showwin_cnt, t_srh_pv_this_mon,
                    t_srh_pv_last_1mon, t_srh_pv_last_2mon, t_srh_pv_last_3mon, t_srh_pv_last_4mon,
                    t_srh_pv_last_5mon, t_srh_pv_last_6mon, t_srh_pv_last_7mon, t_srh_pv_last_8mon,
                    t_srh_pv_last_9mon, t_srh_pv_last_10mon, t_srh_pv_last_11mon, t_category
                ])
