import csv
import os
import json
from datetime import date

class CSV_Generator():

    def __init__(self, database):
        self.database = database

    def generate_overview_csv(self, keywords):
        keywords = list(set(keywords))
        csv_header = ["关键词", "负责人", "产品编号", "产品排名", "第一位排名", "第一产品",
                      "最后更新日期", "贸易表现", "橱窗", "P4P", "供应商竞争度", "橱窗数",
                      "热搜度", "数据更新时间"]

        csv_file = "./csv/overview" + date.today().strftime("%Y%m%d") + ".csv"
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            t_generate_date = date.today()
            for keyword in keywords:
                t_keyword = keyword.strip()
                t_owner = t_style_no = t_product_ranking = None
                t_top1_ranking = t_top1_style_no = t_top1_modify_time = '-'
                t_is_trade_product = t_is_window_product = t_is_p4p_keyword= None
                t_company_cnt = t_showwin_cnt = t_srh_pv = "-"

                keyword_info = self.database.get_keyword(t_keyword)
                if keyword_info is not None:
                    t_is_p4p_keyword = keyword_info.is_p4p_keyword
                    t_company_cnt = keyword_info.company_cnt
                    t_showwin_cnt = keyword_info.showwin_cnt
                    t_srh_pv = keyword_info.srh_pv['srh_pv_this_mon']

                products = self.database.get_keyword_products(t_keyword)
                if len(products) == 0:
                    if t_style_no is None and t_top1_ranking != '-' and int(t_top1_ranking) < 2:
                        t_style_no = t_product_ranking = '-'
                    writer.writerow([
                        t_keyword, t_owner, t_style_no, t_product_ranking, t_top1_ranking,
                        t_top1_style_no, t_top1_modify_time, t_is_trade_product,
                        t_is_window_product, t_is_p4p_keyword, t_company_cnt,
                        t_showwin_cnt, t_srh_pv, t_generate_date
                    ])
                    continue

                for product in products:
                    t_owner = product.owner
                    t_style_no = product.style_no
                    t_is_trade_product = product.is_trade_product
                    t_is_window_product = product.is_window_product
                    t_product_ranking, top1_product_id, t_top1_ranking = \
                        self.database.get_rank_info(keyword=t_keyword, product_id=product.id)
                    if t_product_ranking is None:
                        t_product_ranking = '-'
                    if top1_product_id is not None:
                        top1_product = self.database.get_product_by_id(top1_product_id)
                        t_top1_style_no = top1_product.style_no
                        t_top1_modify_time = top1_product.modify_time
                    writer.writerow([
                        t_keyword, t_owner, t_style_no, t_product_ranking, t_top1_ranking,
                        t_top1_style_no, t_top1_modify_time, t_is_trade_product,
                        t_is_window_product, t_is_p4p_keyword, t_company_cnt,
                        t_showwin_cnt, t_srh_pv, t_generate_date
                    ])

    def generate_p4p_csv(self):
        csv_header = ["关键词", "推广评分", "关键词组", "状态"]
        csv_file = "./csv/p4p-" + date.today().strftime("%Y%m%d") + ".csv"
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        p4ps = self.database.get_p4ps()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in p4ps:
                writer.writerow([item.keyword, item.qs_star, item.tag, item.is_start])

    def generate_keywords_csv(self):
        csv_header = ["关键词", "供应商竞争度", "橱窗数", "热搜度", "类目"]
        csv_file = "./csv/month-keywords-" + date.today().strftime("%Y%m") + ".csv"
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        keywords = self.database.get_all_keywords()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in keywords:
                t_keyword = item.value
                t_company_cnt = item.company_cnt
                t_showwin_cnt = item.showwin_cnt
                t_srh_pv_this_mon = item.srh_pv['srh_pv_this_mon']
                t_category = item.category
                writer.writerow([
                    t_keyword, t_company_cnt, t_showwin_cnt, t_srh_pv_this_mon,
                    t_category
                ])
