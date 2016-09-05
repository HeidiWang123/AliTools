import csv
import json
from datetime import date

class CSV_Generator():

    def __init__(self, database):
        self.database = database

    def generate_alibaba_csv(self, extend_keywords_only=False, products_only=False):
        csv_header = ["关键词", "负责人", "产品编号", "产品排名", "第一位排名", "第一产品",
                      "最后更新日期", "贸易表现", "橱窗", "P4P", "供应商竞争度", "橱窗数", "热搜度"]

        csv_file = "./csv/alibaba" + date.today().strftime("%Y%m%d") + ".csv"
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            products = self.database.get_products()
            extend_keywords = self.database.get_all_keywords(extend_keywords_only=True)
            if extend_keywords_only:
                self._write_keywords(writer=writer, keywords=extend_keywords)
                return
            if products_only:
                self._write_products(writer=writer, products=products)
                return
            self._write_products(writer=writer, products=products)
            self._write_keywords(writer=writer, keywords=extend_keywords)

    def generate_unused_keywords_csv(self):
        csv_header = ["关键词", "第一位排名", "第一产品", "供应商竞争度", "橱窗数", "热搜度"]
        csv_file = "./csv/unused-keywords-" + date.today().strftime("%Y%m%d") + ".csv"
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
        csv_file = "./csv/month-new-keywords-" + date.today().strftime("%Y%m%d") + ".csv"
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
        csv_file = "./csv/p4p-" + date.today().strftime("%Y%m%d") + ".csv"
        p4ps = self.database.get_p4ps()
        with open(csv_file, "w", encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            for item in p4ps:
                writer.writerow([item.keyword, item.qs_star, item.tag, item.is_start])

    def generate_month_keywords_csv(self):
        csv_header = ["关键词", "供应商竞争度", "橱窗数", "热搜度", "类目"]
        csv_file = "./csv/month-keywords-" + date.today().strftime("%Y%m") + ".csv"
        keywords = self.database.get_keywords()
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
