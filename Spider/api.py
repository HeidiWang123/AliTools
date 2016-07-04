import requests
import time
import random
from datetime import datetime
from http.cookiejar import MozillaCookieJar
from dateutil.relativedelta import relativedelta
import os
import csv
import sys
import ast

class Spider(object):
    """docstring for AliSpider"""
    def __init__(self):
        self.session = self.create_session("./cookies.txt")

    def generate_overvire():
        pass

    def generate_rank():
        pass

    def generate_keywords(self, keyword, export_path='./csv/'):

        export_filename    = os.path.join( export_path, 'keywords', keyword + '.csv' )
        data_dump_filename = os.path.join( './tmp/keywords/',  keyword + '.dump' )

        os.makedirs(os.path.dirname(export_filename), exist_ok=True)
        os.makedirs(os.path.dirname(data_dump_filename), exist_ok=True)

        if os.path.isfile(export_filename):
            print('[%s] done' % keyword)
            return

        keywords = self.read_dump_file(data_dump_filename)

        page_size = 10
        page_no = int(len(keywords) / page_size) + 1

        with open(data_dump_filename, 'a') as dump_file:
            while True:
                l = self.search_keywords(keyword, page_no=page_no, page_size=page_size)
                if l is None or len(keywords) == l.get('total', None):
                    break

                data = l.get('data', None)
                for data_item in data:
                    dump_file.write("%s\n" % data_item)
                dump_file.flush()
                keywords.extend(data)

                sys.stdout.write('\r[%s] %d - %d' % (keyword, len(keywords), l['total']))
                sys.stdout.flush()
                page_no += 1
        print(' done.')

        headers = [
            ("keywords",          "关键词"),
            ("company_cnt",       "卖家竞争度"),
            ("showwin_cnt",       "橱窗数"),
            ("srh_pv_this_mon",   "搜索热度",
            ("srh_pv_last_1mon",  self.months_ago_str(2)),
            ("srh_pv_last_2mon",  self.months_ago_str(3)),
            ("srh_pv_last_3mon",  self.months_ago_str(4)),
            ("srh_pv_last_4mon",  self.months_ago_str(5)),
            ("srh_pv_last_5mon",  self.months_ago_str(6)),
            ("srh_pv_last_6mon",  self.months_ago_str(7)),
            ("srh_pv_last_7mon",  self.months_ago_str(8)),
            ("srh_pv_last_8mon",  self.months_ago_str(9)),
            ("srh_pv_last_9mon",  self.months_ago_str(10)),
            ("srh_pv_last_10mon", self.months_ago_str(11)),
            ("srh_pv_last_11mon", self.months_ago_str(12)),
            ("repeatKeyword",     "变体词"),
            ("isP4pKeyword",      "P4P关键词"),
            ("yyyymm",            "更新时间")
        ]
        self.dict_writer(filename=export_filename, headers_info=headers_info, data_list=keywords)

    def dict_writer(self, filename, headers_info, data_list):
        data = list()
        for item in data_list:
            data_item = dict()
            for x in headers_info:
                data_item[x[0]] = item.get(x[0], "")
            data.append(data_item)

        with open(filename, 'w', encoding='utf-8-sig') as export_file:
            dict_writer = csv.DictWriter(export_file, fieldnames = [x[0] for x in headers_info])
            dict_writer.writerow(dict(headers))
            dict_writer.writerows(data)

    def read_dump_file(self, dump_filename):

        if os.path.isfile(dump_filename):
            with open(dump_filename, 'r') as data_file:
                keywords = [ast.literal_eval(x) for x in data_file.readlines() if x is not None]

        return None

    def create_session(self, cookies_file_path = "./cookies.txt"):
        s = requests.Session()

        cj = MozillaCookieJar(cookies_file_path)
        cj.load(ignore_discard=True, ignore_expires=True)
        for cookie in cj:
            cookie.expires = time.time() + 14 * 24 * 3600
        s.cookies = cj

        s.headers = {
            'User-Agent':      'Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0',
            'Accept':          '*/*',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection':      'keep-alive'
        }

        s.get("http://i.alibaba.com/index.htm")
        self.delay()

        return s

    def search_keywords(self, keyword, page_no=1, page_size=10):
        api_url = "http://hz-mydata.alibaba.com/industry/.json?action=CommonAction&iName=searchKeywords"
        request_data = {
            'keywords':   keyword,
            'pageNO':     str(page_no),
            'pageSize':   str(page_size),
            'orderBy':    'srh_pv_this_mon',
            'orderModel': 'desc'
        }
        self.session.headers.update({
            'Host':             'hz-mydata.alibaba.com',
            'Content-Type':     'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer':          'http://hz-mydata.alibaba.com/industry/keywords.htm'
        })
        rsp = self.session.post( api_url, data = request_data ).json()
        result = None
        if rsp.get('successed', False):
            result = rsp.get('value', None)

        self.delay()
        return result

    def months_ago_str(self, months):
        time = datetime.now() - relativedelta(months=months)
        return time.strftime('%y年%m月搜索热度')

    def query_products_list(self):
        pass

    def rank_search(self):
        pass

    def delay(self, seconds=10):
        time.sleep(random.uniform(1, seconds))
