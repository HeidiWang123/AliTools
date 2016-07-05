import time
import random
import os
import csv
import sys
import ast
import re
import json
from datetime import datetime
from http.cookiejar import MozillaCookieJar
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup

class Spider(object):
    """
    数据结构
    {
        keyword:              关键词,
        product_owner:        负责人,
        product_no:           款号,
        product_rank:         排名,
        update_time:          更新日期,
        top1_rank:            第一位排名,
        top1_no:              第一位款号,
        is_selection_prodcut: 搜索首页精选产品,
        is_ydt_product:       贸易表现产品,
        is_window_product:    橱窗,
        isP4pKeyword:         P4P关键词,
        company_cnt:          卖家竞争度,
        showwin_cnt:          橱窗数,
        srh_pv_this_mon:      搜索热度
    }
    """
    def __init__(self, cookies_file_path='config/cookies.txt', dump_path='./tmp/'):
        self.session   = self.create_session(cookies_file_path)
        os.makedirs(os.path.dirname(dump_path), exist_ok=True)
        self.dump_path = dump_path

    def generate_overvire():
        """
        生成所有产品的关键词款号以及排名的总表
        """
        export_filename = os.path.join(export_path, 'overview.csv')
        dump_filename   = os.path.join(self.dump_path, 'overview.dump')
        data            = self.read_dump_file(dump_filename)
        exist_product   = set([x['product_no'] for x in data])

        with open(dump_filename, 'a') as df:
            self.crawl_overview(data, df)

        headers_info = [
            ('keyword',              '关键词'),
            ('product_owner',        '负责人'),
            ('product_no',           '款号'),
            ('product_rank',         '排名'),
            ('update_time',          '更新日期'),
            ('top1_rank',            '第一位排名'),
            ('top1_no',              '第一位款号'),
            ('is_selection_prodcut', '搜索首页精选产品'),
            ('is_ydt_product',       '贸易表现产品'),
            ('is_window_product',    '橱窗'),
            ('isP4pKeyword',         'P4P关键词'),
            ('company_cnt',          '卖家竞争度'),
            ('showwin_cnt',          '橱窗数'),
            ('srh_pv_this_mon',      '搜索热度')
        ]
        self.dict_writer(filename=export_filename, headers_info=headers_info, data_list=data)

    def generate_rank(self, keywords, product_no=None, export_path="./csv/"):
        """
        根据关键词列表抓取所有关键词的排名
        """
        export_filename = os.path.join(export_path, 'rank.csv')
        dump_filename   = os.path.join(self.dump_path, 'rank.dump')
        data            = self.read_dump_file(dump_filename)
        exist_keywords  = [x['keyword'] for x in data]

        with open(dump_filename, 'a') as df:
            for keyword in keywords:
                keyword = keyword.strip()
                if keyword not in exist_keywords:
                    rank_info = self.get_keyword_rank(keyword)
                    product_rank = None
                    if product_no is not None:
                        for info_item in rank_info:
                            if info_item['product_no'] == product_no:
                                product_rank = info_item['product_rank']
                                break
                    item = {
                        "keyword":      keyword,
                        "product_no":   product_no,
                        "product_rank": product_rank,
                        "top1_rank":    rank_info[0]['product_rank'] if rank_info is not None else None,
                        "top1_no":      rank_info[0]['product_no'] if rank_info is not None else None
                    }
                    data.append(item)
                    df.write("%s\n" % item)
                    df.flush()
                print('[%s] done' % keyword)

        headers_info = [
            ("keyword",      "关键词"),
            ("product_no",   "款号"),
            ("product_rank", "产品排名"),
            ("top1_rank",    "第一位排名"),
            ("top1_no",      "第一位产品")
        ]
        self.dict_writer(filename=export_filename, headers_info=headers_info, data_list=data)


    def generate_keywords(self, keyword, export_path='./csv/'):
        """
        查找给定关键词在热门搜索词中的结果并到处到 csv
        """
        export_filename = os.path.join( export_path, 'keywords', keyword + '.csv' )
        dump_filename   = os.path.join(self.dump_path, 'keywords',  keyword + '.dump' )

        os.makedirs(os.path.dirname(export_filename), exist_ok=True)
        os.makedirs(os.path.dirname(dump_filename), exist_ok=True)

        if os.path.isfile(export_filename):
            print('[%s] done' % keyword)
            return

        keywords = self.read_dump_file(dump_filename)

        page_no = int(len(keywords) / page_size) + 1

        with open(dump_filename, 'a') as dump_file:
            data = self.search_keywords(keyword, page_no=page_no)
            for data_item in data:
                dump_file.write("%s\n" % data_item)
            dump_file.flush()
            keywords.extend(data)
        print(' done.')

        headers_info = [
            ("keywords",          "关键词"),
            ("company_cnt",       "卖家竞争度"),
            ("showwin_cnt",       "橱窗数"),
            ("srh_pv_this_mon",   "搜索热度"),
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
            dict_writer.writerow(dict(headers_info))
            dict_writer.writerows(data)

    def read_dump_file(self, dump_filename):
        data = []
        if os.path.isfile(dump_filename):
            with open(dump_filename, 'r') as data_file:
                data = [ast.literal_eval(x) for x in data_file.readlines() if x is not None]

        return data

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

    def crawl_overview(self, data, dump_file, page=1):
        api_url = "http://hz-productposting.alibaba.com/product/managementproducts/asyQueryProductsList.do"
        self.session.headers.update({
            'Host':             'hz-productposting.alibaba.com',
            'Accept':           'application/json, text/javascript, */*; q=0.01',
            'Content-Type':     'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        })
        page_size = 50
        request_data = {
            '_csrf_token_':   self.get_csrf_token('http://hz-productposting.alibaba.com/product/products_manage.htm'),
            'page':           page,
            'status':         'approved',
            'size':           str(page_size),
            'statisticsType': 'month',
            'imageType':      'all',
            'displayStatus':  'all',
            'repositoryType': 'all',
            'samplingTag':    'false',
            'marketType':     'all'
        }
        self.delay()
        rsp = self.session.post(api_url, data = request_data).json()

        exist_products  = [x['redModel'] for x in data]
        exist_keywords = [x['keyword'] for x in data]

        if rsp is not None and rsp.get('result') is True:
            products     = rsp.get('products', list())
            if item in products:
                keywords   = products.get('keywords')
                product_no = products.get('redModel')
                item_list  = list()
                for keyword in keywords:
                    if keyword not in exist_keywords and product_no not in exist_products:
                        search_keywords = self.search_keywords(keyword)
                        keyword_info    = [x for x in search_keywords if x['keywords'] == keyword][0]
                        rank_info       = self.get_keyword_rank(keyword, product_no)

                        record = dict()
                        record.update(item)
                        record.update(keyword_info)
                        record.update(rank_info)

                        item_list.append(record)
                        dump_file.write('%s\n' % record)
                        dump_file.flush()
                    print("\r [%s - %s] done" % (product_no, keyword))
                data.extend(item_list)
            if len(products) == page_size:
                self.crawl_overview(data, dump_file, page_no + 1)

    def search_keywords(self, keyword, page_no=1, page_size=10):
        self.session.headers.update({
        'Host':             'hz-mydata.alibaba.com',
        'Content-Type':     'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer':          'http://hz-mydata.alibaba.com/industry/keywords.htm'
        })

        api_url = "http://hz-mydata.alibaba.com/industry/.json?action=CommonAction&iName=searchKeywords"
        request_data = {
            'keywords':   keyword,
            'pageNO':     str(page_no),
            'pageSize':   str(page_size),
            'orderBy':    'srh_pv_this_mon',
            'orderModel': 'desc'
        }
        self.delay()
        rsp = self.session.post( api_url, data = request_data ).json()
        if rsp is not None and rsp.get['successed']:
            result = rsp.get['value']['data']

            sys.stdout.write('\r[%s] %d - %d' % (keyword, len(keywords), rsp['value']['total']))
            sys.stdout.flush()

            if len(result) == page_size:
                result.extend( self.search_keywords(keyword, page_no+1) )
            return result

    def months_ago_str(self, months):
        time = datetime.now() - relativedelta(months=months)
        return time.strftime('%y年%m月搜索热度')

    def get_keyword_rank(self, keyword):
        self.session.headers.update({
            'Host':         'hz-productposting.alibaba.com',
            'Accept':       'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Content-Type': 'application/x-www-form-urlencoded'
        })

        api_url = "http://hz-productposting.alibaba.com/product/ranksearch/rankSearch.htm"
        request_data = {
            '_csrf_token_': self.get_csrf_token(api_url),
            'queryString':  keyword
        }
        html = self.session.post(api_url, data = request_data).text
        return self.parse_rank(html)

    def parse_rank(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        rows = soup.select('#rank-searech-table > tbody > tr')
        tips = soup.select('.search-result')

        if '查询太频繁，请明日再试！' in str(tips):
            raise RuntimeError('查询太频繁，请明日再试！')

        if '无匹配结果' in str(rows):
            return None

        results = list()
        for row in rows:
            self.delay()

            product_href = "http:" + row.select('td:nth-of-type(1) > a')[0].get('href')
            rt           = self.session.get(product_href).text
            match_str    = re.findall(r'(?<=attrData.systemAttr = ).*(?=;)', rt)[0]
            attr_data    = json.loads(match_str)
            product_no   = ""
            for item in attr_data:
                if item['data']['value'] == '型号':
                    product_no = item['nodes'][0]['data']['value']

            rank_text    = row.select('td:nth-of-type(2) > a')[0].text.strip()
            product_rank = (lambda x: round(float(x[0]) + float(x[1])/100, 2))(re.findall(r'(\d+)', rank_text))

            charge_spans      = row.select('td:nth-of-type(3) > span')
            selection_prodcut = True if '搜索首页精选产品' in [x.text for x in charge_spans] else False

            results.append({
                'product_no':        product_no,
                'product_rank':      product_rank,
                'selection_prodcut': selection_prodcut
            })

        return sorted(results, key=lambda x:x['product_rank'])

    def get_csrf_token(self, url):
        if self._crsf_url is None:
            self._crsf_url = url
        elif self._crsf_url != url:
            self.delay()
            html = self.session.get(url).text
            soup = BeautifulSoup(html, 'html.parser')
            self._crsf_token = soup.find('input', {'name': '_csrf_token_'}).get('value')
        return self._crsf_token

    def delay(self, seconds=10):
        time.sleep(random.uniform(1, seconds))
