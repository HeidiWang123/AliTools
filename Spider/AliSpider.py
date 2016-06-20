#!/usr/bin/env python

import configparser
import re
import json
import time
import math
import requests
import sys
from bs4 import BeautifulSoup
import random
import csv
import os.path
import ast

def printProgress (iteration, total, prefix = '', suffix = '', decimals = 2, barLength = 50):
    filledLength = int(round(barLength * iteration / float(total)))
    percents     = round(100.00 * (iteration / float(total)), decimals)
    bar          = '#' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('%s [%s] %.2f%s %s\r' % (prefix, bar, percents, '%', suffix)),
    sys.stdout.flush()
    if iteration == total:
        print("\n")

class AliSpider():
    def __init__(self):
        self._decode_curls()
        self._build_sessions()

    def _build_sessions(self):
        self.request_session = ({
            "products": self._create_session(self.request_info['products']),
            "keywords": self._create_session(self.request_info['keywords']),
            "rank"    : self._create_session(self.request_info['rank'])
        })

    def _create_session(self, request_info):
        r = requests.Session()
        r.headers.update(request_info['headers'])
        self._delay()
        r.post(
            request_info['url'],
            data    = request_info['data'],
            cookies = request_info['cookies']
        )
        return r

    def _decode_curls(self):
        cf = configparser.RawConfigParser()
        cf.read('config.ini')

        product_curl  = cf.get("curls", "products")
        keywords_curl = cf.get("curls", "keywords")
        rank_curl     = cf.get("curls", "rank")

        self.request_info = {
            "products" : self._build_request_info(product_curl),
            "keywords" : self._build_request_info(keywords_curl),
            "rank"     : self._build_request_info(rank_curl)
        }

    def _build_request_info(self, curl_command):
        pattern_url     = re.compile(r"(?<=curl ')http://\S+(?=')")
        pattern_header  = re.compile(r"(?!Cookie)(?<=-H ')\S*: .+?(?=')")
        pattern_cookies = re.compile(r"(?<=-H 'Cookie: ).*(?=' -H)")
        pattern_data    = re.compile(r"(?<=--data ').*(?=')")
        return {
            "url"    : pattern_url.findall(curl_command)[0],
            "headers": dict(x.split(': ', 1) for x in pattern_header.findall(curl_command)),
            "cookies": dict(x.strip().split('=', 1) for x in pattern_cookies.findall(curl_command)[0].split(';')),
            "data"   : dict(x.strip().split('=', 1) for x in pattern_data.findall(curl_command)[0].split('&'))
        }

    def get_data(self):
        self.data      = list()
        begin_index    = 0
        data_dump_path = './data.dump'
        if os.path.isfile(data_dump_path):
            with open(data_dump_path, 'r') as data_file:
                self.data       = [ast.literal_eval(x) for x in data_file.readlines() if x is not None]

        with open(data_dump_path, 'a') as self.data_file:
            self._crawl_data()

        os.remove(data_dump_path)

        return self.data

    def _get_product_no_list(self):
        try:
            self._dumped_product_no_list
        except Exception as e:
            self._dumped_product_no_list = [x['product_no'] for x in self.data]

        return self._dumped_product_no_list

    def _crawl_data(self, begin_index = 0):
        keywords_request_info = self.request_info['keywords']
        rank_request_info     = self.request_info['rank']

        page_size  = 50
        page       = begin_index / page_size + 1
        page_index = begin_index % page_size
        page_count = 0

        while (page_count is 0 or page <= page_count):
            print("Start crawl page %d ......" % page)

            result = self._crawl_products(page_size, page)

            if page_count is 0:
                page_count = math.ceil(result['count'] / page_size)

            self._build_data_item(result, page_index)

            if page_index is not 0:
                page_index = 0

            page += 1

    def _build_data_item(self, data, page_index = 0):
        products_count = len(data['products'])
        for index in range(products_count):
            if index < page_index:
                continue

            printProgress(
                index + 1,
                products_count,
                prefix = "[processing]",
                suffix = "Page %d / %d" % (index + 1, products_count)
            )

            item              = data['products'][index]
            product_keywords  = item['keywords'].split(',')
            product_owner     = item['ownerMemberName']
            product_no        = item['redModel']
            product_id        = item['id']
            is_window_product = item['isWindowProduct']
            is_ydt_product    = item['mappedToYdtProduct']

            if item['displayStatus'] is 'y':
                product_data_list = list()
                is_in_dumped_list = False
                for keyword in product_keywords:
                    data_item = {
                        'keyword'           : keyword,
                        'product_owner'     : product_owner,
                        'product_no'        : product_no,
                        'product_id'        : product_id,
                        'is_window_product' : is_window_product,
                        'is_ydt_product'    : is_ydt_product
                    }
                    if not is_in_dumped_list and product_no in self._get_product_no_list():
                        is_in_dumped_list = True
                        break

                    data_item.update(self._get_keywords_info(keyword))
                    data_item.update(self._get_rank_info(keyword = keyword, product_id = product_id))
                    product_data_list.append(data_item)

                if not is_in_dumped_list:
                    self.data.append(data_item)

                    for data_item in product_data_list:
                        self.data_file.write("%s\n" % data_item)
                        self.data_file.flush()

    def _get_keywords_info(self, keyword):
        current_page = 1
        page_count   = 0
        page_size    = 10
        while (page_count is 0 or current_page <= page_count):
            result = self._crawl_keywords(keyword, page_size = page_size, page_index = current_page)

            total_count = result['value']['total']
            if total_count is 0:
                break

            if page_count is 0:
                page_count = math.ceil(total_count / page_size)

            keyword_info = self._get_keyword_info_from_list(keyword, result['value']['data'])

            if keyword_info is not None:
                data_item = {
                    "keyword_company_count"         : keyword_info['company_cnt'],
                    "keyword_window_products_count" : keyword_info['showwin_cnt'],
                    "keyword_pv_rank"               : keyword_info['srh_pv_this_mon'],
                    "is_p4p_keyword"                : keyword_info['isP4pKeyword']
                }
                return data_item

            current_page += 1

        return {
            "keyword_company_count"         : None,
            "keyword_window_products_count" : None,
            "keyword_pv_rank"               : None,
            "is_p4p_keyword"                : None
        }

    def _get_keyword_info_from_list(self, keyword, keyword_info_list):
        for item in keyword_info_list:
            if item['keywords'] == keyword:
                return item
        return None

    def _get_rank_info(self, keyword, product_id):
        result        = self._crawl_rank(keyword)
        crawl_results = self._parse_crawl_result(result)

        if crawl_results is None:
            return {
                "rank_top1_product_id"       : None,
                "rank_top1_product_position" : None,
                "rank_product_position"      : None,
                "is_selection_prodcut"       : False
            }

        current_product = dict()
        top1_product    = dict()

        for item in crawl_results:
            if item['product_id'] == product_id:
                current_product = item

        crawl_results_sorted = sorted(crawl_results, key=lambda x: x['position'])
        top1_product    = crawl_results_sorted[0]

        return {
            "rank_top1_product_id"       : top1_product['product_id'],
            "rank_top1_product_position" : top1_product['position'],
            "rank_product_position"      : current_product.get('position', None),
            "is_selection_prodcut"  json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
     : current_product.get('is_selection_prodcut', None) or top1_product.get('is_selection_prodcut', None)
        }

    def _parse_crawl_result(self, result):
        soup = BeautifulSoup(result, 'html.parser')
        rows = soup.select('#rank-searech-table > tbody > tr')
        tips = soup.select('.search-result')

        if '查询太频繁，请明日再试！' in str(tips):
            raise RuntimeError('查询太频繁，请明日再试！') from error

        if '无匹配结果' in str(rows):
            return None

        search_results = list()
        for row in rows:
            product_href = row.select('td:nth-of-type(1) > a')[0].get('href')
            rank_text    = row.select('td:nth-of-type(2) > a')[0].text.strip()
            charge_spans = row.select('td:nth-of-type(3) > span')

            product_id           = re.findall(r'(?<=id=)\d+', product_href)[0]
            product_rank         = (lambda x: round(float(x[0]) + float(x[1])/100, 2))(re.findall(r'(\d+)', rank_text))
            is_selection_prodcut = True if '搜索首页精选产品' in [x.text for x in charge_spans] else False

            search_results.append({
                "product_id"          : product_id,
                "position"            : product_rank,
                "is_selection_prodcut": is_selection_prodcut
            })
        return search_results

    def _crawl_products(self, page_size, page):
        item = 'products'
        request_url     = self.request_info[item]['url']
        request_data    = self.request_info[item]['data']

        request_data['size']        = int(page_size)
        request_data['page']        = int(page)
        request_data['gmtModified'] = "asc"

        self._delay()

        r = self.request_session[item].post( request_url, data = request_data)
        r.raise_for_status()
        return r.json()

    def _crawl_keywords(self, keyword, page_size = 10, page_index = 0):
        item = 'keywords'
        request_url     = self.request_info[item]['url']
        request_data    = self.request_info[item]['data']

        request_data['keywords'] = keyword
        request_data['pageSize'] = int(page_size)
        request_data['pageNO']   = int(page_index)

        self._delay()

        r = self.request_session[item].post( request_url, data = request_data)
        r.raise_for_status()
        return r.json()

    def _crawl_rank(self, keyword):
        item = 'rank'
        request_url     = self.request_info[item]['url']
        request_data    = self.request_info[item]['data']

        request_data['queryString'] = keyword

        self._delay()

        r = self.request_session[item].post( request_url, data = request_data)
        r.raise_for_status()
        return r.content

    def _delay(self):
        time.sleep(random.uniform(1, 3))

if __name__ == '__main__':
    spider = AliSpider()
    data = list
    while True:
        try:
            data = spider.get_data()
        except requests.exceptions.ConnectionError as e:
            time.sleep(100)
            continue
        except Exception as e:
            print(e.value)
        break

    with open('./output.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(
            output_file,
            fieldnames = data[0].keys(),
            delimiter = ',',
            quotechar = '|',
            quoting = csv.QUOTE_MINIMAL
        )
        dict_writer.writeheader()
        dict_writer.writerows(data)
