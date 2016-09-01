"""爬虫模块
"""

import re
import sys
import time
import os
import pickle
import http.cookiejar
import random
from http.client import HTTPConnection
import requests
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import selenium.webdriver.support.ui as ui
from selenium.common.exceptions import TimeoutException
import parser

HTTPConnection.debuglevel = 0

class Spider():
    """爬虫类"""

    def __init__(self, db):
        self.db = db
        self.cookies = self._get_cookies()
        self.session = self._create_session()

    def craw_products(self, page=1):
        if not self.db.is_products_need_update():
            print("[Product] unneed update")
            return

        page_size = 50
        csrf_token = self._get_product_csrf_token()
        manager = RequestManager()

        first_request = self._prepare_products_request(csrf_token=csrf_token, page=page, page_size=page_size)
        manager.add_request(first_request)


        self.db.clear_products()
        while manager.has_request():
            print("[Product] %02d" % page, end=" ")

            new_request = manager.get_request()
            response = self.send_request(new_request)

            page, products = parser.parse_product(response, page_size)
            self.db.upsert_products(products)
            print("[done]")

            if page is None:
                break

            next_request = self._prepare_products_request(csrf_token=csrf_token, page=page, page_size=page_size)
            manager.add_request(next_request)

    def craw_keywords(self, index=0, page=1, extend_keywords_only=False, products_only=False):
        keywords = self.get_keywords(extend_keywords_only=extend_keywords_only,
                                     products_only=products_only)
        negative_keywords = self.get_negative_keywords()
        keyword_manager = RequestManager()

        page_size = 10
        keyword = keywords[index]
        first_request = self._prepare_keywords_request(keyword, page, page_size)
        keyword_manager.add_request(first_request)

        while keyword_manager.has_request():
            print('[Keyword] %04d-%03d:"%s"' % (index, page, keyword), end=" ")

            new_request = keyword_manager.get_request()
            if self.db.is_keyword_need_update(keyword):
                response = self.send_request(new_request)
                page, page_keywords = parser.parse_keyword(response, page, page_size, negative_keywords)
                self.db.upsert_keywords(page_keywords)
            else:
                print('is exist & unneed update', end=" ")
                page = None
            print("[done]")

            if page is None:
                page = 1
                index = index + 1
                if index < len(keywords):
                    keyword = keywords[index]
                else:
                    break

            next_request = self._prepare_keywords_request(keyword, page, page_size)
            keyword_manager.add_request(next_request)

    def craw_rank(self, index=0, extend_keywords_only=False, products_only=False):
        keywords = self.get_keywords(extend_keywords_only=extend_keywords_only,
                                     products_only=products_only)
        keywords = [re.sub(" +", " ", x.lower()) for x in keywords]
        keyword = keywords[index]
        ctoken = self._get_ctoken()
        manager = RequestManager()

        if index >= len(keywords):
            print('index over range')
            return

        first_request = self._prepare_rank_request(keyword=keyword, ctoken=ctoken)
        manager.add_request(first_request)

        while manager.has_request():
            print('[Rank] %04d:"%s"' % (index, keyword), end=" ")

            if self.db.rank_exsit_unneed_update(keyword):
                print('is exist & unneed update', end=" ")
                index += 1
            else:
                new_request = manager.get_request()
                response = self.send_request(new_request)
                index, rank = parser.parse_rank(response, index, keywords)
                self.db.upsert_rank(rank)
            print("[done]")

            if index is None or index >= len(keywords):
                break
            keyword = keywords[index]
            new_request = self._prepare_rank_request(keyword=keyword, ctoken=ctoken)
            manager.add_request(new_request)

    def craw_p4p(self):
        self.db.clear_p4p()
        manager = RequestManager()
        csrf_token = self._get_p4p_csrf_token()
        page = 1
        first_request = self._prepare_p4p_request(page=page, csrf_token=csrf_token)
        manager.add_request(first_request)

        while manager.has_request():
            print('[P4P] %02d' % page, end=" ")

            new_request = manager.get_request()
            response = self.send_request(new_request)
            page, p4ps = parser.parse_p4p(response=response)
            self.db.add_p4ps(p4ps)
            print("[done]")

            if page is None :
                break
            new_request = self._prepare_p4p_request(page=page, csrf_token=csrf_token)
            manager.add_request(new_request)

    def _prepare_p4p_request(self, page, csrf_token):
        url = "http://www2.alibaba.com/asyGetAdKeyword.do"
        headers = {
            'Host': 'www2.alibaba.com',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://www2.alibaba.com/manage_ad_keyword.htm',
            'Connection': 'keep-alive',
        }
        data = {
            'json': '{"status":"all","cost":"all","click":"all","exposure":"all","cpc":"all",\
"qsStar":"all","kw":"","isExact":"N","date":7,"tagId":-1,"delayShow":false,"recStrategy":1,\
"recType":"recommend","currentPage":%d}' % page,
            '_csrf_token_': csrf_token,
        }
        req = requests.Request('POST', url, data=data, headers=headers, cookies=self.cookies)
        return req.prepare()

    def _prepare_products_request(self, csrf_token, page, page_size):
        url = "http://hz-productposting.alibaba.com/product/managementproducts/asyQueryProductsList.do"
        headers = {
            'Host': 'hz-productposting.alibaba.com',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://hz-productposting.alibaba.com/product/products_manage.htm',
            'Connection': 'keep-alive',
        }
        data = {
            '_csrf_token_': csrf_token,
            'page': str(page),
            'size': str(page_size),
            'status': 'approved',
            'statisticsType': 'month',
            'imageType': 'all',
            'displayStatus': 'all',
            'repositoryType': 'all',
            'samplingTag': 'false',
            'gmtModified': 'asc',
            'marketType': 'all',
        }
        req = requests.Request('POST', url, data=data, headers=headers, cookies=self.cookies)
        return req.prepare()

    def _prepare_keywords_request(self, keyword, page=1, page_size=10):
        url = "http://hz-mydata.alibaba.com/industry/.json?action=CommonAction&iName=searchKeywords"
        headers = {
            'Host': 'hz-mydata.alibaba.com',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://hz-mydata.alibaba.com/industry/keywords.htm',
            'Connection': 'keep-alive',
        }
        data = {
            'keywords': keyword,
            'pageSize': str(page_size),
            'pageNO': str(page),
            'orderBy': 'srh_pv_this_mon',
            'orderModel': 'desc',
        }
        req = requests.Request('POST', url, data=data, headers=headers, cookies=self.cookies)
        return req.prepare()

    def _prepare_rank_request(self, keyword, ctoken):
        url = "http://hz-mydata.alibaba.com/self/.json"
        params = {
            "iName": "getKeywordSearchProducts",
            "action": "CommonAction",
            "ctoken": ctoken,
        }
        headers = {
            'Host': 'hz-mydata.alibaba.com',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://hz-mydata.alibaba.com/self/keyword.htm?spm=a2700.7756200.1998618981.63.32KNMS',
            'Connection': 'keep-alive',
        }
        data = {
            'keyword': keyword,
        }
        req = requests.Request('POST', url, params=params, data=data, headers=headers, cookies=self.cookies)
        return req.prepare()

    def _get_product_csrf_token(self):
        url = "http://hz-productposting.alibaba.com/product/products_manage.htm"
        html = self.session.get(url).text
        pattern = r"(?<={'_csrf_token_':')\w+(?='})"
        product_csrf_token = re.search(pattern, html).group(0)
        return product_csrf_token

    def _get_p4p_csrf_token(self):
        url = "http://www2.alibaba.com/manage_ad_keyword.htm"
        html = self.session.get(url).text
        pattern = r"(?<='_csrf_token_': ')\w+(?=')"
        csrf_token = re.search(pattern, html).group(0)
        return csrf_token

    def _create_session(self):
        """创建 requests session"""
        session = requests.Session()
        session.cookies = self.cookies
        session.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        r = session.get('http://i.alibaba.com/index.htm', allow_redirects=False)
        if r.status_code != requests.codes.ok:
            self.cookies = self._get_cookies(disable_cache=True)
        return session

    def _get_cookies(self, disable_cache=False):
        cookies = None
        dump_file = './cookies.pkl'
        if not disable_cache and os.path.exists(dump_file):
            with open(dump_file, "rb") as dump:
                cookies = pickle.load(dump)
        else:
            cookies = self._get_cookies_via_selenium()
            if os.path.exists(dump_file):
                os.remove(dump_file)
            with open(dump_file, "wb") as dump:
                pickle.dump(cookies, dump)
        return cookies

    def _get_cookies_via_selenium(self):
        # 使用最后一个版本的 wires web driver
        caps = DesiredCapabilities.FIREFOX
        caps["marionette"] = True
        caps["binary"] = "/usr/bin/firefox"
        driver = webdriver.Firefox(capabilities=caps)
        driver.get("http://i.alibaba.com")
        try:
            ui.WebDriverWait(driver, 60).until(
                lambda driver: driver.find_elements_by_class_name('mui-login-info'))
        except TimeoutException:
            print("登陆超时，程序结束，请重试！")
            sys.exit()
        finally:
            driver_cookies = driver.get_cookies()
            driver.quit()
        return self._create_cookies(driver_cookies)

    def _create_cookies(self, driver_cookies):
        if driver_cookies is None:
            return None

        cookies = http.cookiejar.CookieJar()
        for item in driver_cookies:
            item['expires'] = item.get('expiry', None)
            item['rest'] = {'HttpOnly': item.get('httpOnly', None)}
            item.pop("expiry", None)
            item.pop("httpOnly", None)
            item.pop("maxAge", None)
            cookiejar = requests.cookies.create_cookie(**item)
            cookies.set_cookie(cookiejar)

        return cookies

    def _get_ctoken(self):
        if self.cookies is None:
            return None
        for cookie in self.cookies:
            if cookie.name == 'ctoken':
                return cookie.value

    def get_keywords(self, extend_keywords_only=False, products_only=False):
        extend_keywords = self.get_extend_keywords()
        if extend_keywords_only:
            return sorted(extend_keywords)
        keywords = self.db.get_product_keywords()
        if products_only:
            return sorted(keywords)

        negative_keywords = self.get_negative_keywords()
        if extend_keywords is not None and len(extend_keywords) != 0:
            keywords.extend(extend_keywords)
        if negative_keywords is not None and len(negative_keywords) != 0:
            keywords = [x for x in keywords if x not in negative_keywords]
        return sorted(keywords)

    def get_extend_keywords(self):
        extend_keywords = None
        with open('./config/extend_keywords.txt', 'r') as f:
            extend_keywords = f.read().splitlines()
        return extend_keywords

    def get_negative_keywords(self):
        negative_keywords = None
        with open('./config/negative_keywords.txt', 'r') as f:
            negative_keywords = f.read().splitlines()
        return negative_keywords

    def send_request(self, request):
        # 每次请求之间需要有一定的时间间隔
        time.sleep(random.randint(1, 5))
        return self.session.send(request)

class RequestManager():
    """请求队列管理器。"""

    def __init__(self):
        self.requests = set()
        self.old_requests = set()

    def add_request(self, item):
        """添加请求到请求队列中。"""
        if item is None:
            return
        if item not in self.requests and item not in self.old_requests:
            self.requests.add(item)

    def add_requests(self, items):
        """批量添加请求到队列中。"""
        if items is None or len(items) == 0:
            return
        for item in items:
            self.add_request(item)

    def has_request(self):
        """是否存在为处理的请求"""
        return len(self.requests) != 0

    def get_request(self):
        """从请求列表中获取新的请求"""
        request = self.requests.pop()
        self.old_requests.add(request)
        return request
