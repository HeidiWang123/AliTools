"""爬虫模块
"""

import re
import sys
from datetime import datetime
import time
import os
import pickle
import http.cookiejar
import random
from http.client import HTTPConnection
import os.path
import requests
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.common.exceptions import TimeoutException
import selenium.webdriver.support.ui as ui
import parser
import settings


class Crawer():
    """爬虫类"""

    def __init__(self, database):
        self._add_webdriver_to_path()
        self.database = database
        self.cookies = self._get_cookies()
        self.session = self._create_session()
        HTTPConnection.debuglevel = settings.HTTP_DEBUGLEVEL

    @staticmethod
    def _add_webdriver_to_path():
        webdriver_path = os.path.abspath(settings.WEBDRIVER_PATH)
        os.environ["PATH"] += os.pathsep + webdriver_path

    def craw_products(self, page=1):
        """craw products

        Args:
            page (int): beging page from craw, default value is 1.

        """
        if not self.database.is_products_need_update():
            print("[Product] unneed update")
            return

        page_size = 50
        csrf_token = self._get_product_csrf_token()
        manager = RequestManager()

        first_request = self._prepare_products_request(
            csrf_token=csrf_token,
            page=page,
            page_size=page_size
        )
        manager.add_request(first_request)
        self.database.delete_all_products()

        while manager.has_request():
            print("[Product] %02d" % page, end=" ")
            new_request = manager.get_request()
            response = self._send_request(new_request)
            page, products = parser.parse_product(response, page_size)
            self.database.add_products(products)
            print("[done]")

            if page is None:
                break

            next_request = self._prepare_products_request(
                csrf_token=csrf_token,
                page=page,
                page_size=page_size
            )
            manager.add_request(next_request)

    def craw_keywords(self, keywords, index=0, page=1):
        """craw keywords infomation

        craw all keywords and contains products keywords, base keywords and extension keywords.

        Args:
            index (int): the keywords list index for the beginning craw.
            page (int): the keyword request page for the beginning craw.
        """
        keyword_manager = RequestManager()
        page_size = 10
        keyword = keywords[index]
        first_request = self._prepare_keywords_request(keyword, page, page_size)
        keyword_manager.add_request(first_request)

        while keyword_manager.has_request():
            print('[Keyword] %05d-%03d:"%s"' % (index, page, keyword), end=" ")

            new_request = keyword_manager.get_request()
            if page > 1 or self.database.is_keyword_need_upsert(keyword):
                response = self._send_request(new_request)
                page, page_keywords = parser.parse_keyword(
                    response=response,
                    page=page,
                    page_size=page_size
                )
                self.database.upsert_keywords(page_keywords)
            else:
                print('is exist & unneed update', end=" ")
                page = None
            print("[done]")

            if page is None:
                index, page = (index + 1, 1)
                if index >= len(keywords):
                    break
                keyword = keywords[index]

            next_request = self._prepare_keywords_request(keyword, page, page_size)
            keyword_manager.add_request(next_request)

        self.craw_keywords_category()

    def craw_keywords_category(self, index=0):
        """craw keywords category information.

        Args:
            index (int): keywords index, default 0.
        """
        keywords = self.database.get_all_keywords()
        request_manager = RequestManager()
        keyword = keywords[index]
        csrf_token = self._get_category_csrf_token()
        first_request = self._prepare_catrgory_request(keyword=keyword.value, ctoken=csrf_token)
        request_manager.add_request(first_request)

        while request_manager.has_request():
            print('[Category] %05d:"%s"' % (index, keyword.value), end=" ")

            new_request = request_manager.get_request()
            if self.database.is_keyword_category_need_update(keyword):
                response = self._send_request(new_request)
                index, category = parser.parse_category(response=response, index=index)
                self.database.update_keyword_category(keyword=keyword, category=category)
            else:
                print('is unneed update', end=" ")
                index += 1
            print("[done]")
            if index >= len(keywords):
                break
            keyword = keywords[index]
            next_request = self._prepare_catrgory_request(keyword=keyword.value, ctoken=csrf_token)
            request_manager.add_request(next_request)

    def craw_rank(self, keywords, index=0):
        """craw keywords rank information.

        Args:
            index (int): the beginning craw index of the keywords list.
        """
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

            if self.database.is_rank_need_upsert(keyword):
                new_request = manager.get_request()
                response = self._send_request(new_request)
                index, rank = parser.parse_rank(response, index, keywords)
                self.database.upsert_rank(rank)
            else:
                print('is exist & unneed update', end=" ")
                index += 1
            print("[done]")

            if index is None or index >= len(keywords):
                break
            keyword = keywords[index]
            new_request = self._prepare_rank_request(keyword=keyword, ctoken=ctoken)
            manager.add_request(new_request)

    def craw_p4p(self):
        """craw p4p keywords and information"""

        self.database.delete_all_p4p()
        manager = RequestManager()
        csrf_token = self._get_p4p_csrf_token()
        page = 1
        first_request = self._prepare_p4p_request(page=page, csrf_token=csrf_token)
        manager.add_request(first_request)

        while manager.has_request():
            print('[P4P] %02d' % page, end=" ")

            new_request = manager.get_request()
            response = self._send_request(new_request)
            page, p4ps = parser.parse_p4p(response=response)
            self.database.add_p4ps(p4ps)
            print("[done]")

            if page is None:
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
        url = "http://hz-productposting.alibaba.com/product/managementproducts/\
asyQueryProductsList.do"
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

    def _prepare_catrgory_request(self, keyword, ctoken):
        url = "http://hz-productposting.alibaba.com/product/cate/AjaxRecommendPostCategory.htm"
        params = {
            "keyword": keyword,
            "ctoken": ctoken,
            "origin": None,
            "_": ("%.3f" % datetime.now().timestamp()).replace(".",""),
            "language": "en_us",
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://hz-productposting.alibaba.com/product/posting.htm?spm=a2700.7756200.1998618981.74.gE0qEV',
            'Connection': 'keep-alive',
        }
        req = requests.Request('GET', url, params=params, headers=headers, cookies=self.cookies)
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
            'Referer': 'http://hz-mydata.alibaba.com/self/keyword.htm?\
spm=a2700.7756200.1998618981.63.32KNMS',
            'Connection': 'keep-alive',
        }
        data = {
            'keyword': keyword,
        }
        req = requests.Request(
            'POST', url, params=params, data=data, headers=headers,
            cookies=self.cookies
        )
        return req.prepare()

    def _get_product_csrf_token(self):
        url = "http://hz-productposting.alibaba.com/product/products_manage.htm"
        html = self.session.get(url).text
        pattern = r"(?<={'_csrf_token_':')\w+(?='})"
        product_csrf_token = re.search(pattern, html).group(0)
        return product_csrf_token

    def _get_category_csrf_token(self):
        url = "http://hz-productposting.alibaba.com/product/posting.htm"
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
        resp = session.get('http://i.alibaba.com/index.htm', allow_redirects=False)
        if resp.status_code != 200:
            self.cookies = self._get_cookies(disable_cache=True)
        return session

    def _get_cookies(self, disable_cache=False):
        cookies = None
        dump_file = './cookies.pkl'
        if disable_cache or not os.path.exists(dump_file):
            cookies = self._get_cookies_via_selenium()
            if os.path.exists(dump_file):
                os.remove(dump_file)
            with open(dump_file, "wb") as dump:
                pickle.dump(cookies, dump)
        else:
            with open(dump_file, "rb") as dump:
                cookies = pickle.load(dump)
        return cookies

    def _get_cookies_via_selenium(self):
        # 使用最后一个版本的 wires web driver
        caps = DesiredCapabilities.FIREFOX
        caps["marionette"] = True
        caps["binary"] = "/usr/bin/firefox"
        profile = FirefoxProfile()
        profile.set_preference('permissions.default.stylesheet', 2)
        profile.set_preference('permissions.default.image', 2)
        driver = webdriver.Firefox(profile, capabilities=caps)
        driver.get("http://i.alibaba.com")
        try:
            driver.switch_to_frame(driver.find_element_by_id("alibaba-login-box"))
            login_id = driver.find_element_by_id("fm-login-id")
            login_id.clear()
            login_id.send_keys(settings.LOGIN_ID)
            login_password = driver.find_element_by_id("fm-login-password")
            login_password.clear()
            login_password.send_keys(settings.LOGIN_PASSWORD)
            driver.switch_to_default_content()
            ui.WebDriverWait(driver, settings.LOGIN_TIMEOUT).until(
                lambda driver: driver.find_elements_by_class_name('mui-login-info'))
        except TimeoutException:
            print("登陆超时，程序结束，请重试！")
            sys.exit()
        finally:
            driver_cookies = driver.get_cookies()
            driver.quit()

        return self._create_cookies(driver_cookies)
    @staticmethod
    def _create_cookies(driver_cookies):
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

    def _send_request(self, request):
        # 每次请求之间需要有一定的时间间隔
        time.sleep(random.randint(1, 3))
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
