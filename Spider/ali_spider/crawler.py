"""爬虫模块
"""

import re
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import pickle
import random
import http.cookiejar
import http.client
import requests
from selenium.common import exceptions as selenium_exceptions
from selenium import webdriver
import selenium.webdriver.support.ui as ui
import crawlerparser
import settings

class Crawler():
    """爬虫类"""

    def __init__(self, database):
        self._init_webdriver()
        self.database = database
        self.cookies = self._get_cookies()
        self.session = self._create_session()
        http.client.HTTPConnection.debuglevel = settings.HTTP_DEBUGLEVEL

    @staticmethod
    def _init_webdriver():
        webdriver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), './webdriver')
        os.environ["PATH"] += os.pathsep + webdriver_path

    def craw_products(self, page=1, forceupdate=False):
        """craw products

        Args:
            page (int): beging page from craw, default value is 1.

        """
        if not forceupdate and not self.database.is_products_need_update():
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
            page, products = crawlerparser.parse_product(response, page_size)
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
        
    def craw_keywords(self, keywords=None, index=0, page=1, forceupdate=False):
        """craw keywords infomation

        craw all keywords and contains products keywords, base keywords and extension keywords.

        Args:
            index (int): the keywords list index for the beginning craw.
            page (int): the keyword request page for the beginning craw.
        """

        if keywords is None:
            keywords = self.database.get_product_keywords()
        
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
                page, page_keywords = crawlerparser.parse_keyword(
                    keyword=keyword,
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
        
    def craw_keywords_category(self, index=0, forceupdate=False):
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
                index, category = crawlerparser.parse_category(response=response, index=index)
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
        
    def craw_rank(self, keywords=None, index=0, forceupdate=False):
        """craw keywords rank information.

        Args:
            index (int): the beginning craw index of the keywords list.
        """
        if keywords is None:
            keywords = self.database.get_craw_keywords()
        
        keywords = [re.sub(" +", " ", x.lower()) for x in keywords]
        ctoken = self._get_ctoken()
        dmtrack_pageid = self._get_dmtrack_pageid()

        if index >= len(keywords):
            print('index over range')
            return

        while index is not None and index < len(keywords):
            keyword = keywords[index]

            print('[Rank] %04d:"%s"' % (index, keyword), end=" ")
            
            if forceupdate or self.database.is_rank_need_upsert(keyword):
                new_request = self._prepare_rank_request(keyword=keyword, ctoken=ctoken, dmtrack_pageid=dmtrack_pageid)
                response = self._send_request(new_request)
                index, rank = crawlerparser.parse_rank(response, index, keywords)
                self.database.upsert_rank(rank)
            else:
                print('is exist & unneed update', end=" ")
                index += 1
            
            print("[done]")
      
    def craw_p4p(self, forceupdate=False):
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
            page, p4ps = crawlerparser.parse_p4p(response=response)
            self.database.add_p4ps(p4ps)
            print("[done]")

            if page is None:
                break
            new_request = self._prepare_p4p_request(page=page, csrf_token=csrf_token)
            manager.add_request(new_request)
        
    @staticmethod
    def _prepare_p4p_request(page, csrf_token):
        url = "http://www2.alibaba.com/asyGetAdKeyword.do"
        headers = {
            'Host': 'www2.alibaba.com',
            'User-Agent': '"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:45.0) Gecko/20100101 Firefox/45.0"',
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
        req = requests.Request('POST', url, data=data, headers=headers)
        return req

    @staticmethod
    def _prepare_products_request(csrf_token, page, page_size):
        url = "http://hz-productposting.alibaba.com/product/managementproducts/\
asyQueryProductsList.do"
        headers = {
            'Host': 'hz-productposting.alibaba.com',
            'User-Agent': '"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:45.0) Gecko/20100101 Firefox/45.0"',
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
        req = requests.Request('POST', url, data=data, headers=headers)
        return req

    @staticmethod
    def _prepare_keywords_request(keyword, page=1, page_size=10):
        url = "http://hz-mydata.alibaba.com/industry/.json?action=CommonAction&iName=searchKeywords"
        headers = {
            'Host': 'hz-mydata.alibaba.com',
            'User-Agent': '"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:45.0) Gecko/20100101 Firefox/45.0"',
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
        req = requests.Request('POST', url, data=data, headers=headers)
        return req

    def _prepare_catrgory_request(self, keyword, ctoken):
        url = "http://hz-productposting.alibaba.com/product/cate/AjaxRecommendPostCategory.htm"
        params = {
            "keyword": keyword,
            "ctoken": ctoken,
            "origin": None,
            "_": self._get_ali_timestamp(),
            "language": "en_us",
        }
        headers = {
            'User-Agent': '"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:45.0) Gecko/20100101 Firefox/45.0"',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://hz-productposting.alibaba.com/product/posting.htm',
            'Connection': 'keep-alive',
        }
        req = requests.Request('GET', url, params=params, headers=headers)
        return req

    @staticmethod
    def _get_ali_timestamp():
        return ("%.3f" % datetime.now().timestamp()).replace(".", "")

    @staticmethod
    def _prepare_rank_request(keyword, ctoken, dmtrack_pageid):
        url = "http://hz-mydata.alibaba.com/self/.json?%s" % random.random()
        params = {
            "iName": "getKeywordSearchProducts",
            "action": "CommonAction",
            "ctoken": ctoken,
            "dmtrack_pageid": dmtrack_pageid,
        }
        headers = {
            'Host': 'hz-mydata.alibaba.com',
            'User-Agent': '"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:45.0) Gecko/20100101 Firefox/45.0"',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Origin': 'http://hz-mydata.alibaba.com',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://hz-mydata.alibaba.com/self/keyword.htm',
            'Connection': 'keep-alive',
        }
        data = {
            'keyword': keyword,
        }
        req = requests.Request('POST', url, params=params, data=data, headers=headers)
        return req

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
            'User-Agent': '"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:45.0) Gecko/20100101 Firefox/45.0"',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        resp = session.get('http://i.alibaba.com/index.htm', allow_redirects=False)
        if resp.status_code != 200:
            session.cookies = self.cookies = self._get_cookies(force_update=True)
            session.get('http://i.alibaba.com/index.htm')
        session.get('http://hz-mydata.alibaba.com/self/keyword.htm')
        session.get('http://hz-productposting.alibaba.com/product/products_manage.htm')
        session.get('http://hz-mydata.alibaba.com/industry/keywords.htm')
        session.get('http://hz-productposting.alibaba.com/product/posting.htm')
        session.get('http://www2.alibaba.com/home/index.htm')
        return session

    def _get_cookies(self, force_update=False):
        cookies = None
        dump_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), './cookies.pkl')
        if force_update or not os.path.exists(dump_file):
            cookies = self._get_cookies_via_selenium()
            self._dump_cookies(cookies=cookies)
        else:
            with open(dump_file, "rb") as dump:
                cookies = pickle.load(dump)
        return cookies

    def _dump_cookies(self, cookies=None):
        dump_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), './cookies.pkl')
        if os.path.exists(dump_file):
            os.remove(dump_file)
        with open(dump_file, "wb") as dump:
            if cookies is None:
                cookies = self.session.cookies
            pickle.dump(cookies, dump)
        
    def _get_cookies_via_selenium(self):
        driver = webdriver.Firefox()
        try:
            driver.set_page_load_timeout(settings.LOGIN_TIMEOUT)
            driver.get("http://i.alibaba.com")
        except selenium_exceptions.TimeoutException:
            pass

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
                lambda driver: "i.alibaba.com/index.htm" in driver.current_url)
        except selenium_exceptions.TimeoutException:
            print("登陆超时，程序结束，请重试！")
            sys.exit()
        finally:
            driver_cookies = driver.get_cookies()
            driver.quit()
            logfile = os.path.join(os.getcwd(), "geckodriver.log")
            if os.path.exists(logfile):
                os.remove(logfile)

        return self._create_cookies(driver_cookies)

    @staticmethod
    def _create_cookies(driver_cookies):
        if driver_cookies is None:
            return None

        cookies = requests.utils.cookiejar_from_dict({})
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
        if self.session.cookies is None:
            return None
        for cookie in self.session.cookies:
            if cookie.name == 'xman_us_t':
                pattern = r"(?<=ctoken=)\w+(?=&)"
                return re.search(pattern, cookie.value).group(0)

    def _get_dmtrack_pageid(self):
        url = "http://hz-mydata.alibaba.com/self/keyword.htm"
        html = self.session.get(url).text
        pattern = r"(?<=dmtrack_pageid=')\w+(?=')"
        dmtrack_pageid = re.search(pattern, html).group(0)
        return dmtrack_pageid
        
    def _send_request(self, request):
        # 每次请求之间需要有一定的时间间隔
        if request.cookies is None:
            self.cookies.update(self.session.cookies)
        else:
            self.cookies.update(request.cookies)
        request.cookies = self.cookies
        resp = self.session.send(request.prepare())
        time.sleep(random.uniform(settings.CRAW_SLEEP_MIN, settings.CRAW_SLEEP_MAX))
        return resp

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
