import re
import sys
import time
import pickle
from http.client import HTTPConnection
import requests
import http.cookiejar
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import selenium.webdriver.support.ui as ui
from selenium.common.exceptions import TimeoutException
import parser

HTTPConnection.debuglevel = 0

class Spider():
    """docstring for Spider."""

    _session = None
    def __init__(self, db):
        self._create_session()
        self.db = db
        self.manager = RequestManager()

    def _create_session(self):
        """从浏览器读取 cookies 信息，创建并返回 requests session"""
        if Spider._session is not None:
            return

        session = requests.Session()
        session.cookies = self._get_cookies()
        session.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        Spider._session = session

    def _get_cookies(self):
        cookies = None
        try:
            with open("cookies.pkl", "rb") as dump:
                cookies = pickle.load(dump)
        except FileNotFoundError:
            cookies = self._get_cookies_via_selenium()
            with open("cookies.pkl", "wb") as dump:
                pickle.dump(cookies, dump)
        return cookies

    def _get_cookies_via_selenium(self):
        cookies = None

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

        if driver_cookies is not None:
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

class ProductSpider(Spider):
    """docstring for ProductSpider."""

    _csrf_token = None

    def _get_csrf_token(self):
        """获取 csrf_token 值"""
        if ProductSpider._csrf_token is not None:
            return ProductSpider._csrf_token

        url = "http://hz-productposting.alibaba.com/product/products_manage.htm"
        html = Spider._session.get(url).text
        pattern = r"(?<={'_csrf_token_':')\w+(?='})"
        _csrf_token = re.search(pattern, html).group(0)
        return _csrf_token

    def _prepare_request(self, page=1):
        """根据 page 制作 prepare requests 请求"""

        url = "http://hz-productposting.alibaba.com/product/managementproducts/\
asyQueryProductsList.do"
        data = {
            '_csrf_token_': self._get_csrf_token(),
            'status': 'approved',
            'page': str(page),
            'size': '50',
            'statisticsType': 'month',
            'imageType': 'all',
            'displayStatus': 'all',
            'repositoryType': 'all',
            'samplingTag': 'false',
            'gmtModified': 'asc',
            'marketType': 'all',
        }
        headers = Spider._session.headers.update({
            'Host': 'hz-productposting.alibaba.com',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'
        })
        cookies = Spider._session.cookies
        req = requests.Request('POST', url, data=data, headers=headers, cookies=cookies)
        return req.prepare()

    def craw(self):
        """开始抓取"""
        start_page = 15
        new_request = self._prepare_request(start_page)
        self.manager.add_request(new_request)

        while self.manager.has_request():
            time.sleep(5)
            new_request = self.manager.get_request()

            response = self._session.send(new_request)

            new_page, products = parser.parse_product(response)
            self.db.add_products(products)

            if new_page is not None:
                new_request = self._prepare_request(new_page)
                self.manager.add_request(new_request)

            print("%d page done." % (new_page - 1))

class RequestManager():
    """docstring for RequestManager."""
    def __init__(self):
        self.requests = set()
        self.old_requests = set()

    def add_request(self, item):
        if item is None:
            return
        if item not in self.requests and item not in self.old_requests:
            self.requests.add(item)

    def add_requests(self, items):
        if items is None or len(items) == 0:
            return
        for item in items:
            self.add_request(item)

    def has_request(self):
        return len(self.requests) != 0

    def get_request(self):
        request = self.requests.pop()
        self.old_requests.add(request)
        return request
