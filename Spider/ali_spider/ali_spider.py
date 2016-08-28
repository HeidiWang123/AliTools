#! /usr/bin/env python

"""
"""

import os.path
from spider import Spider
from models import Database

webdriver_path = os.path.abspath(os.path.join(os.path.dirname(__file__),'webdriver'))
os.environ["PATH"] += os.pathsep + webdriver_path

if __name__ == "__main__":
    db = Database()
    spider = Spider(db)
    # spider.craw_products()
    # spider.craw_keywords()
    spider.craw_rank()
    db.close()
