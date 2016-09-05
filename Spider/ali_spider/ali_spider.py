#! /usr/bin/env python

"""
"""

from spider import Spider
from database import Database
from generator import CSV_Generator


if __name__ == "__main__":
    database = Database()
    try:
        # spider = Spider(database=database)
        # spider.craw_products()
        # spider.craw_keywords(index=34, page=25)
        # products = database.get_products()
        # print(products[0].keywords)
        csv_generator = CSV_Generator(database=database)
        csv_generator.generate_month_keywords_csv()
        # csv_generator.generate_alibaba_csv()
    except Exception as e:
        raise
    finally:
        database.close()
