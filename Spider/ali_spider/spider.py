#! /usr/bin/env python

"""
"""

from crawer import Crawer
from database import Database
from generator import CSV_Generator


if __name__ == "__main__":
    database = Database()
    try:
        ali_crawer = Crawer(database=database)
        ali_crawer.craw_products()
        keywords = database.get_craw_keywords()
        ali_crawer.craw_keywords(keywords=keywords)
        ali_crawer.craw_rank(keywords=keywords)
        csv_generator = CSV_Generator(database=database)
        csv_generator.generate_overview_csv(keywords=keywords)
    except Exception as e:
        raise
    finally:
        database.close()
