#! /usr/bin/env python

"""Spider Main
"""

import argparse
import json
from crawler import Crawler
from database import Database
from generator import CSV_Generator

database = Database()

def craw(args=None):
    """craw command bind function
    """
    
    try:
        crawler = Crawler(database=database)
        func = {
            "products": crawler.craw_products,
            "keywords": crawler.craw_keywords,
            "rank": crawler.craw_rank,
            "p4p": crawler.craw_p4p,
        }

        if args is not None:
            actions = [args.action]
            forceupdate = args.forceupdate
        else:
            actions = ['products', 'keywords', 'rank']
            forceupdate = False
            
        for action in actions:
            func.get(action)(forceupdate=forceupdate)
            
    except Exception:
        raise

def generate(args=None):
    """generate bind function
    """
    try:
        csv_generator = CSV_Generator(database=database)
        func = {
            'overview': csv_generator.generate_overview_csv,
            'keywords': csv_generator.generate_keywords_csv,
            'p4p': csv_generator.generate_p4p_csv,
        }
        if args is not None:
            actions = [args.action]
        else:
            actions = ['overview']
            
        for action in actions:
            func.get(action)()

    except Exception:
        raise

def main():
    """main function
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    craw_parser = subparsers.add_parser('craw', help="craw data and save to database")
    generate_parser = subparsers.add_parser('generate', help="generate csv file")
    craw_parser.add_argument(
        'action', choices=['products', 'keywords', 'rank', 'p4p'],
        help='craw specific type items'
    )
    craw_parser.add_argument(
        '-f', dest="forceupdate", action="store_true", help='force update'
    )
    generate_parser.add_argument(
        'action', choices=['overview', 'keywords', 'p4p'],
        help='generate specific csv file'
    )
    craw_parser.set_defaults(func=craw)
    generate_parser.set_defaults(func=generate)
    args = parser.parse_args()
    try:
        args.func(args)
    except AttributeError:
        craw()
        generate()
    finally:
        database.close()

if __name__ == "__main__":
    main()
