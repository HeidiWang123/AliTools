#! /usr/bin/env python

"""Spider Main
"""

import argparse
from crawer import Crawer
from database import Database
from generator import CSV_Generator

def craw(craw_args):
    """craw command bind function
    """
    database = Database()
    try:
        ali_crawer = Crawer(database=database)
        actions = {
            'products': ali_crawer.craw_products,
            'keywords': ali_crawer.craw_keywords,
            'rank': ali_crawer.craw_rank,
            'p4p': ali_crawer.craw_p4p,
            'keywordscategory': ali_crawer.craw_keywords_category,
        }
        for arg in craw_args.action:
            action_args = {
                'products': {},
                'keywords': {'keywords': database.get_craw_keywords()},
                'rank': {'keywords': database.get_craw_keywords()},
                'p4p': {},
                'keywordscategory': {},
            }
            actions.get(arg)(**action_args.get(arg))
    except Exception:
        raise
    finally:
        database.close()

def generate(generate_args):
    """generate bind function
    """
    database = Database()
    try:
        csv_generator = CSV_Generator(database=database)
        actions = {
            'overview': csv_generator.generate_overview_csv,
            'keywords': csv_generator.generate_keywords_csv,
            'allkeywords': csv_generator.generate_keywords_csv,
            'p4p': csv_generator.generate_p4p_csv,
        }
        for arg in generate_args.action:
            action_args = {
                'overview': {'keywords': database.get_craw_keywords()},
                'keywords': {},
                'allkeywords': {'is_all': True},
                'p4p': {},
            }
            actions.get(arg)(**action_args.get(arg))
    except Exception:
        raise
    finally:
        database.close()

def main():
    """main function
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    craw_parser = subparsers.add_parser('craw', help="craw data and save to database")
    generate_parser = subparsers.add_parser('generate', help="generate csv file")
    craw_parser.add_argument(
        '-a', '--action', action='append', choices=['products', 'keywords', 'rank', 'p4p', 'keywordscategory'],
        help='craw specific type items'
    )
    generate_parser.add_argument(
        '-a', '--action', action='append', choices=['overview', 'keywords', 'allkeywords', 'p4p'],
        help='generate specific csv file'
    )
    craw_parser.set_defaults(func=craw)
    generate_parser.set_defaults(func=generate)
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
