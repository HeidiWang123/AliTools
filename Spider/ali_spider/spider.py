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
        # crawer.craw_products()
        # crawer.craw_keywords(index=34, page=25)
        keywords=[
            "bag",
            "handbag",
            "hand bag",
            "women's bag",
            "leather bag",
            "jute bag",
            "tote bag",
            "woman handbag",
            "purse",
            "lady bag",
            "canvas bag",
            "clutch bag",
            "diaper bag",
            "beach bag",
            "lady hand bag",
            "lady handbag",
            "messenger bag",
            "shoulder bag",
            "sling bag",
            "man bag",
            "wallets leather men",
            "ladies purse",
            "backpack tactical",
            "baby bag",
            "designer handbag",
            "italian shoes and bag set",
            "susen handbag",
            "woman hand bag 2016 designer",
            "clutch",
            "canvas tote bag",
            "coin purse",
            "straw bag",
            "bag to",
            "handbags women's bag",
            "ladies pars",
            "portfolio",
            "anello bag",
            "fashion handbag 2016",
            "low price ladies pars hand ladies wallet",
            "jelly bag",
            "taobao bag",
            "designer bag",
            "evening bag",
            "evening clutch bags",
            "nylon bag",
            "chanel bag",
            "genuine leather handbag",
            "silicone bag",
            "felt bag",
            "canvas shoulder bag",
            "o bag",
            "hand bags women handbag",
            "fashion bag ladies handbag 2016",
            "vuiton bag",
            "men leather bag",
            "mens messenger bag",
            "newest pictures lady fashion handbag",
            "lady leather handbag",
            "fashion bag",
            "lady leather bag",
            "genuine leather bag",
            "ladies bags images",
            "leather pu backpack",
            "branded bag",
            "brand bag",
            "new model purses and ladies handbags",
            "laptop bag",
            "laptop backpack",
            "leather backpack",
            "laptop sleeve",
            "briefcase",
            "herschel backpack",
            "leather laptop bag",
            "leather journal",
            "laptop backpack bag",
            "leather wallet",
            "card holder",
            "men's wallet",
            "passport holder",
            "woman wallet",
            "id card holder",
            "secrid wallet",
            "wallet women",
            "ladies wallet ladies pars hand set bag genuine wal",
            "rfid wallet",
            "passport cover",
            "credit card holder",
            "business card holder",
            "wallet men",
            "man leather wallet",
            "leather card holder",
            "belt",
            "man belt",
            "genuine leather belt",
            "leather belts for men",
            "gucci belt",
            "military belt"
        ]
        ali_crawer.craw_rank(keywords=keywords)
        # ali_crawer.craw_keywords_category()
        # products = database.get_products()
        # print(products[0].keywords)
        csv_generator = CSV_Generator(database=database)
        # csv_generator.generate_month_keywords_csv()
        # csv_generator.generate_alibaba_csv()
        csv_generator.generate_overview_csv(keywords=keywords)
    except Exception as e:
        raise
    finally:
        database.close()
