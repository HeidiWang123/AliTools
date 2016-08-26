from spider import ProductSpider
from models import Database

if __name__ == "__main__":
    db = Database()
    product_spider = ProductSpider(db)
    product_spider.craw()
    db.close()
