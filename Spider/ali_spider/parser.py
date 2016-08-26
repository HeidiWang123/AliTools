import math
import datetime
from models import Product

def parse_product(response):
    json = response.json()
    json_products = json['products']
    products = list()
    for item in json_products:
        product = _product_from_json(item)
        products.append(product)

    current_page = json['currentPage']
    page_size = len(json_products)
    products_count = json['count']
    new_page = _get_next_page(current_page, page_size, products_count)

    return new_page, products

def _product_from_json(json_item):
    product = Product(id=json_item.get('id', None),
                      style_no=json_item.get('redModel', None),
                      title=json_item.get('subject', None),
                      keywords=json_item.get('keywords', None),
                      owner=json_item.get('ownerMemberName', None),
                      modify_time=datetime.datetime.fromtimestamp(json_item.get('modifyTime', None)/1000),
                      is_trade_product=json_item.get('mappedToYdtProduct', None),
                      is_window_product=json_item.get('isWindowProduct', None)
                     )

    return product

def _get_next_page(page, size, count):
    next_page = None

    page_count = math.ceil(count/size)
    if page_count > page + 1:
        next_page = page + 1

    return next_page

def parse_keyword(response):
    pass

def parse_rank(response):
    pass
