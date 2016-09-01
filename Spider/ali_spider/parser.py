"""解析器
"""

import math
import json
from datetime import datetime
from models import Product, Rank, Keyword, P4P

def parse_product(response, page_size):
    """产品结果解析器
    """
    resp_json = response.json()
    json_products = resp_json['products']
    products = list()
    for item in json_products:
        product = _product_from_json(item)
        products.append(product)

    current_page = resp_json['currentPage']
    products_count = resp_json['count']
    new_page = _get_next_page(1, current_page, page_size, products_count)

    return new_page, products

def parse_rank(response, index, keywords):
    """rank 结果解析器
    """
    keyword = keywords[index]
    rank = Rank(keyword=keyword)

    json_ranks = response.json()['value']
    if json_ranks is not None and len(json_ranks) > 0:
        ranking_list = list()
        for item in json_ranks:
            ranking_list.append({
                'product_id': item['id'],
                'ranking': item['pageNO'] + item['rowNO']/100,
            })
        rank.ranking = json.dumps(ranking_list)

    next_index = _get_next_page(0, index, 1, len(keywords))
    return next_index, rank

def parse_keyword(response, page, page_size, negative_keywords):
    """keyword 解析器
    """

    resp_json = response.json()
    resp_keywords = resp_json['value']['data']

    if not resp_json['successed'] or len(resp_keywords) == 0:
        return None, None

    resp_total = resp_json['value']['total']
    next_page = None if page >= 500 else _get_next_page(1, page, page_size, resp_total)

    keywords = list()
    for item in resp_keywords:
        value = item['keywords']
        if value in negative_keywords:
            continue
        value = item['keywords']
        repeat_keyword = item.get('repeatKeyword', None)
        company_cnt = item['company_cnt']
        showwin_cnt = item['showwin_cnt']
        update = datetime.strptime(item['yyyymm']+'03 09:00:00-0800', '%Y%m%d %H:%M:%S%z')
        is_p4p_keyword = item.get('isP4pKeyword', None)
        if is_p4p_keyword is None:
            # 如果为空则重试
            return page, None
        srh_pv = json.dumps({'srh_pv_this_mon': item['srh_pv_this_mon'],
                             'srh_pv_last_1mon': item['srh_pv_last_1mon'],
                             'srh_pv_last_2mon': item['srh_pv_last_2mon'],
                             'srh_pv_last_3mon': item['srh_pv_last_3mon'],
                             'srh_pv_last_4mon': item['srh_pv_last_4mon'],
                             'srh_pv_last_5mon': item['srh_pv_last_5mon'],
                             'srh_pv_last_6mon': item['srh_pv_last_6mon'],
                             'srh_pv_last_7mon': item['srh_pv_last_7mon'],
                             'srh_pv_last_8mon': item['srh_pv_last_8mon'],
                             'srh_pv_last_9mon': item['srh_pv_last_9mon'],
                             'srh_pv_last_10mon': item['srh_pv_last_10mon'],
                             'srh_pv_last_11mon': item['srh_pv_last_11mon'],
                            })
        keyword = Keyword(value=value,
                          srh_pv=srh_pv,
                          update=update,
                          company_cnt=company_cnt,
                          showwin_cnt=showwin_cnt,
                          repeat_keyword=repeat_keyword,
                          is_p4p_keyword=is_p4p_keyword
                         )
        keywords.append(keyword)

    return next_page, keywords

def parse_p4p(response):
    resp_json = response.json()

    total_page = resp_json['totalPage']
    current_page = resp_json['currentPage']
    next_page = current_page + 1 if current_page < total_page else None

    p4ps = list()
    data = resp_json['data']
    for item in data:
        p4ps.append(P4P(
            keyword=item['keyword'],
            qs_star=item['qsStar'],
            is_start=(item['state']=="1"),
            tag=str(item['tag']),
        ))
    return next_page, p4ps

def _product_from_json(json_item):
    """将 json 拼装成产品对象并返回
    """

    timestamp = int(json_item.get('modifyTime', None)) / 1000
    product = Product(id=json_item.get('id', None),
                      style_no=json_item.get('redModel', None),
                      title=json_item.get('subject', None),
                      keywords=json_item.get('keywords', None),
                      owner=json_item.get('ownerMemberName', None),
                      modify_time=datetime.fromtimestamp(timestamp),
                      is_trade_product=json_item.get('mappedToYdtProduct', None),
                      is_window_product=json_item.get('isWindowProduct', None)
                     )

    return product

def _get_next_page(start, page, size, count):
    """根据当前页数、总页数和每页个数返回下一页页数。如果页数不存在，则返回空
    """
    next_page = None

    page_count = math.ceil(count/size)
    if page_count + start - 1 > page:
        next_page = page + 1

    return next_page

class OverRequestCountError(Exception):
    def __init__(self, value):
        super(OverRequestCountError, self).__init__(value)
