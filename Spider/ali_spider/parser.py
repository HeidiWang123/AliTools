"""解析器
"""

import math
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from html.parser import HTMLParser
from models import Product, Rank, Keyword, P4P

def parse_product(response, page_size):
    """产品结果解析器
    """
    resp_json = response.json()
    json_products = resp_json['products']
    products = list()
    for item in json_products:
        product = Product(
            id=item.get('id'),
            style_no=item.get('redModel'),
            title=item.get('subject'),
            keywords=[x.strip() for x in item.get('keywords').split(',')],
            owner=item.get('ownerMemberName'),
            modify_time=datetime.fromtimestamp(int(item.get('modifyTime')) / 1000),
            is_trade_product=item.get('mappedToYdtProduct'),
            is_window_product=item.get('isWindowProduct')
         )

        products.append(product)

    current_page = resp_json['currentPage']
    products_count = resp_json['count']
    new_page = _get_next_page(1, current_page, page_size, products_count)

    return new_page, products

def parse_keyword(response, page, page_size):
    """keyword 解析器"""

    resp_json = response.json()
    resp_keywords = resp_json['value']['data']

    if not resp_json['successed'] or len(resp_keywords) == 0:
        return None, None

    resp_total = resp_json['value']['total']
    next_page = None if page >= 500 else _get_next_page(1, page, page_size, resp_total)

    keywords = list()
    for item in resp_keywords:
        keyword = Keyword(
            value=item['keywords'],
            company_cnt=item['company_cnt'],
            showwin_cnt=item['showwin_cnt'],
            repeat_keyword=item.get('repeatKeyword', None),
            is_p4p_keyword=item['isP4pKeyword'],
            update=datetime.strptime(item['yyyymm']+'0309', '%Y%m%d%H') + relativedelta(months=+1),
            srh_pv={
                'srh_pv_this_mon': item['srh_pv_this_mon'],
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
            },
        )
        keywords.append(keyword)

    return next_page, keywords

def parse_category(response, index):
    """category 结果解析器
    """
    try:
        content_categories = response.json()['categories']
        category = [x['enName'] for x in content_categories]
        return index+1, category
    except KeyError:
        raise ParseError("数据解析错误")

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
        rank.ranking = ranking_list
        rank.update = date.today()

    next_index = _get_next_page(0, index, 1, len(keywords))
    return next_index, rank

def parse_p4p(response):
    resp_json = response.json()

    total_page = resp_json['totalPage']
    current_page = resp_json['currentPage']
    next_page = current_page + 1 if current_page < total_page else None

    p4ps = list()
    data = resp_json['data']
    parser = HTMLParser()
    for item in data:
        p4ps.append(P4P(
            keyword=parser.unescape(item['keyword']),
            qs_star=item['qsStar'],
            is_start=(item['state']=="1"),
            tag=item['tag'],
        ))
    return next_page, p4ps

def _get_next_page(start, page, size, count):
    """根据当前页数、总页数和每页个数返回下一页页数。如果页数不存在，则返回空
    """
    next_page = None

    page_count = math.ceil(count/size)
    if page_count + start - 1 > page:
        next_page = page + 1

    return next_page

class ParseError(Exception):
    def __init__(self, value):
        super(ParseError, self).__init__(value)
