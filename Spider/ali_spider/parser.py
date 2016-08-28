"""解析器
"""

import re
import math
import json
from datetime import datetime
from models import Product, Rank, Keyword
from bs4 import BeautifulSoup

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
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.select('#rank-searech-table > tbody > tr')
    tips = soup.select('.search-result')

    if '查询太频繁，请明日再试！' in str(tips):
        raise RuntimeError('查询太频繁，请明日再试！')

    next_index = _get_next_page(0, index, 1, len(keywords))

    if '无匹配结果' in str(rows):
        return next_index, None

    ranking = list()
    for row in rows:
        product_href = row.select('td:nth-of-type(1) > a')[0].get('href')
        rank_text = row.select('td:nth-of-type(2) > a')[0].text.strip()
        charge_spans = row.select('td:nth-of-type(3) > span')

        product_id = re.findall(r'(?<=id=)\d+', product_href)[0]
        rank = (lambda x: round(float(x[0]) + float(x[1])/100, 2))(
            re.findall(r'(\d+)', rank_text))
        is_selection = True if '搜索首页精选产品' in [x.text for x in charge_spans] else False
        is_p4p = True if 'P4P产品' in [x.text for x in charge_spans] else False
        is_window = True if '橱窗产品' in [x.text for x in charge_spans] else False

        ranking.append({'product_id': product_id,
                        'ranking': rank,
                        'is_selection': is_selection,
                        'is_p4p': is_p4p,
                        'is_window': is_window})

    rank = Rank(keyword=keywords[index], ranking=json.dumps(ranking))
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
        update = datetime.strptime(item['yyyymm']+'03 09:00:00-08:00', '%Y%m%d %H:%M:%S%z')
        is_p4p_keyword = item['isP4pKeyword']
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
                          repeat_keyword=repeat_keyword,
                          is_p4p_keyword=is_p4p_keyword
                         )
        keywords.append(keyword)

    return next_page, keywords

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
