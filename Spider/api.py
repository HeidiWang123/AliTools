{
    'rank': {
        'top1_product_id'      : str,
        'top1_product_position': float,
        'product_position'     : float,
        'is_selection_poduct'  : bool
    },
    'product': {
        'keyword'           : list,
        'product_owner'     : str,
        'product_no'        : str,
        'product_id'        : str,
        'is_window_product' : bool,
        'is_ydt_product'    : bool
    },
    'keyword': {
        "keyword_company_count"         : int,
        "keyword_window_products_count" : int,
        "keyword_pv_rank"               : int,
        "is_p4p_keyword"                : bool
    }
}

class Rank(object):
    """docstring for Rank"""
    def __init__(self, arg):
        super(Rank, self).__init__()
        self.arg = arg
