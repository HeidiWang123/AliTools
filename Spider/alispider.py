#!/usr/bin/env python
from api import Spider

if __name__ == '__main__':
    spider = Spider()

    # with open('config/keywords.txt') as kf:
    #     for line in kf:
    #         spider.generate_keywords(line.strip())

    with open('config/rank.txt') as kf:
        spider.generate_rank(kf)
