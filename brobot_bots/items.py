# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BrobotBotsItem(scrapy.Item):
    scraper_name = scrapy.Field()
    scrape_id = scrapy.Field()
    files_count = scrapy.Field()
    screenshots_count = scrapy.Field()
    cnpj = scrapy.Field()
    result = scrapy.Field()
    errors = scrapy.Field()
    renavam = scrapy.Field()
    cnpj = scrapy.Field()
