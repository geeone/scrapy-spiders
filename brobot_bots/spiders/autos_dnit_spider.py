# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import os
import sys
import re

from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_dnit_spider(CustomSpider):
    # required scraper name
    name = "autos_dnit"

    # initial urls
    start_url = "https://servicos.dnit.gov.br/multas"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_dnit_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        url = self.start_url + '/assets/js/app.472c78924e851b60221f.js'
        yield Request(url, callback=self.get_login_page,
                      errback=self.errback_func, dont_filter=True)

    def solve_captcha(self, sitekey, captcha_url, **kwargs):
        try:
            # SOLVE RECAPTCHA
            attempts = 0
            while 1:
                # check attempts count to avoid cycled solving
                if attempts < self.captcha_retries:
                    attempts += 1
                    self.g_recaptcha_id, gcaptcha_txt = self.captcha_solver(
                        self.captcha_service,
                        sitekey=sitekey,
                        captcha_url=captcha_url,
                        captcha_type=kwargs.get('captcha_type', 4),
                        captcha_action=kwargs.get('captcha_action'))
                    if gcaptcha_txt:
                        print("ReCaptcha:", gcaptcha_txt)
                        return gcaptcha_txt
                else:
                    break
            # check if captcha was solved
            if not gcaptcha_txt:
                details_msg = "Failed to solve captcha for {} times.".format(self.captcha_retries)
                error_msg = {"error_type": "CAPTCHA_NOT_SOLVED",
                             "captcha_service": self.captcha_service, "details": details_msg}
                raise Exception(error_msg)
        except Exception as exc:
            error_msg = exc.args[0]
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return None

    def get_login_page(self, response):
        """Function to get request options to login.
        Used to get ReCaptcha token; image captcha value."""

        # solve the ReCaptcha
        sitekey = re.search('sitekey:\"(.*?)\"', response.text)
        if sitekey:
            sitekey = sitekey.group(1)

        gcaptcha_txt = self.solve_captcha(sitekey, response.url)
        if not gcaptcha_txt:
            return

        headers = {
            'Content-Type': 'application/json',
        }

        payload = {
            "placa": self.placa,
            "renavam": self.renavam,
            "ResponseCode": gcaptcha_txt
        }
        url = 'https://servicos.dnit.gov.br/multas/api/Auth/Renavam'

        yield Request(url=url, method="POST", headers=headers, callback=self.veiculo,
                      body=json.dumps(payload), errback=self.errback_func, dont_filter=True)

    def veiculo(self, response):
        self.result['dadosveiculo'] = json.loads(response.text)

        headers = {
            "Authorization": "Bearer " + json.loads(response.text)['token']
        }
        url = 'https://servicos.dnit.gov.br/multas/api/Infracao?skip=0&pageSize=1000&ordenacaoCrescente=true'
        yield Request(url=url, callback=self.debito, headers=headers,
                      errback=self.errback_func, dont_filter=True)

    def debito(self, response):
        # create screenshot using imgkit
        if self.capture_screenshot:
            self.take_screenshot(response)

        self.result['infracoes'] = json.loads(response.text)['infracoes']

    def get_final_result(self, spider):
        """Will be called before spider closed
        Used to save data_collected result
        Required to return ScrapingHub item."""

        # stop crawling after yeild_item called
        if not self.result_received:
            # push to webhook
            if self.screenshots_ids:
                self.result['__screenshots_ids__'] = self.screenshots_ids
            self.data = {
                'scrape_id': self.scrape_id,
                'scraper_name': self.name,
                'files_count': self.files_count,
                'screenshots_count': self.screenshots_count,
                'cnpj': self.cnpj}
            self.data.update({'result': self.result})
            if self.errors:
                self.data.update({'errors': self.unique_list(self.errors)})
            webhook_file_path = os.path.join(
                path, "downloads", self.scrape_id, '{renavam}-data_collected.json'.format(
                    renavam=self.renavam))
            self.data_collected(self.data, webhook_file_path)
            # return item for scrapinghub
            self.result_received = True
            req = Request(self.start_url, callback=self.yield_item,
                          errback=self.yield_item, dont_filter=True)
            self.crawler.engine.crawl(req, spider)

    def yield_item(self, response):
        """Function is using to yield Scrapy Item
        Required for us to see the result in ScrapingHub"""
        item = BrobotBotsItem()
        item.update(self.data)
        yield item
