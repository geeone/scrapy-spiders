# -*- coding: utf-8 -*-

import base64
from datetime import datetime as dt
import json
import os
import sys
import urllib.parse
import uuid

from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class telecom_algar_spider(CustomSpider):
    # required scraper name
    name = "telecom_algar"

    # initial urls
    start_url = "https://algartelecom.com.br/AreaClienteCorporativo/login"

    urls = {'customers': 'https://api-portal.algartelecom.com.br/portal/v1/customers/{customer_key}',
            'bills': 'https://api-portal.algartelecom.com.br/portal/v1/customers/{customer_key}/bills'}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(telecom_algar_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        yield Request(self.start_url, callback=self.login_me,
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
                        captcha_type=kwargs.get('captcha_type', 5),
                        captcha_action=kwargs.get('captcha_action', 'leads'))
                    if gcaptcha_txt:
                        print("ReCaptcha:", gcaptcha_txt)
                        return gcaptcha_txt
                else:
                    break
            # check if captcha was solved
            if not gcaptcha_txt:
                details_msg = "Failed to solve captcha for {} times.".format(self.captcha_retries)
                error_msg = {"error_type": "CAPTCHA_NOT_SOLVED",
                             "captcha_service": self.captcha_service,
                             "details": details_msg}
                raise Exception(error_msg)
        except Exception as exc:
            error_msg = exc.args[0]
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return None

    def login_me(self, response):
        """Function to get request options to login.
        Used to get ReCaptcha token; image captcha value."""

        # get the Captcha's options
        sitekey = "6Lc9CtEUAAAAAMHmDFYiQ45-jY-5ox9HIq0B0pEM"

        gcaptcha_txt = self.solve_captcha(sitekey, self.start_url)
        if not gcaptcha_txt:
            return

        frm_data = {
            'user': self.usuario,
            'password': self.senha,
            'reCaptchaHash': gcaptcha_txt}
        frm_json_data = json.dumps(frm_data)

        login_api_url = "https://api-portal.algartelecom.com.br/portal/v1/login"
        yield Request(login_api_url, method='POST', body=frm_json_data,
                      callback=self.login_api_response,
                      errback=self.errback_func,
                      meta={"handle_httpstatus_list": [400, 401, 404]}, dont_filter=True)

    def login_api_response(self, response):
        json_response = json.loads(response.text)
        if json_response['resultCode'] == 'error':
            error_message = json_response['resultMessage']
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        result_data = json_response['resultData']
        self.customer_key = result_data['customerKey']
        x_authorization = response.headers['X-Authorization'].decode('ascii')

        self.headers = {
            'documentNumber': result_data['documentNumber'],
            'clientStatus': result_data['clientStatus'],
            'user': result_data['user'],
            'clientId': result_data['clientId'],
            'userName': result_data['user'],
            'x-authorization': x_authorization,
            'customerKey': result_data['customerKey'],
            'authDate': result_data['authDate']}

        # go to URLS
        for key, url in self.urls.items():
            url = url.format(
                customer_key=self.customer_key)
            yield Request(url, headers=self.headers,
                          meta={'result_key': key},
                          callback=self.customers_and_bills_api_response,
                          errback=self.errback_func,
                          dont_filter=True)

    def customers_and_bills_api_response(self, response):
        result_key = response.meta['result_key']
        result_data = json.loads(response.text)['resultData']
        if result_data:
            self.result[result_key] = result_data

        if result_key == 'bills':
            for doc in result_data:
                query_str = {
                    'agreementId': doc['agreementId'],
                    'targetSystem': doc['targetSystem'],
                    'billingSystem': doc['billingSystem'],
                    'originSystem': 'PORTAL_B2B'}
                invoices_url = "https://api-portal.algartelecom.com.br/portal/v1/customers/{customer_key}/bills/{accountNumber}/invoices?".format(
                    customer_key=self.customer_key, accountNumber=doc['accountNumber'])
                invoices_url += urllib.parse.urlencode(query_str)
                yield Request(invoices_url, headers=self.headers,
                              meta={'accountNumber': doc['accountNumber'],
                                    'query_str': query_str},
                              callback=self.invoinces_api_response, dont_filter=True)

    def invoinces_api_response(self, response):
        accountNumber = response.meta['accountNumber']
        query_str = response.meta['query_str']
        query_str.update({"fileType": "DUPLICATE"})

        result_data = json.loads(response.text)['resultData']
        invoince_data = [item for item in result_data if self.sort_date(item['dueDate'])]
        if invoince_data:
            self.result[accountNumber] = invoince_data

        if self.get_files:
            files_data = [item for item in invoince_data if item['status'] != 'CLOSED']
            for invoice in files_data:
                invoice_id = base64.b64encode(invoice['id'].encode("ascii")).decode("ascii")
                files_url = "https://api-portal.algartelecom.com.br/portal/v1/customers/{customer_key}/bills/{accountNumber}/invoices/{invoice_id}/files?".format(
                    customer_key=self.customer_key, accountNumber=accountNumber, invoice_id=invoice_id)
                files_url += urllib.parse.urlencode(query_str)
                yield Request(files_url, headers=self.headers,
                              meta={'accountNumber': response.meta['accountNumber'],
                                    'file_data': invoice},
                              callback=self.files_api_response, dont_filter=True)

    def files_api_response(self, response):
        file_url = json.loads(response.text)['resultData']['pathURL']
        yield Request(file_url, headers=self.headers,
                      meta={'accountNumber': response.meta['accountNumber'],
                            'file_data': response.meta['file_data']},
                      callback=self.save_pdf, dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = response.meta['accountNumber']
        # hardcoded in this case since we don't have another types
        file_type = "__boleto__"

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        with open(file_path, 'wb') as f:
            f.write(response.body)

        # upload pdf to s3 and call the webhook
        self.upload_file(file_id)

        # update values in result
        result_value = self.result.get(result_key, [])
        [item.update({
            file_type: {
                "file_id": file_id}
            }) for item in result_value if item == response.meta['file_data']]
        self.result.update({result_key: result_value})

    def sort_date(self, due_date):
        due_datetime = dt.strptime(due_date, "%d/%m/%Y")
        if self.start_date <= due_datetime <= self.end_date:
            return 1
        return

    def get_final_result(self, spider):
        """Will be called before spider closed
        Used to save data_collected result."""

        # stop crawling after yeild_item called
        if not self.result_received:
            # push to webhook
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
                path, "downloads", self.scrape_id, '{usuario}-data_collected.json'.format(
                    usuario=self.usuario))
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
