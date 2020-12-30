# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import uuid

from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest
from websocket import create_connection

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script, script_10_sec_wait
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
#del path


class telecom_vivo_movel_spider(CustomSpider):
    # required scraper name
    name = "telecom_vivo_movel"

    start_url = 'https://mve.vivo.com.br'

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(telecom_vivo_movel_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # internal arguments
        self.navigation_constraints = [
            item['cnpj'] for item in self.navigation_constraints] \
            if self.navigation_constraints else []

    def start_requests(self):
        frm_data = {"email": self.e_mail,
                    "password": self.senha}

        login_url = self.start_url + '/login/sign_in'
        yield SplashFormRequest(login_url, formdata=frm_data,
                                callback=self.sign_in_me,
                                errback=self.errback_func,
                                endpoint='execute',
                                cache_args=['lua_source'],
                                args={'lua_source': script_10_sec_wait},
                                dont_filter=True)

    def sign_in_me(self, response):
        error_message = response.selector.xpath(
            "//span[contains(normalize-space(),'E-mail e/ou senha incorretos')]/text()").get()
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        dashboard_url = "https://mve.vivo.com.br/dashboard"
        yield Request(dashboard_url, callback=self.get_dashbord,
                      cookies=response.data['cookies'],
                      meta={'cookies': response.data['cookies']},
                      dont_filter=True)

    def get_dashbord(self, response):
        print(response.url)

        documents = response.selector.xpath("//li[@data-documentnumber]/@data-documentnumber").extract()
        for document in documents:
            if not self.navigation_constraints or \
                    document in self.navigation_constraints:
                current_time = str(time.time() * 1000)
                query_str = {
                    'documentNumber': document,
                    'customerPlatformType': 'mobile',
                    'offset': '0',
                    'limit': '20',
                    'ts': current_time}

                invoices_url = "https://mve.vivo.com.br/module/invoices/list/summary?" + urllib.parse.urlencode(query_str)
                yield Request(invoices_url, headers={'x-document-number': document},
                              callback=self.get_invoices,
                              cookies=response.meta['cookies'],
                              meta={'cookies': response.meta['cookies'],
                                    'document': document},
                              dont_filter=True)

    def get_invoices(self, response):
        cookies = response.meta['cookies']
        document = response.meta['document']
        cookie_string = "; ".join(["{}={}".format(cookie['name'], cookie['value']) for cookie in cookies])

        json_response = json.loads(response.text)
        print(json_response)
        # add to result if not empty
        if json_response['due'] or json_response['paid'] or \
                json_response['in_arrears'] or json_response['inactive']:
            self.result[document] = json_response

        if self.get_files:
            for invoice_status in ['due', 'in_arrears']:
                due_invoices = json_response[invoice_status]
                for invoice in due_invoices:
                    billing_info = invoice['invoice']
                    current_time = str(time.time() * 1000)
                    due_date = dt.strptime(billing_info['paymentDueDate'], "%Y-%m-%d")
                    account_status = invoice['billingAccountStatus']
                    if account_status != "cancelled" and self.start_date <= due_date <= self.end_date:
                        bills = [{
                            "billMonth": invoice['billMonth'],
                            "billYear": invoice['billYear'],
                            "billingAccountId": invoice['billingAccountId'],
                            "cycleCode": invoice['cycleCode'],
                            "dateFinalCycle": invoice['dateFinalCycle'],
                            "documentId": document, # document number
                            "originalInvoiceRefDueDate": billing_info['paymentDueDate'],
                            "paymentStatus": [billing_info['paymentStatus']],
                            "type": "detailed-invoice"}]

                        file_data = {
                            "filename": document, # document number
                            "info": "{account_id}\n              - {due_date}\n              - Detalhada (.pdf)".format(
                                account_id=invoice['billingAccountId'], due_date=due_date.strftime("%d/%m/%Y")),
                            "id": document + current_time,
                            "documentNumber": document, # document number
                            "type": "detailed-invoice",
                            "fileFormat": "detailed-invoice",
                            "requestFrom": "grid-invoices",
                            "paymentStatusAnalytics": [billing_info['paymentStatus']],
                            "downloadId": document + current_time,
                            "status": 2,
                            "bills": bills,
                            "download": bills}

                        ws = create_connection("wss://mve.vivo.com.br/wss/file",
                                               cookie=cookie_string)
                        ws.send(json.dumps(file_data))
                        ws_result = json.loads(ws.recv())
                        ws.close()

                        if ws_result.get('url'):
                            pdf_url = "https://mve.vivo.com.br{url_path}&vivo_download_token={download_token}".format(
                                url_path=ws_result['url'], download_token=ws_result['downloadToken'])
                            print(pdf_url)
                            yield Request(pdf_url, callback=self.save_pdf,
                                          meta={'document': document,
                                                'invoice_status': invoice_status,
                                                'invoice': invoice},
                                          cookies=cookies, dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        if response.status != 200 and self.file_retries > 0:
            self.file_retries -= 1
            yield response.request.replace(dont_filter=True)
            return
        elif response.status != 200:
            return
        else:
            # refresh
            self.file_retries = 3

        # get metadata
        file_type = "__boleto__"
        invoice_status = response.meta['invoice_status']
        document = response.meta['document']

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        with open(file_path, 'wb') as f:
            f.write(response.body)

        # upload pdf to s3 and call the webhook
        self.upload_file(file_id)

        # update values in result
        document_value = self.result[document]
        [item.update({
            file_type: {
                "file_id": file_id}
        }) for item in document_value[invoice_status]
            if item == response.meta['invoice']]
        self.result.update({document: document_value})

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
                path, "downloads", self.scrape_id,
                '{cnpj}-data_collected.json'.format(cnpj=self.cpf_cnpj))
            self.data_collected(self.data, webhook_file_path)
            # return item for scrapinghub
            self.result_received = True
            req = Request(self.start_url,
                          callback=self.yield_item,
                          errback=self.yield_item, dont_filter=True)
            self.crawler.engine.crawl(req, spider)

    def yield_item(self, response):
        """Function is using to yield Scrapy Item
        Required for us to see the result in ScrapingHub"""
        item = BrobotBotsItem()
        item.update(self.data)
        yield item
