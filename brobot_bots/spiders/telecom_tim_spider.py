# -*- coding: utf-8 -*-

from datetime import datetime as dt
from http.cookies import SimpleCookie
import json
import os
import re
import sys
import uuid

from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
#del path


class telecom_tim_spider(CustomSpider):
    # required scraper name
    name = "telecom_tim"

    start_url = 'https://meutim.tim.com.br/novo/login'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(telecom_tim_spider, cls).from_crawler(
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
        yield Request(self.start_url, callback=self.login_me,
                      errback=self.errback_func, dont_filter=True)

    def login_me(self, response):
        print('current url:', response.url)
        match = re.search("request_id=(.*?)&", response.url)
        if match:
            request_id = match.group(1)

        frm_data = {
            'type': 'ADM',
            'username': self.login,
            'password': self.senha,
            'request_id': request_id,
            'PersistentLogin': 'true',
            'radio': 'on',
            'campo-login-num-tim': '',
            'campo-login': self.login,
            'senha-login': self.senha,
            'manter-corporativo': ''
        }

        login_url = "https://auth1.tim.com.br/meutim_pwd/oam/server/auth_cred_submit"
        yield FormRequest(login_url, formdata=frm_data,
                          callback=self.get_main_page,
                          errback=self.errback_func, dont_filter=True)

    def get_main_page(self, response):
        print('current url:', response.url)

        error_message = response.selector.xpath(
            "//span[@id='mensagem-erro-login-corporativo']/text()").get("")
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        match = re.search("acc_jwt_token = '(.*?)';", response.text)
        if match:
            acc_jwt_token = match.group(1)

        url = "https://meutim.tim.com.br/api/v1/functionality/8/url"
        headers = {'JWT': acc_jwt_token}
        yield Request(url, headers=headers,
                      callback=self.get_conta_online,
                      errback=self.errback_func, dont_filter=True)

    def get_conta_online(self, response):
        print('current url:', response.url)

        json_response = json.loads(response.text)
        api_url = json_response['url']
        yield Request(api_url, callback=self.get_cnpj_list,
                      errback=self.errback_func, dont_filter=True)

    def get_cnpj_list(self, response):
        cnpj_list = response.selector.xpath("//select[@name='cnpj']/option/text()").extract()
        print(cnpj_list)

        for cnpj in cnpj_list:
            if not self.navigation_constraints or \
                    cnpj in self.navigation_constraints:
                json_data = json.dumps({
                    "customerIdSession": self.login,
                    "consultar": True,
                    "cnpj": cnpj,
                    "codes": [],
                    "dates": [],
                    "numbers": [],
                    "pageSize": 24,
                    "pageNumber": 1,
                    "filterStatus": None,
                    "pageSizecustomer": 24})
                print(json_data)
                url = "https://meutim.tim.com.br/corporate-ecm/web/invoices-all"
                headers = {'Content-Type': 'application/json; charset=UTF-8'}
                yield Request(url, method='POST', headers=headers,
                              body=json_data, meta={'cnpj': cnpj},
                              callback=self.get_minhas_contas, dont_filter=True)

    def get_minhas_contas(self, response):
        cnpj = response.meta['cnpj']

        rows = response.selector.xpath("//div[@class='invoices-list-item-body']")
        table_content = []
        files_list = []
        for row in rows:
            status = row.xpath(".//dl[./dt[contains(text(),'Status')]]/dd/text()").get("").strip()
            cod_cliente = row.xpath(".//dl[./dt[contains(text(),'Cod. cliente')]]/dd/text()").get("").strip()
            vencimento = row.xpath(".//dl[./dt[contains(text(),'Vencimento')]]/dd/text()").get("").strip()
            valor = row.xpath(".//div[@class='row']//h2/text()").get("").strip()
            file_data = {
                'status': status,
                'cod_cliente': cod_cliente,
                'vencimento': vencimento,
                'valor': valor
                }

            vencimento_data = dt.strptime(vencimento, "%d/%m/%Y")
            if self.start_date <= vencimento_data <= self.end_date:
                table_content.append(file_data)
                if self.get_files and status != "Pago":
                    invoiceId = row.xpath(
                        ".//div[@class='row']/input[@name='invoiceId']/@value").get("").strip()
                    customerIdSession = row.xpath(
                        ".//div[@class='row']/input[@name='customerIdSession']/@value").get("").strip()
                    frm_data = {
                        'invoiceId': invoiceId,
                        'customerIdSession': customerIdSession}
                    file_data_copy = file_data
                    file_data_copy.update(frm_data)
                    files_list.append(file_data_copy)
        self.result[cnpj] = {'minhas_contas': table_content}

        for file_data in files_list:
            invoiceId = file_data.pop('invoiceId')
            customerIdSession = file_data.pop('customerIdSession')
            frm_data = {
                'invoiceId': invoiceId,
                'customerIdSession': customerIdSession}
            pdf_url = "https://meutim.tim.com.br/corporate-ecm/web/api/showPdf"
            yield FormRequest(pdf_url, formdata=frm_data,
                              meta={'cnpj': cnpj, 'file_data': file_data},
                              callback=self.save_pdf, dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = response.meta['cnpj']
        file_data = response.meta.get('file_data')
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
        result_value = self.result.get(result_key, {})
        minhas_contas = result_value.get("minhas_contas", [])
        [item.update({
            file_type: {
                "file_id": file_id}
        }) for item in minhas_contas
            if item == file_data]
        self.result.update({result_key: {
            "minhas_contas": minhas_contas}})

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
                '{cnpj}-data_collected.json'.format(cnpj=self.login))
            self.data_collected(self.data, webhook_file_path)
            # return item for scrapinghub
            self.result_received = True
            req = Request(self.start_url.replace('model', 'login'),
                          callback=self.yield_item,
                          errback=self.yield_item, dont_filter=True)
            self.crawler.engine.crawl(req, spider)

    def yield_item(self, response):
        """Function is using to yield Scrapy Item
        Required for us to see the result in ScrapingHub"""
        item = BrobotBotsItem()
        item.update(self.data)
        yield item
