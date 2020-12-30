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


class telecom_suldigital_spider(CustomSpider):
    # required scraper name
    name = "telecom_suldigital"

    # initial urls
    start_url = "https://ixc.suldigital.com.br/central_assinante_web/model"

    urls = {'consumos': {'action': 'getConsumo',
                         'url': '/consumos/consumos.php?'},
            'faturas': {'action': 'getFaturas',
                        'url': '/faturas/faturas.php?'},
            'planos': {'action': 'getPlanos',
                       'url': '/planos/planos.php?'}}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(telecom_suldigital_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        cpf_cnpj_mask = "{}.{}.{}/{}-{}".format(
            self.cpf_cnpj[:2],
            self.cpf_cnpj[2:5],
            self.cpf_cnpj[5:8],
            self.cpf_cnpj[8:-2],
            self.cpf_cnpj[-2:])
        query_str = {
            'ID_CLIENTE': '0',
            'USER': cpf_cnpj_mask,
            'PASSWORD': '',
            'APP': 'N',
            'TOKEN': '',
            'ACTION': 'getValidaLogin',
            'MANTER_CONNECTADO': 'false'}
        start_url = "{main_url}/login/login.php?{url_args}".format(
            main_url=self.start_url, url_args=urllib.parse.urlencode(query_str))
        print(start_url)
        yield Request(start_url, callback=self.get_login_response,
                      errback=self.errback_func, dont_filter=True)

    def get_login_response(self, response):
        json_response = json.loads(response.text)[0]
        status = json_response['tipo']
        message = json_response['mensagem']
        if status == 'sucesso':
            session = message['sessao']
            self.cookies_list = [{'name': 'sessao', 'value': session}]

            session_url = self.start_url + "/login/login.php?ACTION=getValidaSessao"
            print(session_url)
            yield Request(session_url,
                          callback=self.make_api_calls,
                          errback=self.errback_func,
                          cookies=self.cookies_list, dont_filter=True)
        else:
            if "senha incorretos" in message.lower():
                error_msg = {"error_type": "WRONG_CREDENTIALS", "details": message}
            elif "acesso foi bloqueado" in message.lower():
                error_msg = {"error_type": "ACCESS_DENIED", "details": message}
            else:
                error_msg = {"error_type": "UNDEFINED_ERROR", "details": message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

    def make_api_calls(self, response):
        for result_key, data in self.urls.items():
            query_str = {
                'SLICE': '2000000000',
                'ACTION': data['action']}
            service_url = "{main_url}{url_path}{url_args}".format(
                main_url=self.start_url,
                url_path=data['url'],
                url_args=urllib.parse.urlencode(query_str))
            if result_key != 'consumos':
                service_url += '&HOME=true'
            yield Request(service_url,
                          callback=self.get_json_response,
                          meta={'result_key': result_key},
                          cookies=self.cookies_list, dont_filter=True)

    def get_json_response(self, response):
        result_key = response.meta['result_key']

        json_response = json.loads(response.text)
        if result_key == 'faturas':
            json_response.pop('notifi_dash')
            self.result.update(
                {result_key: json_response})

            if self.get_files:
                query_str = {
                    'SLICE': '5',
                    'HOME': 'false',
                    'ACTION': 'getFaturas'}
                url = "{main_url}{url_path}{url_args}".format(
                    main_url=self.start_url,
                    url_path=self.urls['faturas']['url'],
                    url_args=urllib.parse.urlencode(query_str))
                yield Request(url, callback=self.get_faturas,
                              errback=self.errback_func,
                              cookies=self.cookies_list, dont_filter=True)
        else:
            self.result.update(
                {result_key: json_response})

    def get_faturas(self, response):
        json_response = json.loads(response.text)
        faturas = json_response['faturas']
        for fatura in faturas:
            document_id = fatura['id']
            data_vencimento = fatura['data_vencimento_formatada']
            vencimento_datetime = dt.strptime(data_vencimento, "%d/%m/%Y")
            if self.start_date <= vencimento_datetime <= self.end_date:
                query_str = {
                    'ID_RECEBER': document_id,
                    'APP': 'N',
                    'ACTION': 'getBoletoArquivo'}
                url = "{main_url}{url_path}{url_args}".format(
                    main_url=self.start_url,
                    url_path=self.urls['faturas']['url'],
                    url_args=urllib.parse.urlencode(query_str))
                yield Request(url, callback=self.save_pdf,
                              meta={'file_data': fatura},
                              cookies=self.cookies_list, dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get pdf body
        json_response = json.loads(response.text)[0]
        tipo = json_response['tipo']
        if tipo != "erro":
            pdf_data = base64.b64decode(json_response['mensagem']['base_pdf'])

            # get metadata
            result_key = "faturas"
            file_type = "__boleto__"
            file_data = response.meta['file_data']

            # options to save pdf
            file_id = str(uuid.uuid4())
            filename = "{file_id}.pdf".format(file_id=file_id)
            file_path = os.path.join(path, "downloads", self.scrape_id, filename)
            with open(file_path, 'wb') as f:
                f.write(pdf_data)

            # upload pdf to s3 and call the webhook
            self.upload_file(file_id)

            # update values in result
            result_value = self.result.get(result_key)
            faturas = result_value.get(result_key, [])
            [item.update({
                file_type: {
                    "file_id": file_id}
                }) for item in faturas if item == file_data]
            result_value.update({result_key: faturas})
            self.result.update({result_key: result_value})
        else:
            error_details = json_response['mensagem']
            error_msg = {"error_type": "FILE_NOT_SAVED",
                         "file": filename, "details": error_details}
            self.errors.append(error_msg)
            self.logger.error(error_msg)

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
                '{cnpj}-data_collected.json'.format(
                    cnpj=self.cpf_cnpj))
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
