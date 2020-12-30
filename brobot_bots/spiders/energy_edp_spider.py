# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import os
import sys
import uuid

from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script, script_10_sec_wait
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class energy_edp_spider(CustomSpider):
    # required scraper name
    name = "energy_edp"

    # initial urls
    start_url = "https://www.edponline.com.br/para-seu-negocio/login"

    urls = {'cockpit': 'https://www.edponline.com.br/servicos/api/historico-consumo/cockpit',
            'detalhes_da_ultima_conta': 'https://www.edponline.com.br/servicos/api/faturas/detalhes-da-ultima-conta',
            'faturas': 'https://www.edponline.com.br/servicos/api/faturas/lista'}

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(energy_edp_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # internal arguments
        self.navigation_constraints = [
            item['cnpj'] for item in self.navigation_constraints] \
            if self.navigation_constraints else []

    def start_requests(self):
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

    def login_me(self, response):
        RequestVerificationToken = response.selector.xpath(
            "//form[@id='login-form']//input[@name='__RequestVerificationToken']/@value").get("")

        # get the Captcha's options
        sitekey = response.selector.xpath(
            "//div[@class='g-recaptcha']/@data-sitekey").get("")

        gcaptcha_txt = self.solve_captcha(sitekey, response.url)
        if not gcaptcha_txt:
            return

        frm_data = {
            'Empresa': '1',
            'Email': self.e_mail,
            'Senha': self.senha,
            'g-recaptcha-response': gcaptcha_txt,
            '__RequestVerificationToken': RequestVerificationToken}
        print(frm_data)

        yield SplashFormRequest(self.start_url, formdata=frm_data,
                                callback=self.get_main_page,
                                errback=self.errback_func,
                                endpoint='execute',
                                cache_args=['lua_source'],
                                args={'lua_source': script_10_sec_wait}, dont_filter=True)

    def get_main_page(self, response):
        print(response.url)
        cookies = response.data['cookies']

        error_message = response.selector.xpath(
            "//div[@class='message' and contains(text(),'Desculpe! Um erro ocorreu enquanto sua requisição era processada.')]/text()").get("")
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        rows = response.selector.xpath("//div[@id='grid-instalacoes']//table/tbody/tr")
        for row in rows:
            instalacao = row.xpath("./td[1]/a[@class='instalacao']/text()").get("").strip()
            if not self.navigation_constraints or \
                    instalacao in self.navigation_constraints:
                endereco = row.xpath("./td[2]/a[@class='instalacao']/text()").get("").strip()
                bairro = row.xpath("./td[3]/text()").get("").strip()
                municipio = row.xpath("./td[4]/text()").get("").strip()
                contrato = row.xpath("./td[5]/i/@title").get("").strip()
                table_data = {
                    'endereco': endereco,
                    'bairro': bairro,
                    'municipio': municipio}
                self.result.update({instalacao: table_data})
                url = "https://www.edponline.com.br/servicos/selecionar-instalacao/selecionar?instalacao={}".format(instalacao)
                # print(url)
                yield SplashRequest(url, callback=self.call_api_url,
                                    endpoint='execute',
                                    cache_args=['lua_source'],
                                    args={'lua_source': script_10_sec_wait,
                                          'cookies': cookies},
                                    meta={'instalacao': instalacao}, dont_filter=True)

    def call_api_url(self, response):
        # print(response.url)

        for key, url in self.urls.items():
            yield Request(url, callback=self.get_json_response,
                          meta={'instalacao': response.meta['instalacao'],
                                'key': key, 'cookies': response.data['cookies']},
                          cookies=response.data['cookies'], dont_filter=True)

    def get_json_response(self, response):
        key = response.meta['key']
        instalacao = response.meta['instalacao']

        json_response = json.loads(response.text)
        # print(json_response)
        # update values in result
        instalacao_value = self.result[instalacao]
        instalacao_value.update({key: json_response})
        self.result.update({instalacao: instalacao_value})
        if key == 'faturas':
            faturas = json_response['faturas']
            for fatura in faturas[:1]:
                status = fatura['status']
                vencimento_date = fatura['dataDeVencimento']
                vencimento_datetime = dt.strptime(vencimento_date, "%d/%m/%Y")
                if (self.start_date <= vencimento_datetime <= self.end_date) and status != 'Paga':
                    dataKey = fatura['dataKey']
                    pdf_url = 'https://www.edponline.com.br/servicos/api/faturas/download-da-fatura?key={}'.format(dataKey)
                    yield Request(pdf_url, callback=self.save_pdf,
                                  meta={'instalacao': response.meta['instalacao'],
                                        'file_data': fatura},
                                  cookies=response.meta['cookies'], dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        instalacao = response.meta['instalacao']
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
        instalacao_value = self.result[instalacao]
        faturas = instalacao_value['faturas']
        [item.update({
            file_type: {
                "file_id": file_id}
            }) for item in faturas['faturas'] if item == response.meta['file_data']]
        self.result.update({instalacao: instalacao_value})

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
                'utilidades-data_collected.json')
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
