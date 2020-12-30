# -*- coding: utf-8 -*-

import os
import sys
import uuid
import re

from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_pge_sp_spider(CustomSpider):
    # required scraper name
    name = "autos_pge_sp"

    # initial urls
    start_url = "https://www.dividaativa.pge.sp.gov.br/sc/pages/pagamento/gareLiquidacao.jsf"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_pge_sp_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # internal variables
        self.detalhamento_do_debito = []
        self.files_request_params = []

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
        """Function to get request options to login.
        Used to get ReCaptcha token; image captcha value."""

        if not response.meta.get('login_form'):
            # get the Captcha's options
            #sitekey = response.selector.xpath(
            #    "//div[@class='g-recaptcha']/@data-sitekey").get("")

            #gcaptcha_txt = self.solve_captcha(sitekey, response.url)
            #if not gcaptcha_txt:
            #    return

            viewstate_value = response.selector.xpath(
                "//form[@id='menu']//input[@id='javax.faces.ViewState']/@value").get("")
            renavam_name = response.selector.xpath(
                "//input[@value='RENAVAM']/@name").get("")
            consultar_name = response.selector.xpath(
                "//input[@value='Consultar']/@name").get("")

            login_form = {
                'adesaoForm': 'adesaoForm',
                renavam_name: 'RENAVAM',
                'adesaoForm:renavam': self.renavam,
                consultar_name: 'Consultar',
                #'g-recaptcha-response': gcaptcha_txt,
                'javax.faces.ViewState': viewstate_value}

            frm_data = {
                'AJAXREQUEST': '_viewRoot',
                'adesaoForm': 'adesaoForm',
                'adesaoForm:cdaEtiqueta': '',
                'g-recaptcha-response': '',
                'javax.faces.ViewState': viewstate_value,
                renavam_name: 'RENAVAM',
                'ajaxSingle': 'adesaoForm:j_id133',
                'adesaoForm:j_id137': 'adesaoForm:j_id137'
            }

            yield FormRequest(self.start_url, formdata=frm_data,
                              meta={'login_form': login_form},
                              callback=self.login_me, dont_filter=True)
        else:
            yield FormRequest(self.start_url, formdata=response.meta['login_form'],
                              callback=self.get_table,
                              errback=self.errback_func, dont_filter=True)

    def get_table(self, response):
        # create screenshot using imgkit
        if self.capture_screenshot:
            html_text = self.decode_response_to_utf8(response, encoding='utf-8')
            html_text = re.sub(
                '(<input id=\"adesaoForm:renavam\")', r'\1 placeholder="{renavam}"'.format(
                    renavam=self.renavam), html_text)
            self.take_screenshot(response, html_text=html_text)

        if "param" in response.url:
            ViewState = response.selector.xpath(
                "//form[@id='gareForm']//input[@id='javax.faces.ViewState']/@value").get("")
            gareForm = response.selector.xpath(
                "//input[@name='gareForm']/@value").get("")
            rows = response.selector.xpath("//table[@id='gareForm:dataTable']/tbody/tr")
            for row in rows:
                form_id = row.xpath("./td[./a]/@id").get("")
                form_id = form_id[:-1] + str((int(form_id[-1]) + 1))
                frm_data = {
                    'gareForm': gareForm,
                    'javax.faces.ViewState': ViewState,
                    form_id: form_id}
                self.files_request_params.append(frm_data)
            if self.files_request_params:
                yield from self.download_trigger()
        else:
            status_msg = response.selector.xpath(
                "//td[@class='messages-info']//span[contains(@class,'messages-info-label')]/text()").get("").strip()
            self.result.update({
                'renavam': self.renavam,
                'status': status_msg})

    def download_trigger(self):
        form_url = "https://www.dividaativa.pge.sp.gov.br/sc/pages/pagamento/gareLiquidacao-pages/gareLiquidacaoLista.jsf"
        frm_data = self.files_request_params.pop()
        yield FormRequest(form_url, formdata=frm_data,
                          callback=self.get_cda_form,
                          errback=self.errback_func, dont_filter=True)

    def get_cda_form(self, response):
        devedor = response.selector.xpath(
            "//tr[.//label[contains(text(),'Devedor')]]/td[2]/text()").get("").strip()
        cnpj_cpf = response.selector.xpath(
            "//tr[.//label[contains(text(),'CNPJ/CPF')]]/td[2]/text()").get("").strip()
        cda = response.selector.xpath(
            "//tr[.//label[contains(text(),'CDA')]]/td[2]/text()").get("").strip()
        tipo_de_debito = response.selector.xpath(
            "//tr[.//label[contains(text(),'Tipo de Débito')]]/td[2]/text()").get("").strip()
        saldo = response.selector.xpath(
            "//tr[.//label[contains(text(),'Saldo')]]/td[2]/text()").get("").strip()
        file_data = {
            'devedor': devedor,
            'cnpj_cpf': cnpj_cpf,
            'cda': cda,
            'tipo_de_debito': tipo_de_debito,
            'saldo': saldo}

        if self.get_files:
            btn_name = response.selector.xpath(
                "//input[@value='Gerar GARE de Liquidação']/@name").get("")
            ViewState = response.selector.xpath(
                "//form[@id='gareForm']//input[@id='javax.faces.ViewState']/@value").get("")
            gareForm = response.selector.xpath(
                "//input[@name='gareForm']/@value").get("")
            frm_data = {
                'gareForm': gareForm,
                'javax.faces.ViewState': ViewState,
                btn_name: 'Gerar GARE de Liquidação'}
            file_url = "https://www.dividaativa.pge.sp.gov.br/sc/pages/pagamento/gareLiquidacao-pages/gareLiquidacaoDetalhe.jsf"
            yield FormRequest(file_url, formdata=frm_data,
                              meta={'file_data': file_data},
                              callback=self.save_pdf, dont_filter=True)
        else:
            self.detalhamento_do_debito.append(file_data)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get variables from metadata
        file_data = response.meta['file_data']

        # hardcoded in this case since we don't have another types
        file_type = "__boleto__"

        try:
            # options to save pdf
            file_id = str(uuid.uuid4())
            filename = "{file_id}.pdf".format(file_id=file_id)
            file_path = os.path.join(path, "downloads", self.scrape_id, filename)
            with open(file_path, 'wb') as f:
                f.write(response.body)

            # upload pdf to s3 and call the webhook
            self.upload_file(file_id)

            # update values in file_data; then add to detalhamento_do_debito
            file_data.update({
                file_type: {
                    "file_id": file_id}
                })
        except:
            pass
        finally:
            self.detalhamento_do_debito.append(file_data)
            if self.files_request_params:
                yield from self.download_trigger()

    def get_final_result(self, spider):
        """Will be called before spider closed
        Used to save data_collected result."""

        # stop crawling after yeild_item called
        if not self.result_received:
            # push to webhook
            if self.screenshots_ids:
                self.result['__screenshots_ids__'] = self.screenshots_ids
            if self.detalhamento_do_debito:
                self.result.update(
                    {'detalhamento_do_debito': self.detalhamento_do_debito})
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
                '{renavam}-data_collected.json'.format(
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

    def replace_links(self, response):
        html_text = response.body.decode(
            "ascii", "ignore"
        ).replace(
            "/sc/", "https://www.dividaativa.pge.sp.gov.br/sc/"
        )
        return html_text
