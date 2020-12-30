# -*- coding: utf-8 -*-

import os
import re
import sys
import uuid

from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_detran_terceiros_sp_spider(CustomSpider):
    # required scraper name
    name = "autos_detran_terceiros_sp"

    # initial urls
    start_url = 'https://www.detran.sp.gov.br/wps/portal/portaldetran/cidadao/veiculos/servicos/pesquisaDebitosRestricoesVeiculos'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_detran_terceiros_sp_spider, cls).from_crawler(
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
        debitos_form = response.selector.xpath("//form[.//p[contains(text(),'Débitos')]]")
        form_name = debitos_form.xpath("./@name").get("")
        form_value = debitos_form.xpath(".//a[./p[contains(text(),'Débitos')]]/@id").get("")
        frm_data = {"{}:_idcl".format(form_name): form_value}
        # print(frm_data)
        form_inputs = debitos_form.xpath(".//input")
        for inpt in form_inputs:
            inpt_name = inpt.xpath("./@name").get("")
            inpt_val = inpt.xpath("./@value").get("")
            frm_data.update({inpt_name: inpt_val})
            # print("{}:{}".format(inpt_name, inpt_val))

        url = "https://www.detran.sp.gov.br" + debitos_form.xpath("./@action").get("")
        yield FormRequest(url, formdata=frm_data,
                          callback=self.set_renavam,
                          errback=self.errback_func, dont_filter=True)

    def set_renavam(self, response):
        # get the Captcha's options
        sitekey = response.selector.xpath(
            "//div[@class='g-recaptcha']/@data-sitekey").get("")

        gcaptcha_txt = self.solve_captcha(sitekey, response.url)
        if not gcaptcha_txt:
            return

        renavam_form = response.selector.xpath("//form[.//td[contains(text(),'Renavam')]]")
        frm_data = {'g-recaptcha-response': gcaptcha_txt}
        # print(frm_data)
        form_inputs = renavam_form.xpath(".//input")
        for inpt in form_inputs:
            inpt_name = inpt.xpath("./@name").get("")
            inpt_val = inpt.xpath("./@value").get("")
            if ":renavam" in inpt_name:
                inpt_val = self.renavam
            if ":placa" in inpt_name:
                inpt_val = self.placa
            if "Voltar" in inpt_val:
                continue
            frm_data.update({inpt_name: inpt_val})
            # print("{}:{}".format(inpt_name, inpt_val))

        url = "https://www.detran.sp.gov.br" + renavam_form.xpath("./@action").get("")
        # print(url)
        yield FormRequest(url, formdata=frm_data,
                          callback=self.get_main_page,
                          errback=self.errback_func, dont_filter=True)

    def get_main_page(self, response):
        # take a screenshot
        if self.capture_screenshot:
            html_text = self.decode_response_to_utf8(response, "utf-8")
            html_text = re.sub("<script.+barra_governo\.js.+</script>", "", html_text)
            self.take_screenshot(response, html_text=html_text)

        error_message = response.selector.xpath(
            "//ul[contains(@class,'alert-error') and not(@style)]/li/span/text()").get("")
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        tables = response.selector.xpath("//table[@class='tabela']")
        regex = re.compile(r'\s+')
        for table in tables:
            title = self.remove_diacritics(
                regex.sub(" ", table.xpath(
                    ".//tr/td[not(./span[@id])]/strong/text()").get("")))
            if "licenciamento_digital" in title:
                continue
            elif "laudo_de_vistoria" in title:
                rows = response.selector.xpath("//table[@class='tableResultadoLaudo']/tbody/tr")
                table_content = []
                regex = re.compile(r'\s+')
                for row in rows:
                    data_da_vistoria = regex.sub(" ", " ".join(
                        row.xpath("./td[1]/span//text()").extract()).strip())
                    empresa_responsavel = regex.sub(" ", " ".join(
                        row.xpath("./td[2]/span//text()").extract()).strip())
                    km = regex.sub(" ", " ".join(
                        row.xpath("./td[3]/span//text()").extract()).strip())
                    resultado_da_vistoria = regex.sub(" ", " ".join(
                        row.xpath("./td[4]/span//text()").extract()).strip())
                    motivo_resultado = regex.sub(" ", " ".join(
                        row.xpath("./td[5]/span//text()").extract()).strip())
                    situacao = regex.sub(" ", " ".join(
                        row.xpath("./td[6]/span//text()").extract()).strip())
                    table_content.append({
                        'data_da_vistoria': data_da_vistoria,
                        'empresa_responsavel': empresa_responsavel,
                        'km': km,
                        'resultado_da_vistoria': resultado_da_vistoria,
                        'motivo_resultado': motivo_resultado,
                        'situacao': situacao})
                if table_content:
                    self.result[title] = table_content
                else:
                    self.result["sem_laudo_de_vistoria"] = "Não existem vistorias eletrônicas realizadas no estado de São Paulo para o veiculo."
            elif 'dados_do_veiculo' in title:
                self.result['renavam'] = table.xpath("//td[./strong[contains(text(),'Renavam')]]/span/text()").get("")
                self.result['placa'] = table.xpath("//td[./strong[contains(text(),'Placa')]]/span/text()").get("")
            else:
                rows = table.xpath(".//tr[.//span[@id]]/td")
                table_content = {}
                for row in rows:
                    key = self.remove_diacritics(
                        regex.sub(
                            " ", row.xpath("./strong/text()").get("").strip()))
                    value = regex.sub(" ", " ".join(
                        row.xpath(
                            "./span//text() | ./text() | ./a/text()").extract()
                        ).strip())
                    table_content[key] = value
                if table_content:
                    self.result[title] = table_content

        # get formdata
        multas_form = response.selector.xpath("//div[@class='container']/form")
        form_name = multas_form.xpath("./@name").get("")
        frm_data = {}
        form_inputs = multas_form.xpath(".//input")
        for inpt in form_inputs:
            inpt_name = inpt.xpath("./@name").get("")
            inpt_val = inpt.xpath("./@value").get("")
            frm_data.update({inpt_name: inpt_val})

        # get file
        if self.get_files:
            btn_id = response.selector.xpath("//a[@title='Imprimir']/@id").get("")
            if btn_id:
                frm_data_copy = frm_data.copy()
                frm_data_copy.update({form_name: form_name,
                                      'javax.faces.behavior.event': 'click',
                                      'javax.faces.partial.event': 'click',
                                      'javax.faces.source': btn_id,
                                      'javax.faces.partial.ajax': 'true',
                                      'javax.faces.partial.execute': btn_id})
                url = "http://www.detran.sp.gov.br" + frm_data_copy['javax.faces.encodedURL']
                yield FormRequest(url, formdata=frm_data_copy,
                                  callback=self.downoad_request,
                                  errback=self.errback_func, dont_filter=True)

    def downoad_request(self, response):
        url = "https://www.detran.sp.gov.br/.VeiculosWeb/ReportConsultaDebitosServlet"
        yield Request(url, callback=self.save_pdf, dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        file_type = "__comprovante_de_acesso__"

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        with open(file_path, 'wb') as f:
            f.write(response.body)

        # upload pdf to s3 and call the webhook
        self.upload_file(file_id)

        # update values in result
        self.result.update({file_type: {"file_id": file_id}})

    def get_final_result(self, spider):
        """Will be called before spider closed
        Used to save data_collected result."""

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
