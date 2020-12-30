# -*- coding: utf-8 -*-

from datetime import datetime as dt
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


class energy_celesc_spider(CustomSpider):
    # required scraper name
    name = "energy_celesc"

    # initial urls
    start_url = "https://agenciaweb.celesc.com.br/AgenciaWeb/autenticar/loginCliente.do"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(energy_celesc_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # internal variables
        self.table_content = []
        self.file_links = []

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        yield Request(self.start_url, callback=self.agencia_enter,
                      errback=self.errback_func, dont_filter=True)

    def agencia_enter(self, response):
        url = "https://agenciaweb.celesc.com.br/AgenciaWeb/autenticar/autenticar.do"

        frm_data = {'param_url': '/agencia/',
                    'sqUnidadeConsumidora': self.unidade_consumidora,
                    'numeroMedidor': 'false',
                    'tpDocumento': 'CPJ',
                    'numeroDocumentoCPF': '',
                    'numeroDocumentoCNPJ': self.cnpj,
                    'autenticarSemDocumento': 'false',
                    'tipoUsuario': 'clienteUnCons'}

        yield FormRequest(url, formdata=frm_data,
                          callback=self.senha_validation,
                          errback=self.errback_func, dont_filter=True)

    def senha_validation(self, response):
        error_alert = response.selector.xpath("//span[@class='textoErroMensagemNegrito']/text()").get("")
        if "Por favor, corrija os seguintes erros antes de continuar" in error_alert:
            error_message = response.selector.xpath("//span[@class='textoErroMensagem']/text()").get("")
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return
        url = "https://agenciaweb.celesc.com.br/AgenciaWeb/autenticar/validarSenha.do"
        yield FormRequest(url, formdata={'senha': self.senha},
                          callback=self.conta_page,
                          errback=self.errback_func, dont_filter=True)

    def conta_page(self, response):
        error_alert = response.selector.xpath("//span[@class='textoErroMensagemNegrito']/text()").get("")
        if "Desculpe, algo não ocorreu como esperavamos" in error_alert:
            error_message = response.selector.xpath("//span[@class='textoErroMensagem']/text()").get("")
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.error(error_msg)
            return

        nome = response.selector.xpath("//span[contains(text(),'Nome:')]/following::span[1]/text()").get("").strip()
        cnpj = response.selector.xpath("//span[contains(text(),'CNPJ:')]/following::span[1]/text()").get("").strip()
        seu_codigo = response.selector.xpath("//span[contains(text(),'Seu Código:')]/following::span[1]/text()").get("").strip()
        endereco = response.selector.xpath("//span[contains(text(),'Endereço:')]/following::span[1]/text()").get("").strip()
        cidade = response.selector.xpath("//span[contains(text(),'Cidade:')]/following::span[1]/text()").get("").strip()
        telefone = response.selector.xpath("//span[contains(text(),'Telefone:')]/following::span[1]/text()").get("").strip()
        celular = response.selector.xpath("//span[contains(text(),'Celular:')]/following::span[1]/text()").get("").strip()
        fax = response.selector.xpath("//span[contains(text(),'Fax:')]/following::span[1]/text()").get("").strip()
        e_mail = response.selector.xpath("//span[contains(text(),'E-mail:')]/following::span[1]/text()").get("").strip()
        e_mail_de_envio_da_fatura = response.selector.xpath("//span[contains(text(),'E-mail de envio da fatura:')]/following::span[1]/text()").get("").strip()
        situacao_atual_da_unidade_consumidora = response.selector.xpath("//span[contains(text(),'Situação atual da Unidade Consumidora:')]/following::span[1]/text()").get("").strip()

        self.result.update(
            {'nome': nome,
             'cnpj': cnpj,
             'seu_codigo': seu_codigo,
             'endereco': endereco,
             'cidade': cidade,
             'telefone': telefone,
             'celular': celular,
             'fax': fax,
             'e_mail': e_mail,
             'e_mail_de_envio_da_fatura': e_mail_de_envio_da_fatura,
             'situacao_atual_da_unidade_consumidora': situacao_atual_da_unidade_consumidora})

        url = "https://agenciaweb.celesc.com.br/AgenciaWeb/consultarHistoricoPagto/consultarHistoricoPagto.do"
        yield Request(url, callback=self.payment_history,
                      errback=self.errback_func, dont_filter=True)

    def payment_history(self, response):
        rows = response.selector.xpath("//table[@id='histFat']/tbody/tr")
        regex = re.compile(r'\s+')
        for row in rows:
            data_de_vencimento = regex.sub(" ", row.xpath("./td[4]/text()").get(""))
            vencimento_datetime = dt.strptime(data_de_vencimento, "%d/%m/%Y")
            if self.start_date <= vencimento_datetime <= self.end_date:
                uc = regex.sub(" ", row.xpath("./td[1]/text()").get(""))
                mes_referencia = regex.sub(" ", row.xpath("./td[2]/a/text()").get(""))
                situacao = regex.sub(" ", row.xpath("./td[3]/text()").get(""))
                data_de_pagamento = regex.sub(" ", row.xpath("./td[5]/text()").get(""))
                valor_emissao = regex.sub(" ", row.xpath("./td[6]/text()").get(""))
                valor_pago = regex.sub(" ", row.xpath("./td[7]/text()").get(""))

                data_item = {'uc': uc,
                             'mes_referencia': mes_referencia,
                             'situacao': situacao,
                             'data_de_vencimento': data_de_vencimento,
                             'data_de_pagamento': data_de_pagamento,
                             'valor_emissao': valor_emissao,
                             'valor_pago': valor_pago}
                self.table_content.append(data_item)

                if not data_de_pagamento:
                    referencia_link = "https://agenciaweb.celesc.com.br" + row.xpath("./td[2]/a/@href").get("")
                    file_data = data_item.copy()
                    file_data['url'] = referencia_link
                    self.file_links.append(file_data)

        next_page = response.selector.xpath("//a[./img[contains(@src,'seta-dir.gif')]]/@href").get("")
        if next_page:
            url = "https://agenciaweb.celesc.com.br" + next_page
            yield Request(url, callback=self.payment_history,
                          errback=self.errback_func, dont_filter=True)
        else:
            self.result.update(
                {'historico_de_pagamento': self.table_content})
            yield from self.download_files()

    def download_files(self):
        for file_data in self.file_links:
            url = file_data.pop('url').replace("imprimirSegundaVia.do", "exibirFat.do")
            yield Request(url, callback=self.save_pdf,
                          meta={'file_data': file_data},
                          dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # hardcoded in this case since we don't have another types
        result_key = "historico_de_pagamento"
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
                    cnpj=self.cnpj))
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
