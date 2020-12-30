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


class autos_detran_rr_spider(CustomSpider):
    # required scraper name
    name = "autos_detran_rr"

    # initial urls
    start_url = 'https://www.detran.rr.gov.br/site/apps/veiculo/filtroplacarenavam-consultaveiculo.jsp'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_detran_rr_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        self.regex = re.compile(r'\s+')
        self.data_to_be_scraped = []

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
        sitekey = response.selector.xpath(
            "//button[@id='submeter']/@data-sitekey").get("")
        gcaptcha_txt = self.solve_captcha(sitekey, response.url)
        if not gcaptcha_txt:
            return

        placa_key = response.selector.xpath("//input[@title='Placa']/@name").get("")
        renavam_key = response.selector.xpath("//input[@title='Renavam']/@name").get("")

        frm_data = {
            placa_key: self.placa,
            renavam_key: self.renavam,
            'g-recaptcha-response': gcaptcha_txt}

        url = "https://www.detran.rr.gov.br/site/apps/veiculo/consulta-veiculo.jsp"
        yield FormRequest(url, formdata=frm_data, callback=self.get_main_page, dont_filter=True)

    def get_main_page(self, response):
        error_message = response.selector.xpath(
            "//a[normalize-space()='Veja mais informações aqui']/text()").get("").strip()
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            return

        # init
        chassi = response.selector.xpath("//input[@name='chassi']/@value").get("")

        dados_do_vaiculo_rows = response.selector.xpath(
            "//table[.//th[contains(text(),'Dados do Veículo')]]/tbody/tr")
        i = 0
        while i < len(dados_do_vaiculo_rows):
            row_keys = dados_do_vaiculo_rows[i].xpath("./th/text()").extract()
            if i != 2:
                row_values = dados_do_vaiculo_rows[i + 1].xpath("./td")
            else:
                row_values = dados_do_vaiculo_rows.xpath("../td")
                i -= 1
            i += 2
            for j in range(len(row_keys)):
                value = self.regex.sub(
                    " ", "".join(row_values[j].xpath("./text()").extract()).strip())
                self.result.update({
                    self.remove_diacritics(row_keys[j]): value})

        sem_debitos = response.selector.xpath(
            "//table[.//th[contains(text(),'DÉBITOS DE')]]")
        if not sem_debitos:
            self.result['sem_debitos'] = 'Não existe(m) débito(s)'
        else:
            # 2018
            debitos_de_2018_rows = response.selector.xpath(
                "//table[.//th[contains(text(),'DÉBITOS DE 2018')]]/tbody/tr[./td]")
            debitos_de_2018 = []
            for row in debitos_de_2018_rows:
                debito = row.xpath("./td[1]/text()").get("").strip()
                vencimento = row.xpath("./td[2]/text()").get("").strip()
                valor = row.xpath("./td[3]/text()").get("").strip()
                row_data = {'debito': self.regex.sub(" ", debito),
                            'vencimento': self.regex.sub(" ", vencimento),
                            'valor': self.regex.sub(" ", valor)}
                debitos_de_2018.append(row_data)
    
                if self.get_files:
                    onclick_options = row.xpath("./td[4]/img/@onclick").get("")
                    match = re.search("\'(\d*?)\',\'(\w*?)\'", onclick_options)
                    if match:
                        ano_exercicio = match.group(1)
                        debito_arg = match.group(2)
                    else:
                        continue
    
                    url = "https://www.detran.rr.gov.br/site/apps/veiculo/bordero-financeiro-{}.jsp".format(
                        debito_arg)
    
                    form_data = {
                        "ano_exe": "",
                        "ano_exercicio": ano_exercicio,
                        "tipo_debito": "",
                        "tipo_envio": "",
                        "reemissao": "",
                        "placa": self.placa,
                        "renavam": self.renavam,
                        "chassi": chassi,
                        "tipoVencimento": ""
                    }
                    yield FormRequest(url, formdata=form_data,
                                      callback=self.save_pdf,
                                      meta={'file_key': 'debitos_de_2018',
                                            'file_data': row_data,
                                            'file_type': '__boleto__'},
                                      dont_filter=True)
    
            if debitos_de_2018:
                self.result['debitos_de_2018'] = debitos_de_2018
    
                if self.get_files:
                    onclick_options = debitos_de_2018_rows.xpath(
                        "./following::input[@name='Nova consulta'][1]/@onclick").get("")
                    match = re.search("\'(\d*?)\'", onclick_options)
                    if match:
                        ano_exercicio = match.group(1)
                    url = "https://www.detran.rr.gov.br/site/apps/veiculo/gera-bordero-ano.jsp?PARAMETRO_IPVA=" \
                        "SIM&PARAMETRO_LICENCIAMENTO=SIM&PARAMETRO_SEGURO=" \
                        "SIM&PARAMETRO_MULTA=NAO&OPERADOR=SITE&GERAL=&PARAMETRO_ANO={}".format(ano_exercicio)
                    yield Request(url, callback=self.save_pdf,
                                  meta={'file_key': 'debitos_de_2018',
                                        'file_type': '__boleto_consolidado__'},
                                  dont_filter=True)
    
            # 2019
            debitos_de_2019_rows = response.selector.xpath(
                "//table[.//th[contains(text(),'DÉBITOS DE 2019')]]/tbody/tr[./td]")
            debitos_de_2019 = []
            for row in debitos_de_2019_rows:
                debito = row.xpath("./td[1]/text()").get("").strip()
                vencimento = row.xpath("./td[2]/text()").get("").strip()
                valor = row.xpath("./td[3]/text()").get("").strip()
                row_data = {'debito': self.regex.sub(" ", debito),
                            'vencimento': self.regex.sub(" ", vencimento),
                            'valor': self.regex.sub(" ", valor)}
                debitos_de_2019.append(row_data)
    
                if self.get_files:
                    onclick_options = row.xpath("./td[4]/img/@onclick").get("")
                    match = re.search("\'(\d*?)\',\'(\w*?)\'", onclick_options)
                    if match:
                        ano_exercicio = match.group(1)
                        debito_arg = match.group(2)
                    else:
                        continue
    
                    url = "https://www.detran.rr.gov.br/site/apps/veiculo/bordero-financeiro-{}.jsp".format(
                        debito_arg)
    
                    form_data = {
                        "ano_exe": "",
                        "ano_exercicio": ano_exercicio,
                        "tipo_debito": "",
                        "tipo_envio": "",
                        "reemissao": "",
                        "placa": self.placa,
                        "renavam": self.renavam,
                        "chassi": chassi,
                        "tipoVencimento": ""
                    }
                    yield FormRequest(url, formdata=form_data,
                                      callback=self.save_pdf,
                                      meta={'file_key': 'debitos_de_2019',
                                            'file_data': row_data,
                                            'file_type': '__boleto__'},
                                      dont_filter=True)
    
            if debitos_de_2019:
                self.result['debitos_de_2019'] = debitos_de_2019
    
                if self.get_files:
                    onclick_options = debitos_de_2019_rows.xpath(
                        "./following::input[@name='Nova consulta'][1]/@onclick").get("")
                    match = re.search("\'(\d*?)\'", onclick_options)
                    if match:
                        ano_exercicio = match.group(1)
                    url = "https://www.detran.rr.gov.br/site/apps/veiculo/gera-bordero-ano.jsp?PARAMETRO_IPVA=" \
                        "SIM&PARAMETRO_LICENCIAMENTO=SIM&PARAMETRO_SEGURO=" \
                        "SIM&PARAMETRO_MULTA=NAO&OPERADOR=SITE&GERAL=&PARAMETRO_ANO={}".format(ano_exercicio)
                    yield Request(url, callback=self.save_pdf,
                                  meta={'file_key': 'debitos_de_2019',
                                        'file_type': '__boleto_consolidado__'},
                                  dont_filter=True)
    
            # 2020
            debitos_de_2020_rows = response.selector.xpath(
                "//table[.//th[contains(text(),'DÉBITOS DE 2020')]]/tbody/tr[./td]")
            debitos_de_2020 = []
            for row in debitos_de_2020_rows:
                debito = row.xpath("./td[1]/text()").get("").strip()
                vencimento = row.xpath("./td[2]/text()").get("").strip()
                valor = row.xpath("./td[3]/text()").get("").strip()
                row_data = {'debito': self.regex.sub(" ", debito),
                            'vencimento': self.regex.sub(" ", vencimento),
                            'valor': self.regex.sub(" ", valor)}
                debitos_de_2020.append(row_data)
    
                if self.get_files:
                    onclick_options = row.xpath("./td[4]/img/@onclick").get("")
                    match = re.search("\'(\d*?)\',\'(\w*?)\'", onclick_options)
                    if match:
                        ano_exercicio = match.group(1)
                        debito_arg = match.group(2)
                    else:
                        continue
    
                    url = "https://www.detran.rr.gov.br/site/apps/veiculo/bordero-financeiro-{}.jsp".format(
                        debito_arg)
    
                    form_data = {
                        "ano_exe": "",
                        "ano_exercicio": ano_exercicio,
                        "tipo_debito": "",
                        "tipo_envio": "",
                        "reemissao": "",
                        "placa": self.placa,
                        "renavam": self.renavam,
                        "chassi": chassi,
                        "tipoVencimento": ""
                    }
                    yield FormRequest(url, formdata=form_data,
                                      callback=self.save_pdf,
                                      meta={'file_key': 'debitos_de_2020',
                                            'file_data': row_data,
                                            'file_type': '__boleto__'},
                                      dont_filter=True)
    
            if debitos_de_2020:
                self.result['debitos_de_2020'] = debitos_de_2020
    
                if self.get_files:
                    onclick_options = debitos_de_2020_rows.xpath(
                        "./following::input[@name='Nova consulta'][1]/@onclick").get("")
                    match = re.search("\'(\d*?)\'", onclick_options)
                    if match:
                        ano_exercicio = match.group(1)
                    url = "https://www.detran.rr.gov.br/site/apps/veiculo/gera-bordero-ano.jsp?PARAMETRO_IPVA=" \
                        "SIM&PARAMETRO_LICENCIAMENTO=SIM&PARAMETRO_SEGURO=" \
                        "SIM&PARAMETRO_MULTA=NAO&OPERADOR=SITE&GERAL=&PARAMETRO_ANO={}".format(ano_exercicio)
                    yield Request(url, callback=self.save_pdf,
                                  meta={'file_key': 'debitos_de_2020',
                                        'file_type': '__boleto_consolidado__'},
                                  dont_filter=True)

        service_do_detran_table = response.selector.xpath("//table[.//th[contains(text(),'Serviços do Detran')]]")
        sem_debitos_servico = service_do_detran_table.xpath(
            ".//th[contains(text(),'Não existe(m) débito(s) de Serviço(s) até o presen')]/text()").get("").strip()
        if sem_debitos_servico:
            self.result['sem_debitos_servico'] = sem_debitos_servico
        else:
            servicos_do_detran_rows = service_do_detran_table.xpath("./tbody/tr[./td]")
            servicos_do_detran = []
            for row in servicos_do_detran_rows:
                servico_s = row.xpath("./td[1]/text()").get("").strip()
                data = row.xpath("./td[2]/text()").get("").strip()
                valor_do_servico_r = row.xpath("./td[3]/text()").get("").strip()
                valor_pago_r = row.xpath("./td[4]/text()").get("").strip()
                valor_devido_r = row.xpath("./td[5]/text()").get("").strip()
                row_data = {'servico_s': self.regex.sub(" ", servico_s),
                            'data': self.regex.sub(" ", data),
                            'valor_do_servico_r': self.regex.sub(" ", valor_do_servico_r),
                            'valor_pago_r': self.regex.sub(" ", valor_pago_r),
                            'valor_devido_r': self.regex.sub(" ", valor_devido_r)}
                servicos_do_detran.append(row_data)
            self.result['servicos_do_detran'] = servicos_do_detran

        sem_infracoes = response.selector.xpath(
            "//div[contains(text(),'Não existem débitos de Multas até o presente momento')]/text()").get("").strip()
        if sem_infracoes:
            self.result['sem_infracoes'] = sem_infracoes
        else:
            infracoes_rows = response.selector.xpath(
                "//table[.//th[contains(text(),'Infrações')]]/tbody/tr[./td]")
            infracoes = []
            for row in infracoes_rows:
                situacao = row.xpath("./td[1]/text()").get("").strip()
                qtd = row.xpath("./td[2]/text()").get("").strip()
                valor = row.xpath("./td[3]/text()").get("").strip()
                valor_com_desconto_r = row.xpath("./th[@class='text-danger']/text()").get("").strip()
                if valor_com_desconto_r:
                    is_indisponivel = row.xpath("./td[4]/input[@name='imageField']")
                else:
                    valor_com_desconto_r = row.xpath("./td[4]/text()").get("").strip()
                    is_indisponivel = row.xpath("./td[5]/input[@name='imageField']")
    
                row_data = {'situacao': self.regex.sub(" ", situacao),
                            'qtd': self.regex.sub(" ", qtd),
                            'valor': self.regex.sub(" ", valor),
                            'valor_com_desconto_r': self.regex.sub(" ", valor_com_desconto_r)}
                if not is_indisponivel:
                    row_data.update({'detalhamento': 'indisponível'})
                    infracoes.append(row_data)
                else:
                    onclick_options = is_indisponivel.xpath("./@onclick").get("").split("(")[0]
                    match = re.search('function ' + onclick_options + '\(\) {\s+document\.form\.tipoVencimento\.value = \"(.*?)\";',
                                      response.text)
                    if match:
                        tipoVencimento = match.group(1)
                    else:
                        continue
    
                    form_data = {
                        "ano_exe": "",
                        "ano_exercicio": "",
                        "tipo_debito": "",
                        "tipo_envio": "",
                        "reemissao": "",
                        "placa": self.placa,
                        "renavam": self.renavam,
                        "chassi": chassi,
                        "tipoVencimento": tipoVencimento
                    }
                    self.data_to_be_scraped.append(
                        {'form_data': form_data,
                         'row_data': row_data})
    
            if self.data_to_be_scraped:
                item = self.data_to_be_scraped.pop()
                url = "https://www.detran.rr.gov.br/site/apps/veiculo/detalhar-multa-veiculo.jsp"
                yield FormRequest(url, formdata=item['form_data'],
                                  callback=self.get_detailed_page,
                                  meta={'file_key': 'infracoes',
                                        'file_data': item['row_data']},
                                  dont_filter=True)
            self.result['infracoes'] = infracoes

    def get_detailed_page(self, response):
        result_key = response.meta['file_key']
        file_data = response.meta['file_data']

        detalhamento_list = []
        one_table = response.selector.xpath('//div[@id="gkContentWrap"]')
        if one_table:
            detalhamento = {}
            rows = one_table.xpath(".//table//tr")
            for row in rows:
                values = row.xpath("./td/text()").extract()
                for i in range(0, len(values), 2):
                    key = self.remove_diacritics(values[i])
                    value = self.regex.sub(" ", values[i + 1])
                    detalhamento.update({key: value})
            file_data['detalhamento'] = detalhamento_list
            self.result[result_key].append(file_data)

            if self.get_files:
                url = "https://www.detran.rr.gov.br/site/apps/veiculo/emitir-bordero-multas.jsp"

                multa = response.selector.xpath("//input[@name='multa']/@value").get("")
                tipovenci = response.selector.xpath("//input[@name='tipoVencimento']/@value").get("")
                chassi = response.selector.xpath("//input[@name='chassi']/@value").get("")

                form_data = {
                    "multa": multa,
                    "tipoVencimento": tipovenci,
                    "chassi": chassi,
                    "todos": "on"
                }
                yield FormRequest(url, formdata=form_data,
                                  callback=self.process_pdf,
                                  meta={'file_key': result_key,
                                        'file_data': file_data,
                                        'file_type': '__boleto__'},
                                  dont_filter=True)

        multiple_table = response.selector.xpath('//table[@id="dados"]')
        if multiple_table:
            for table in multiple_table:
                rows = table.xpath(".//tr")
                detalhamento = {}
                for row in rows:
                    key = self.remove_diacritics(row.xpath("./td[1]/text()").get("").strip())
                    value = self.regex.sub(" ", row.xpath("./td[2]/text()").get("").strip())
                    detalhamento.update({key: value})
                detalhamento_list.append(detalhamento)
            file_data['detalhamento'] = detalhamento_list
            self.result[result_key].append(file_data)

            if self.get_files:
                url = "https://www.detran.rr.gov.br/site/apps/veiculo/emitir-bordero-multas.jsp"

                multa = response.selector.xpath("//input[@name='multa']/@value").extract()
                tipovenci = response.selector.xpath("//input[@name='tipoVencimento']/@value").extract()
                chassi = response.selector.xpath("//input[@name='chassi']/@value").extract()

                form_data = {
                    "agrupar": "S",
                    "tipoVencimento": tipovenci,
                    "chassi": chassi,
                    "multa": multa,
                    "todos": ""
                }
                yield FormRequest(url, formdata=form_data,
                                  callback=self.process_pdf,
                                  meta={'file_key': result_key,
                                        'file_data': file_data,
                                        'file_type': '__boleto__'},
                                  dont_filter=True)

            if self.data_to_be_scraped:
                item = self.data_to_be_scraped.pop()
                url = "https://www.detran.rr.gov.br/site/apps/veiculo/detalhar-multa-veiculo.jsp"
                yield FormRequest(url, formdata=item['form_data'],
                                  callback=self.get_detailed_page,
                                  meta={'file_key': 'infracoes',
                                        'file_data': item['row_data']},
                                  dont_filter=True)

    def process_pdf(self, response):
        url = "https://www.detran.rr.gov.br/site/apps/veiculo/bordero-pdf.jsp"
        yield Request(url, callback=self.save_pdf,
                      meta={'file_key': response.meta['file_key'],
                            'file_data': response.meta['file_data'],
                            'file_type': response.meta['file_type']},
                      dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        file_type = response.meta['file_type']
        file_data = response.meta.get('file_data')
        result_key = response.meta['file_key']

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
        if file_data:
            if result_key == "infracoes":
                [[item.update({
                    file_type: {
                        "file_id": file_id}
                }) for item in infracoes_item['detalhamento']
                ] for infracoes_item in result_value
                    if infracoes_item == file_data]
            else:
                [item.update({
                    file_type: {
                        "file_id": file_id}
                }) for item in result_value if item == file_data]
        else:
            [item.update({
                file_type: {
                    "file_id": file_id}
            }) for item in result_value]
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
