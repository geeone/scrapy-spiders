# -*- coding: utf-8 -*-

from datetime import datetime as dt
from http.cookies import SimpleCookie
import os
import sys
import urllib.parse
import uuid

import requests
from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_detran_pr_spider(CustomSpider):
    # required scraper name
    name = "autos_detran_pr"

    # initial urls
    start_url = "https://www.extratodebito.detran.pr.gov.br/detranextratos/geraExtrato.do?"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_detran_pr_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # internal variables
        self.retry_form = 5

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        start_url = self.start_url + urllib.parse.urlencode({'action': 'iniciarProcesso'})
        yield Request(start_url, callback=self.login_me,
                      errback=self.errback_func, dont_filter=True)

    def solve_captcha(self):
        try:
            # SOLVE IMAGE CAPTCHA
            attempts = 0
            while 1:
                # check attempts count to avoid cycled solving
                if attempts < self.captcha_retries:
                    attempts += 1
                    self.img_captcha_id, imgcaptcha_txt = self.captcha_solver(
                        self.captcha_service, captcha_img="captcha.jpg")
                    # check for two words (DBC issue)
                    if imgcaptcha_txt and (" " not in imgcaptcha_txt):
                        print("IMAGE Captcha:", imgcaptcha_txt)
                        return imgcaptcha_txt
                        break
                else:
                    break
            # check if captcha was solved
            if not imgcaptcha_txt:
                details_msg = "Failed to solve captcha for {} times.".format(self.captcha_retries)
                error_msg = {"error_type": "CAPTCHA_NOT_SOLVED", "captcha_service": self.captcha_service, "details": details_msg}
                raise Exception(error_msg)
        except Exception as exc:
            error_msg = exc.args[0]
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return None

    def login_me(self, response):
        """Function to solve Captcha.
        Used to get ReCaptcha options; usual captcha image
        Then call the captcha solver."""

        # get the Captcha's options
        imgcaptcha = response.selector.xpath(
            "//img[@id='imagemCaptcha']/@src").get("")
        img_url = "https://www.extratodebito.detran.pr.gov.br/detranextratos/" + \
            imgcaptcha

        # get cookies to download captcha image
        cookies = response.headers.getlist('Set-Cookie')
        c = SimpleCookie()
        for cookie in cookies:
            c.load(cookie.decode("utf-8"))
        self.cookies_list = [{"name": key, "value": c[key].value} for key in c]

        # set cookies to current session
        session = requests.Session()
        for cookie in self.cookies_list:
            print(cookie)
            session.cookies.set(**cookie)

        # save captcha image
        r = session.get(img_url, stream=True)
        with open("captcha.jpg", 'wb') as f:
            f.write(r.content)

        imgcaptcha_txt = self.solve_captcha()
        if not imgcaptcha_txt:
            return

        frm_data = {
            'renavam': self.renavam,
            'senha': imgcaptcha_txt}
        print(frm_data)

        login_url = self.start_url + urllib.parse.urlencode({'action': 'viewExtract'})
        if not response.meta.get('renavam'):
            yield FormRequest(login_url, formdata=frm_data,
                              callback=self.get_main_page,
                              errback=self.errback_func,
                              cookies=self.cookies_list, dont_filter=True)
        else:
            cookie_list = self.cookies_list.copy()
            yield FormRequest(login_url, formdata=frm_data,
                              callback=self.refresh_cookies,
                              meta={'result_key': response.meta.get('result_key'),
                                    'all_files': response.meta.get('all_files'),
                                    'renavam': response.meta['renavam'],
                                    'cookie_list': cookie_list},
                              cookies=self.cookies_list, dont_filter=True)

    def get_main_page(self, response):
        """Redirect to main page."""
        # initialization
        renavam_int = int(self.renavam)

        alerts = response.selector.xpath("//div[@role='alert']/text()")
        if isinstance(alerts, list):
            error_message = "".join([t.get().strip() for t in alerts])
        else:
            error_message = alerts.get("").strip()

        if "O número de RENAVAM está incorreto!" in error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return
        elif "Código da imagem não confere!" in error_message:
            self.incorrect_captcha_report(
                self.captcha_service, self.img_captcha_id)
            if self.incorrect_captcha_retries > 0:
                start_url = self.start_url + urllib.parse.urlencode(
                    {'action': 'iniciarProcesso'})
                yield Request(start_url, callback=self.login_me,
                              meta={'dont_merge_cookies': True}, dont_filter=True)
            return

        # get values
        self.result['renavam'] = response.selector.xpath("//label[@for='renavam']/following::div[1]/text()").get("").strip()
        self.result['chassi'] = response.selector.xpath("//label[@for='chassi']/following::div[1]/text()").get("").strip()
        self.result['placa'] = response.selector.xpath("//label[@for='placaatual']/following::div[1]/text()").get("").strip()
        self.result['marca_modelo'] = response.selector.xpath("//label[@for='marcamodelo']/following::div[1]/text()").get("").strip()
        self.result['municipio'] = response.selector.xpath("//label[@for='municipiodeemplacamento']/following::div[1]/text()").get("").strip()
        self.result['ano_de_fabricacao_modelo'] = response.selector.xpath("//label[@for='anofabricacaomodelo']/following::div[1]/text()").get("").strip()
        self.result['combustivel'] = response.selector.xpath("//label[@for='combustivel']/following::div[1]/text()").get("").strip()
        self.result['cor'] = response.selector.xpath("//label[@for='cor']/following::div[1]/text()").get("").strip()
        self.result['categoria'] = response.selector.xpath("//label[@for='categoria']/following::div[1]/text()").get("").strip()
        self.result['especie_tipo'] = response.selector.xpath("//label[@for='especietipo']/following::div[1]/text()").get("").strip()
        self.result['situacao_do_veiculo'] = response.selector.xpath("//label[@for='situacao']/following::div[1]/text()").get("").strip()
        self.result['tipo_de_financiamento_restricao'] = response.selector.xpath("//label[@for='restricaoavenda']/following::div[1]/text()").get("").strip()

        total_de_debitos_rows = response.selector.xpath(
            "//h3[contains(text(),'Total de débitos para emissão do CRLV')]/following::table[1]//tr[not(./th) and not(@class='total')]")
        total_de_debitos_para_emissao_do_crlv = []
        for row in total_de_debitos_rows:
            discriminacao = row.xpath("./td[1]/text()").get("").strip()
            valor_em_reais_r = row.xpath("./td[2]/text()").get("").strip()
            total_de_debitos_para_emissao_do_crlv.append({
                'discriminacao': discriminacao,
                'valor_em_reais_r': valor_em_reais_r})
        if total_de_debitos_para_emissao_do_crlv:
            self.result['total_de_debitos_para_emissao_do_crlv'] = total_de_debitos_para_emissao_do_crlv

        debitos_de_ipva_rows = response.selector.xpath(
            "//h3[contains(text(),'Débitos de IPVA')]/following::table[1]//tr[not(./th) and not(@class='total')]")
        debitos_de_ipva = []
        for row in debitos_de_ipva_rows:
            discriminacao = row.xpath("./td[1]/text()").get("").strip()
            valor_em_reais_r = row.xpath("./td[last()]/text()").get("").strip()
            debitos_de_ipva.append({
                'discriminacao': discriminacao,
                'valor_em_reais_r': valor_em_reais_r})
        if debitos_de_ipva:
            self.result['debitos_de_ipva'] = debitos_de_ipva

        debitos_de_licenciamento_rows = response.selector.xpath(
            "//h3[contains(text(),'Débitos de Licenciamento')]/following::table[1]//tr[not(./th) and not(@class='total')]")
        debitos_de_licenciamento = []
        for row in debitos_de_licenciamento_rows:
            discriminacao = row.xpath("./td[1]/text()").get("").strip()
            vencimento = row.xpath("./td[2]/text()").get("").strip()
            valor_em_reais_r = row.xpath("./td[3]/text()").get("").strip()
            debitos_de_licenciamento.append({
                'discriminacao': discriminacao,
                'vencimento': vencimento,
                'valor_em_reais_r': valor_em_reais_r})
        if debitos_de_licenciamento:
            self.result['debitos_de_licenciamento'] = debitos_de_licenciamento

        ultimo_crlv_emitido_rows = response.selector.xpath(
            "//h3[contains(text(),'Último CRLV emitido')]/following::table[1]//tr[not(./th) and not(@class='total') and not(@class='list_cor_nao')]")
        ultimo_crlv_emitido = {}
        for row in ultimo_crlv_emitido_rows:
            key = self.remove_diacritics(row.xpath("./td[1]/text()").get(""))
            value = row.xpath("./td[2]/text()").get("").strip()
            ultimo_crlv_emitido.update({key: value})
        if ultimo_crlv_emitido:
            self.result['ultimo_crlv_emitido'] = ultimo_crlv_emitido

        # files ids to be downloaded
        multas_autuacoes_files = []

        resumo_das_multas_de_transito_rows = response.selector.xpath(
            "//h3[contains(text(),'Resumo das Multas de Trânsito')]/following::table[1]//tr[not(./th) and not(@class='total')]")
        resumo_das_multas_de_transito = []
        for row in resumo_das_multas_de_transito_rows:
            discriminacao = " ".join([item.strip() for item in row.xpath("./td[1]//text()").extract() if item.strip()])
            quantidade = " ".join([item.strip() for item in row.xpath("./td[2]//text()").extract() if item.strip()])
            valor_em_reais_r = " ".join([item.strip() for item in row.xpath("./td[3]//text()").extract() if item.strip()])
            resumo_das_multas_de_transito.append({
                'discriminacao': discriminacao,
                'quantidade': quantidade,
                'valor_em_reais_r': valor_em_reais_r})
            # add file id to be downloaded
            if int(quantidade) > 0:
                multas_autuacoes_files.append(discriminacao)
        if resumo_das_multas_de_transito:
            self.result['resumo_das_multas_de_transito'] = resumo_das_multas_de_transito

        resumo_das_autuacoes_de_transito_rows = response.selector.xpath(
            "//h3[contains(text(),'Resumo das Autuações de Trânsito')]/following::table[1]//tr[not(./th) and not(@class='total')]")
        resumo_das_autuacoes_de_transito = []

        for row in resumo_das_autuacoes_de_transito_rows:
            discriminacao = " ".join([item.strip() for item in row.xpath("./td[1]//text()").extract() if item.strip()])
            quantidade = " ".join([item.strip() for item in row.xpath("./td[2]//text()").extract() if item.strip()])
            valor_em_reais_r = " ".join([item.strip() for item in row.xpath("./td[3]//text()").extract() if item.strip()])
            resumo_das_autuacoes_de_transito.append({
                'discriminacao': discriminacao,
                'quantidade': quantidade,
                'valor_em_reais_r': valor_em_reais_r})
            # add file id to be downloaded
            if int(quantidade) > 0:
                multas_autuacoes_files.append(discriminacao)
        if resumo_das_autuacoes_de_transito:
            self.result['resumo_das_autuacoes_de_transito'] = resumo_das_autuacoes_de_transito

        multas_pagas_rows = response.selector.xpath("//h3[contains(text(),'Resumo das Infrações')]/following::table[1]//tr[not(./th) and not(@class='total')]")
        multas_pagas = []
        for row in multas_pagas_rows:
            td_a = row.xpath("./td[1]/a")
            multas_pagas_date = " ".join([item.strip() for item in td_a.xpath("./text()").extract() if item.strip()])
            multas_pagas_date = dt.strptime(multas_pagas_date, "%d/%m/%Y")
            if self.start_date <= multas_pagas_date <= self.end_date:
                multas_pagas_href = td_a.xpath("./@href").get("").replace("#", "")
                # get pop-up window
                table = response.selector.xpath("//div[@id='{file_id}']//h3/following::table[1]".format(
                    file_id=multas_pagas_href))
                auto = table.xpath(".//label[contains(text(),'Auto')]/following::div[1]/text()").get("").strip()
                situacao = table.xpath(".//label[contains(text(),'Situação')]/following::div[1]/text()").get("").strip()
                orgao_competente = table.xpath(".//label[contains(text(),'Órgão Competente')]/following::div[1]/text()").get("").strip()
                data = table.xpath(".//label[contains(text(),'Data')]/following::div[1]/text()").get("").strip()
                hora = table.xpath(".//label[contains(text(),'Hora')]/following::div[1]/text()").get("").strip()
                infracao = table.xpath(".//label[contains(text(),'Infração')]/following::div[1]/text()").get("").strip()
                local = table.xpath(".//label[contains(text(),'Local')]/following::div[1]/text()").get("").strip()
                valor_infracao = table.xpath(".//label[contains(text(),'Valor Infração')]/following::div[1]/text()").get("").strip()
                valor_pago = table.xpath(".//label[contains(text(),'Valor Pago')]/following::div[1]/text()").get("").strip()
                numero_documento_rec = table.xpath(".//label[contains(text(),'Número Documento/REC')]/following::div[1]/text()").get("").strip()
                tipo = table.xpath(".//label[contains(text(),'Tipo')]/following::div[1]/text()").get("").strip()
                data_de_pagamento = table.xpath(".//label[contains(text(),'Data de Pagamento')]/following::div[1]/text()").get("").strip()
                multas_pagas.append({
                    'auto': auto,
                    'situacao': situacao,
                    'orgao_competente': orgao_competente,
                    'data': data,
                    'hora': hora,
                    'infracao': infracao,
                    'local': local,
                    'valor_infracao': valor_infracao,
                    'valor_pago': valor_pago,
                    'numero_documento_rec': numero_documento_rec,
                    'tipo': tipo,
                    'data_de_pagamento': data_de_pagamento})
        if multas_pagas:
            self.result['multas_pagas'] = multas_pagas

        financiamento_btn = response.selector.xpath("//a[contains(text(),'Financiamento')]")
        if financiamento_btn:
            query_str = {'action': 'viewExtract',
                         'renavam': renavam_int}
            financiamento_url = self.start_url + urllib.parse.urlencode(query_str)
            yield Request(financiamento_url, callback=self.get_financiamento,
                          cookies=self.cookies_list, dont_filter=True)

        '''Bellow is the code to download files.'''

        if self.get_files:
            emissao_licenciamento_download_btn = response.selector.xpath("//div[@id='content-emissao-licenciamento']//a[@id='extPgtoGuia']")
            if emissao_licenciamento_download_btn:
                start_url = self.start_url + urllib.parse.urlencode(
                    {'action': 'iniciarProcesso'})
                yield Request(start_url, callback=self.login_me,
                              meta={'result_key': 'total_de_debitos_para_emissao_do_crlv',
                                    'renavam': renavam_int,
                                    'dont_merge_cookies': True},
                              dont_filter=True)

            all_files = []
            for file_id in multas_autuacoes_files:
                result_key = self.remove_diacritics(file_id)
                rows = response.selector.xpath(
                    "//div[@id='modal-{file_id}']//h3/following::table[1]//tr[contains(@class,'multa-descricao')]".format(
                        file_id=file_id.encode('ascii', 'ignore').decode('utf-8').replace(" ", "").upper()))
                files_list = []
                for row in rows:
                    auto = row.xpath(".//label[contains(text(),'Auto')]/following::div[1]/text()").get("").strip()
                    situacao = row.xpath(".//label[contains(text(),'Situação')]/following::div[1]/text()").get("").strip()
                    orgao_competente = row.xpath(".//label[contains(text(),'Órgão Competente')]/following::div[1]/text()").get("").strip()
                    data = row.xpath(".//label[contains(text(),'Data')]/following::div[1]/text()").get("").strip()
                    hora = row.xpath(".//label[contains(text(),'Hora')]/following::div[1]/text()").get("").strip()
                    infracao = row.xpath(".//label[contains(text(),'Infração')]/following::div[1]/text()").get("").strip()
                    local = row.xpath(".//label[contains(text(),'Local')]/following::div[1]/text()").get("").strip()
                    vencimento_do_auto = row.xpath(".//label[contains(text(),'Vencimento do Auto')]/following::div[1]/text()").get("").strip()
                    valor_original_r = row.xpath(".//label[contains(text(),'Valor Original')]/following::div[1]/text()").get("").strip()
                    valor_desconto_r = row.xpath(".//label[contains(text(),'Valor Desconto')]/following::div[1]/text()").get("").strip()
                    valor_juros_r = row.xpath(".//label[contains(text(),'Valor Juros')]/following::div[1]/text()").get("").strip()
                    total_r = row.xpath(".//label[contains(text(),'Total')]/following::div[1]/text()").get("").strip()
                    download_btn = row.xpath(".//button[contains(text(),'Emitir guia para pagamento')]/text()").get("")

                    file_data = {
                        'auto': auto,
                        'situacao': situacao,
                        'orgao_competente': orgao_competente,
                        'data': data,
                        'hora': hora,
                        'infracao': infracao,
                        'local': local,
                        'vencimento_do_auto': vencimento_do_auto,
                        'valor_original_r': valor_original_r,
                        'valor_desconto_r': valor_desconto_r,
                        'valor_juros_r': valor_juros_r,
                        'total_r': total_r,
                        'download_btn': download_btn}
                    files_list.append(file_data)
                if files_list:
                    all_files.append({result_key: files_list})
            if all_files:
                start_url = self.start_url + urllib.parse.urlencode(
                    {'action': 'iniciarProcesso'})
                yield Request(start_url, callback=self.login_me,
                              meta={'all_files': all_files,
                                    'renavam': renavam_int,
                                    'dont_merge_cookies': True},
                              dont_filter=True)

    def get_financiamento(self, response):
        financiamento_table = response.selector.xpath(
            "//h3[contains(text(),'Financiamento/Restrição')]/following::div[@class='content-tab'][1]")
        financiamento_restricao = {}
        if financiamento_table:
            financiamento_restricao['nome_da_financeira'] = financiamento_table.xpath(
                ".//label[contains(text(),'Nome da Financeira')]/following::div[1]/text()").get("").strip()
            financiamento_restricao['tipo_de_financiamento_restricao'] = financiamento_table.xpath(
                ".//label[contains(text(),'Tipo de Financiamento/Restrição')]/following::div[1]/text()").get("").strip()
            financiamento_restricao['situacao_da_restricao'] = financiamento_table.xpath(
                ".//label[contains(text(),'Situação da restrição')]/following::div[1]/text()").get("").strip()
            financiamento_restricao['nome_do_contratante'] = financiamento_table.xpath(
                ".//label[contains(text(),'Nome do contratante')]/following::div[1]/text()").get("").strip()
            financiamento_restricao['numero_do_contrato'] = financiamento_table.xpath(
                ".//label[contains(text(),'Número do contrato')]/following::div[1]/text()").get("").strip()
            financiamento_restricao['data_do_contrato'] = financiamento_table.xpath(
                ".//label[contains(text(),'Data do contrato')]/following::div[1]/text()").get("").strip()
            financiamento_restricao['data_horario_de_atualizacao'] = financiamento_table.xpath(
                ".//label[contains(text(),'Data/Horário de atualização')]/following::div[1]/text()").get("").strip()
        if financiamento_restricao:
            self.result['financiamento_restricao'] = financiamento_restricao

    def refresh_cookies(self, response):
        renavam = response.meta['renavam']
        cookie_list = response.meta['cookie_list']
        result_key = response.meta.get('result_key')
        all_files = response.meta.get('all_files')

        if result_key:
            query_str = {'action': 'emiteGuiaLicenciamento',
                         'renavam': renavam,
                         'tipo': 1}
            emissao_licenciamento_url = self.start_url + urllib.parse.urlencode(query_str)
            yield Request(emissao_licenciamento_url, callback=self.save_pdf,
                          meta={'result_key': 'total_de_debitos_para_emissao_do_crlv'},
                          cookies=cookie_list, dont_filter=True)
        else:
            for files_dct in all_files:
                for result_key, files_list in files_dct.items():
                    for file_data in files_list:
                        if file_data.pop('download_btn'):
                            query_str = {
                                'action': 'emiteAuto',
                                'renavam': renavam,
                                'auto': file_data['auto']}
                            download_url = self.start_url + urllib.parse.urlencode(query_str)
                            yield Request(download_url, callback=self.save_pdf,
                                          meta={'result_key': result_key,
                                                'file_data': file_data},
                                          cookies=cookie_list, dont_filter=True)
                    self.result[result_key] = files_list

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = response.meta['result_key']
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
        result_value = self.result.get(result_key, [])
        [item.update({
            file_type: {
                "file_id": file_id}
        }) for item in result_value
            if not file_data or (file_data and item == file_data)]
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
            start_url = self.start_url + urllib.parse.urlencode({'action': 'iniciarProcesso'})
            req = Request(start_url, callback=self.yield_item,
                          errback=self.yield_item, dont_filter=True)
            self.crawler.engine.crawl(req, spider)

    def yield_item(self, response):
        """Function is using to yield Scrapy Item
        Required for us to see the result in ScrapingHub"""
        item = BrobotBotsItem()
        item.update(self.data)
        yield item
