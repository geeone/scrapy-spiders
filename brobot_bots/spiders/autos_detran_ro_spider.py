# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import uuid

import pdfkit
from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script, autos_detran_ro_script
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_detran_ro_spider(CustomSpider):
    # required scraper name
    name = "autos_detran_ro"

    # initial urls
    start_url = "https://consulta.detran.ro.gov.br/CentralDeConsultasInternet/Internet/Veiculo/ConsultaVeiculo.asp"

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_detran_ro_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # internal variables
        self.splash_args.update({'wait': 5})
        self.retry_form = 5

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

        # get the Captcha's options
        sitekey = response.selector.xpath(
            "//div[@class='g-recaptcha']/@data-sitekey").get("")

        gcaptcha_txt = self.solve_captcha(sitekey, response.url)
        if not gcaptcha_txt:
            return

        frm_data = {
            'Renavam': self.renavam,
            'Placa': self.placa,
            'g-recaptcha-response': gcaptcha_txt}

        # yield FormRequest(self.start_url, formdata=frm_data, callback=self.get_main_page, dont_filter=True)
        yield SplashFormRequest(self.start_url, formdata=frm_data,
                                callback=self.get_main_page,
                                errback=self.errback_func,
                                endpoint='execute', cache_args=['lua_source'],
                                args={'lua_source': autos_detran_ro_script}, dont_filter=True)

    def get_main_page(self, response):
        """Redirect to main page."""

        error_message = response.selector.xpath("//div[@class='msgErro']/text()").get("")
        print(error_message)
        if "Nenhum registro encontrado, verifique os dados digitados" in error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return
        elif "Confirme que você não é um robô." in error_message:
            self.incorrect_captcha_report(
                self.captcha_service, self.g_recaptcha_id)
            if self.incorrect_captcha_retries > 0:
                yield Request(self.start_url, callback=self.login_me,
                              meta={'dont_merge_cookies': True}, dont_filter=True)
            return

        regex = re.compile(r'\s+')
        self.result['placa'] = regex.sub(" ", response.selector.xpath("//span[text()='Placa']/../text()").get("").strip())
        self.result['marca_modelo'] = regex.sub(" ", response.selector.xpath("//span[text()='Marca/Modelo']/../text()").get("").strip())
        self.result['fabricacao_modelo'] = regex.sub(" ", response.selector.xpath("//span[text()='Fabricacao/Modelo']/../text()").get("").strip())
        self.result['cor'] = regex.sub(" ", response.selector.xpath("//span[text()='Cor']/../text()").get("").strip())
        renavam = regex.sub(" ", response.selector.xpath("//span[text()='Renavam']/../text()").get("").strip())
        self.result['renavam'] = renavam
        self.result['tipo'] = regex.sub(" ", response.selector.xpath("//span[text()='Tipo']/../text()").get("").strip())
        self.result['carroceria'] = regex.sub(" ", response.selector.xpath("//span[text()='Carroceria']/../text()").get("").strip())
        self.result['especie'] = regex.sub(" ", response.selector.xpath("//span[text()='Especie']/../text()").get("").strip())
        self.result['lugares'] = regex.sub(" ", response.selector.xpath("//span[text()='Lugares']/../text()").get("").strip())
        self.result['categoria'] = regex.sub(" ", response.selector.xpath("//span[text()='Categoria']/../text()").get("").strip())
        self.result['potencia'] = regex.sub(" ", response.selector.xpath("//span[text()='Potência']/../text()").get("").strip())
        self.result['combustivel'] = regex.sub(" ", response.selector.xpath("//span[text()='Combustível']/../text()").get("").strip())
        self.result['nome_do_proprietario'] = regex.sub(" ", response.selector.xpath("//span[text()='Nome do Proprietário']/../text()").get("").strip())
        self.result['situacao_lacre'] = regex.sub(" ", response.selector.xpath("//span[text()='Situação Lacre']/../text()").get("").strip())
        self.result['proprietario_anterior'] = regex.sub(" ", response.selector.xpath("//span[text()='Proprietário Anterior']/../text()").get("").strip())
        self.result['origem_dos_dados_do_veiculo'] = regex.sub(" ", response.selector.xpath("//span[text()='Origem dos Dados do Veículo']/../text()").get("").strip())
        self.result['placa_anterior'] = regex.sub(" ", response.selector.xpath("//span[text()='Placa Anterior']/../text()").get("").strip())
        self.result['municipio_de_emplacamento'] = regex.sub(" ", response.selector.xpath("//span[text()='Municipio de Emplacamento']/../text()").get("").strip())
        self.result['licenciado_ate'] = regex.sub(" ", response.selector.xpath("//span[text()='Licenciado ate']/../text()").get("").strip())
        self.result['adquirido_em'] = regex.sub(" ", response.selector.xpath("//span[text()='Adquirido em']/../text()").get("").strip())
        self.result['situacao'] = regex.sub(" ", response.selector.xpath("//span[text()='Situação']/../text()").get("").strip())
        self.result['restricao_a_venda'] = regex.sub(" ", response.selector.xpath("//span[text()='Restrição a Venda']/../text()").get("").strip())
        self.result['informacoes_pendentes_originadas_das_financeiras_via_sng_sistema_nacional_de_gravame'] = regex.sub(" ", response.selector.xpath(
            "//span[text()='Informações PENDENTES originadas das financeiras via SNG - Sistema Nacional de Gravame']/../text()").get("").strip())
        self.result['impedimentos'] = regex.sub(" ", response.selector.xpath("//span[text()='Impedimentos']/../text()").get("").strip())

        debitos_rows = response.selector.xpath("//div[@id='corpo_DebitosVeiculo']/div[@id='Integral']/table[@id='TabelaIntegral']/tbody/tr")
        debitos = []
        for row in debitos_rows:
            descricao = regex.sub(" ", row.xpath("./td[1]/text()").get("").strip())
            vencimento = regex.sub(" ", row.xpath("./td[2]/text()").get("").strip())
            nominal_r = regex.sub(" ", row.xpath("./td[3]/text()").get("").strip())
            corrigido_r = regex.sub(" ", row.xpath("./td[4]/text()").get("").strip())
            desconto_r = regex.sub(" ", row.xpath("./td[5]/text()").get("").strip())
            juros_r = regex.sub(" ", row.xpath("./td[6]/text()").get("").strip())
            multa_r = regex.sub(" ", row.xpath("./td[7]/text()").get("").strip())
            atual_r = regex.sub(" ", row.xpath("./td[8]/text()").get("").strip())
            debitos.append({
                'descricao': descricao,
                'vencimento': vencimento,
                'nominal_r': nominal_r,
                'corrigido_r': corrigido_r,
                'desconto_r': desconto_r,
                'juros_r': juros_r,
                'multa_r': multa_r,
                'atual_r': atual_r})
        self.result.update({'debitos': debitos})

        infracoes_em_autuacao_rows = response.selector.xpath(
            "//div[@id='corpo_AutuacoesVeiculo']//tbody/tr")
        infracoes_em_autuacao = []
        for row in infracoes_em_autuacao_rows:
            num_auto = " ".join([s.strip() for s in row.xpath("./td[1]/text()").extract() if s.strip()])
            if "veículo até o momento." in num_auto:
                break
            status = regex.sub(" ", " ".join(row.xpath("./td[2]/text()").extract()).strip())
            descricao = regex.sub(" ", " ".join(row.xpath("./td[3]/text()").extract()).strip())
            local_complemento = regex.sub(" ", " ".join(row.xpath("./td[4]/text()").extract()).strip())
            valor = regex.sub(" ", " ".join(row.xpath("./td[5]/text()").extract()).strip())
            infracoes_em_autuacao.append({
                'num_auto': num_auto,
                'status': status,
                'descricao': descricao,
                'local_complemento': local_complemento,
                'valor': valor})
        self.result.update({'infracoes_em_autuacao': infracoes_em_autuacao})

        penalidades_multas_rows = response.selector.xpath(
            "//div[@id='corpo_MultasVeiculo']//tbody/tr")
        penalidades_multas = []
        for row in penalidades_multas_rows:
            num_auto = regex.sub(" ", " ".join(row.xpath("./td[1]/text()").extract()).strip())
            if "veículo até o momento." in num_auto:
                break
            status = regex.sub(" ", " ".join(row.xpath("./td[2]/text()").extract()).strip())
            descricao = regex.sub(" ", " ".join(row.xpath("./td[3]/text()").extract()).strip())
            local_complemento = regex.sub(" ", " ".join(row.xpath("./td[4]/text()").extract()).strip())
            valor = regex.sub(" ", " ".join(row.xpath("./td[5]/text()").extract()).strip())
            penalidades_multas.append({
                'num_auto': num_auto,
                'status': status,
                'descricao': descricao,
                'local_complemento': local_complemento,
                'valor': valor})
        self.result.update({'penalidades_multas': penalidades_multas})

        recursos_infracao_rows = response.selector.xpath(
            "//div[@id='corpo_RecursosInfracao']//tbody/tr")
        recursos_infracao = []
        for row in recursos_infracao_rows:
            processo = regex.sub(" ", " ".join(row.xpath("./td[1]/text()").extract()).strip())
            if "veículo até o momento." in processo:
                break
            n_proc_renainf = regex.sub(" ", " ".join(row.xpath("./td[2]/text()").extract()).strip())
            numero_do_auto = regex.sub(" ", " ".join(row.xpath("./td[3]/text()").extract()).strip())
            detalhamento_da_infracao = regex.sub(" ", " ".join(row.xpath("./td[4]/text()").extract()).strip())
            situacao_do_processo = regex.sub(" ", " ".join(row.xpath("./td[5]/text()").extract()).strip())
            recursos_infracao.append({
                'processo': processo,
                'nº_proc_renainf': n_proc_renainf,
                'numero_do_auto': numero_do_auto,
                'detalhamento_da_infracao': detalhamento_da_infracao,
                'situacao_do_processo': situacao_do_processo})
        self.result.update({'recursos_infracao': recursos_infracao})

        dare_btn = response.selector.xpath("//input[@id='BotaoIntegral']")
        if self.get_files and dare_btn:
            dare_url = "https://consulta.detran.ro.gov.br/CentralDeConsultasInternet/Internet/DARE.asp"
            hdListaIdDebitos = response.selector.xpath("//input[@name='hdListaIdDebitos']/@value").get("")
            hdPlaca = response.selector.xpath("//input[@name='hdPlaca']/@value").get("")
            frm_data = {'hdListaIdDebitos': hdListaIdDebitos,
                        'hdPlaca': hdPlaca}
            print(frm_data)
            yield SplashFormRequest(dare_url, formdata=frm_data,
                                    endpoint='render.json', args=self.splash_args,
                                    meta={'result_key': 'debitos'},
                                    callback=self.print_html_to_pdf, dont_filter=True)

        ipva_url = "https://portalcontribuinte.sefin.ro.gov.br/Publico/ConsultaRenavam.jsp?renavam={}".format(renavam)
        yield Request(ipva_url, callback=self.get_ipva_search, meta={'renavam': renavam}, dont_filter=True)

    def get_ipva_search(self, response):
        action = response.selector.xpath("//input[@id='action']/@value").get("")
        csrf_token = response.selector.xpath("//input[@id='csrf_token']/@value").get("")
        sitekey = response.selector.xpath(
            "//script[contains(@src,'recaptcha/api')]/@src").get("").split("render=")[-1]
        gcaptcha_txt = self.solve_captcha(sitekey, response.request.url,
                                          captcha_type=5,
                                          captcha_action='portal_consulta_renavam')
        if not gcaptcha_txt:
            return

        frm_data = {'action': action,
                    'renavam': response.meta['renavam'],
                    'csrf_token': csrf_token,
                    'recaptcha_response': gcaptcha_txt}
        ipva_url = "https://portalcontribuinte.sefin.ro.gov.br/Publico/__Resultado_Renavam_.jsp"
        yield SplashFormRequest(ipva_url, formdata=frm_data,
                                endpoint='render.json', args=self.splash_args,
                                meta={'renavam': response.meta['renavam']},
                                callback=self.get_ipva_result, dont_filter=True)

    def get_ipva_result(self, response):
        page_loaded = response.selector.xpath(
            "//legend[contains(text(),'Dados do Veiculo')]")
        if not page_loaded and self.retry_form > 0:
            self.retry_form -= 1
            time.sleep(30)
            ipva_url = "https://portalcontribuinte.sefin.ro.gov.br/Publico/ConsultaRenavam.jsp?renavam={}".format(response.meta['renavam'])
            yield Request(ipva_url, callback=self.get_ipva_search,
                          meta={'renavam': response.meta['renavam'],
                                'dont_merge_cookies': True}, dont_filter=True)
            return
        elif not page_loaded:
            error_msg = {"error_type": "PAGE_NOT_LOADED", "url": response.request.url}
            self.errors.append(error_msg)
            self.logger.error(error_msg)
            return

        debitos_de_ipva_em_aberto = []
        open_debts_rows = response.selector.xpath(
            "//legend[contains(text(),'Debitos de IPVA em Aberto')]/following::table[1]//tbody/tr[./td]")
        regex = re.compile(r'\s+')
        for row in open_debts_rows:
            no_da_guia = regex.sub(" ", row.xpath("./td[1]/text()").get("").strip())
            no_parcela = regex.sub(" ", row.xpath("./td[2]/text()").get("").strip())
            receita = regex.sub(" ", row.xpath("./td[3]/text()").get("").strip())
            vencimento_original = regex.sub(" ", row.xpath("./td[4]/text()").get("").strip())
            valor_original = regex.sub(" ", row.xpath("./td[5]/text()").get("").strip())
            vencimento_atualizado = regex.sub(" ", row.xpath("./td[6]/text()").get("").strip())
            valor_atualizado = regex.sub(" ", row.xpath("./td[7]/text()").get("").strip())
            situacao = regex.sub(" ", row.xpath("./td[8]/text()").get("").strip())
            debitos_de_ipva_em_aberto.append({
                'no_da_guia': no_da_guia,
                'no_parcela': no_parcela,
                'receita': receita,
                'vencimento_original': vencimento_original,
                'valor_original': valor_original,
                'vencimento_atualizado': vencimento_atualizado,
                'valor_atualizado': valor_atualizado,
                'situacao': situacao})
        self.result.update({"debitos_de_ipva_em_aberto": debitos_de_ipva_em_aberto})

        divida_ativa_do_renavam = []
        divida_ativa_do_renavam_rows = response.selector.xpath(
            "//legend[contains(text(),'Divida Ativa do Renavam')]/following::table[1]//tbody/tr[./td]")
        for row in divida_ativa_do_renavam_rows:
            no_da_guia = regex.sub(" ", row.xpath("./td[1]/text()").get("").strip())
            no_parcela = regex.sub(" ", row.xpath("./td[2]/text()").get("").strip())
            receita = regex.sub(" ", row.xpath("./td[3]/text()").get("").strip())
            vencimento_original = regex.sub(" ", row.xpath("./td[4]/text()").get("").strip())
            valor_original = regex.sub(" ", row.xpath("./td[5]/text()").get("").strip())
            vencimento_atualizado = regex.sub(" ", row.xpath("./td[6]/text()").get("").strip())
            valor_atualizado = regex.sub(" ", row.xpath("./td[7]/text()").get("").strip())
            situacao = regex.sub(" ", row.xpath("./td[8]/text()").get("").strip())
            divida_ativa_do_renavam.append({
                'no_da_guia': no_da_guia,
                'no_parcela': no_parcela,
                'receita': receita,
                'vencimento_original': vencimento_original,
                'valor_original': valor_original,
                'vencimento_atualizado': vencimento_atualizado,
                'valor_atualizado': valor_atualizado,
                'situacao': situacao})
        self.result.update({"divida_ativa_do_renavam": divida_ativa_do_renavam})

        if self.get_files:
            for item in debitos_de_ipva_em_aberto:
                pdf_url = "https://portalcontribuinte.sefin.ro.gov.br/Publico/Dare.jsp?NuLancamento={no_da_guia}&NuParcela={no_parcela}&DataPagto={vencimento_atualizado}"
                pdf_url = pdf_url.format(**item)
                print(pdf_url)
                yield SplashRequest(pdf_url, callback=self.print_html_to_pdf,
                                    endpoint='render.json', args=self.splash_args,
                                    meta={'result_key': 'debitos_de_ipva_em_aberto',
                                          'file_data': item}, dont_filter=True)
            for item in divida_ativa_do_renavam:
                pdf_url = "https://portalcontribuinte.sefin.ro.gov.br/Publico/Dare.jsp?NuLancamento={no_da_guia}&NuParcela={no_parcela}&DataPagto={vencimento_atualizado}"
                pdf_url = pdf_url.format(**item)
                print(pdf_url)
                yield SplashRequest(pdf_url, callback=self.print_html_to_pdf,
                                    endpoint='render.json', args=self.splash_args,
                                    meta={'result_key': 'divida_ativa_do_renavam',
                                          'file_data': item}, dont_filter=True)

    def print_html_to_pdf(self, response):
        """Function to print HTML to PDF, save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = response.meta['result_key']
        file_data = response.meta.get('file_data')
        file_type = "__boleto__"

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        try:
            options = {
                'page-size': 'A4',
                'encoding': "UTF-8"
            }
            html_text = response.body.decode("ascii", "ignore")
            match = re.search("\s+(<.+js/barcode.+>)", html_text)
            if match:
                match = match.group(1)
                html_text = html_text.replace(match, "\n")
            if file_data:
                html_text = html_text.replace(
                        "/images/", "https://portalcontribuinte.sefin.ro.gov.br/images/"
                    ).replace(
                        "/BarCodeServlet", "https://portalcontribuinte.sefin.ro.gov.br/BarCodeServlet"
                    ).replace(
                        "/js/", "https://portalcontribuinte.sefin.ro.gov.br/js/")
            else:
                html_text = html_text.replace(
                        "img/", "https://consulta.detran.ro.gov.br/CentralDeConsultasInternet/Internet/img/"
                    ).replace(
                        "css/", "https://consulta.detran.ro.gov.br/CentralDeConsultasInternet/Internet/css/"
                    ).replace(
                        "js/", "https://consulta.detran.ro.gov.br/CentralDeConsultasInternet/Internet/js/")
            pdfkit.from_string(html_text, file_path, options=options)

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

        except Exception as exc:
            error_msg = {"error_type": "FILE_NOT_SAVED", "file": filename, "details": str(exc)}
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
