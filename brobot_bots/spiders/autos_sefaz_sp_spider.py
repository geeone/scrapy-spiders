# -*- coding: utf-8 -*-

import os
import re
import sys

from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script_10_sec_wait
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_sefaz_sp_spider(CustomSpider):
    # required scraper name
    name = "autos_sefaz_sp"

    # initial urls
    start_url = 'https://www.ipva.fazenda.sp.gov.br/IPVANET_Consulta/Consulta.aspx'

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_sefaz_sp_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        yield SplashRequest(self.start_url,
                            callback=self.get_login_page,
                            errback=self.errback_func,
                            endpoint='execute',
                            cache_args=['lua_source'],
                            args={'lua_source': script_10_sec_wait}, dont_filter=True)

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

    def get_login_page(self, response):
        """Function to get request options to login.
        Used to get ReCaptcha token; image captcha value."""

        # get the Captcha's options
        sitekey = response.selector.xpath(
            "//div[@class='g-recaptcha']/@data-sitekey").get("")

        gcaptcha_txt = self.solve_captcha(sitekey, response.url)
        if not gcaptcha_txt:
            return

        # Get options for request
        EVENTTARGET = response.selector.xpath(
            "//input[@id='__EVENTTARGET']/@value").get("")
        EVENTARGUMENT = response.selector.xpath(
            "//input[@id='__EVENTARGUMENT']/@value").get("")
        VIEWSTATE = response.selector.xpath(
            "//input[@id='__VIEWSTATE']/@value").get("")
        VIEWSTATEGENERATOR = response.selector.xpath(
            "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
        EVENTVALIDATION = response.selector.xpath(
            "//input[@id='__EVENTVALIDATION']/@value").get("")

        frm_data = {
            '__EVENTTARGET': EVENTTARGET,
            '__EVENTARGUMENT': EVENTARGUMENT,
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': EVENTVALIDATION,
            'ctl00$conteudoPaginaPlaceHolder$txtRenavam': self.renavam,
            'ctl00$conteudoPaginaPlaceHolder$txtPlaca': self.placa,
            'g-recaptcha-response': gcaptcha_txt,
            'ctl00$conteudoPaginaPlaceHolder$btn_Consultar': 'Consultar'}

        yield SplashFormRequest(self.start_url, formdata=frm_data,
                                callback=self.login_me,
                                errback=self.errback_func,
                                endpoint='execute',
                                cache_args=['lua_source'],
                                args={'lua_source': script_10_sec_wait,
                                      'cookies': response.data['cookies']}, dont_filter=True)

    def login_me(self, response):
        renavam = response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtRenavam']/text()").get("").strip()
        placa = response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtPlaca']/text()").get("").strip()
        print("renavam:", renavam)
        print("placa:", placa)

        error_message = response.selector.xpath("//span[@id='conteudoPaginaPlaceHolder_lblErro']/text()").get("")
        if "Preencha o campo 'Placa' corretamente." in error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return
        elif "Favor validar o captcha corretamente" in error_message:
            self.incorrect_captcha_report(
                self.captcha_service, self.g_recaptcha_id)
            if self.incorrect_captcha_retries > 0:
                yield SplashRequest(self.start_url,
                                    callback=self.get_login_page,
                                    errback=self.errback_func,
                                    endpoint='execute', cache_args=['lua_source'],
                                    args={'lua_source': script_10_sec_wait}, dont_filter=True)
            return

        # create screenshot using imgkit
        if self.capture_screenshot:
            self.take_screenshot(response, url_path='IPVANET_Consulta')

        regex = re.compile(r'\s+')
        marca_modelo = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtMarcaModelo']/text()").get("").strip())
        faixa_do_ipva = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtFaixaIPVA']/text()").get("").strip())
        ano_de_fabricacao = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtAnoFabric']/text()").get("").strip())
        municipio = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtMunicipio']/text()").get("").strip())
        combustivel = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtCombustivel']/text()").get("").strip())
        especie = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtEspecie']/text()").get("").strip())
        categoria = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtCategoria']/text()").get("").strip())
        tipo = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtTipo']/text()").get("").strip())
        passageiros = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtPassageiros']/text()").get("").strip())
        carroceria = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtCarroceria']/text()").get("").strip())
        ultimo_licenciamento = regex.sub(" ", response.selector.xpath(
            "//span[@id='conteudoPaginaPlaceHolder_txtAnoUltLicen']/text()").get("").strip())
        self.result.update({
            'renavam': renavam,
            'placa': placa,
            'marca_modelo': marca_modelo,
            'faixa_do_ipva': faixa_do_ipva,
            'ano_de_fabricacao': ano_de_fabricacao,
            'municipio': municipio,
            'combustivel': combustivel,
            'especie': especie,
            'categoria': categoria,
            'tipo': tipo,
            'passageiros': passageiros,
            'carroceria': carroceria,
            'ultimo_licenciamento': ultimo_licenciamento
            })

        tables = response.selector.xpath("//div[@id='conteudoPaginaPlaceHolder_Panel1']/table[.//td[@class='alinharEsquerda, negrito' and ./span[contains(@id,'conteudoPaginaPlaceHolder_Label') and not(text()=' ')]]]")
        for table in tables[1:-1]:
            table_name = " ".join([t.strip() for t in table.xpath(".//td[@class='alinharEsquerda, negrito' and ./span[contains(@id,'conteudoPaginaPlaceHolder_Label') and not(text()=' ')]]/span/text()").extract()])
            table_name = self.remove_diacritics(table_name)

            main_table = table.xpath("./following::table[@class='loginTable' and .//tr[not(@class) and ./td[not(@class)]/span]][1]")
            rows = main_table.xpath(".//tr[not(@class)]")

            if table_name == "ipva_2020":
                table_content = {}
                for row in rows:
                    title = self.remove_diacritics(row.xpath("./td[1]/span/text()").get("").strip())
                    value = re.sub('\s+', " ", " ".join(
                        row.xpath("./td[last()]/span/text()").extract()).strip())
                    if title:
                        table_content.update({title: value})
                self.result.update({table_name: table_content})

                second_table = main_table.xpath("./following::table[@class='loginTable'][1]//tr[1]/td/span")
                st_name = self.remove_diacritics(second_table.xpath("./text()").get("").strip())

                st_rows = second_table.xpath("./following::table[@class='loginTable'][1]//tr")
                table_content = {}
                for row in st_rows[1:]:
                    title = row.xpath("./td[1]/span/text()").get("").strip()
                    date = row.xpath("./td[2]/span/text()").get("").strip()
                    value = re.sub('\s+', " ", " ".join(
                        row.xpath("./td[last()]/span/text()").extract()).strip())
                    if title and date:
                        table_content.update({'modalidades_disponiveis': title,
                                              'vencimento': date,
                                              'valor': value})
                if table_content:
                    self.result.update({st_name: table_content})

            elif table_name != "ipva_2020" and table_name != 'taxas':
                table_content = []
                for row in rows[1:]:
                    exercicio = row.xpath("./td[1]/span/text()").get("").strip()
                    valor = row.xpath("./td[last()]/span/text()").get("").strip()
                    is_valor_table = rows[0].xpath(".//span[contains(text(),'Valor')]")
                    if exercicio:
                        rows_data = {'exercicio': exercicio}
                        if is_valor_table:
                            rows_data.update({'valor': valor})
                        table_content.append(rows_data)
                self.result.update({table_name: table_content})

            elif table_name == "taxas":
                table_content = {}
                taxas_type = rows[0].xpath("./td[1]/span/text()").get("").strip()
                table_content.update({'type': taxas_type})
                # workaround for different types
                is_nada_costa = rows[1].xpath(".//span[contains(text(),'NADA CONSTA')]")
                if is_nada_costa:
                    i = 1
                else:
                    i = 2
                for row in rows[i:]:
                    title = self.remove_diacritics(row.xpath("./td[1]/span/text()").get("").strip())
                    value = row.xpath("./td[last()]/span/text()").get("").strip()
                    if title:
                        table_content.update({title: value})
                self.result.update({table_name: table_content})

        multas_btn = response.selector.xpath("//input[contains(@id,'Multas')]")
        if multas_btn:
            # Get options for request
            multas_btn_name = multas_btn.xpath("./@name").get("").strip()
            multas_btn_value = multas_btn.xpath("./@value").get("").strip()
            EVENTTARGET = response.selector.xpath(
                "//input[@id='__EVENTTARGET']/@value").get("")
            EVENTARGUMENT = response.selector.xpath(
                "//input[@id='__EVENTARGUMENT']/@value").get("")
            VIEWSTATE = response.selector.xpath(
                "//input[@id='__VIEWSTATE']/@value").get("")
            VIEWSTATEGENERATOR = response.selector.xpath(
                "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
            EVENTVALIDATION = response.selector.xpath(
                "//input[@id='__EVENTVALIDATION']/@value").get("")

            frm_data = {
                '__EVENTTARGET': EVENTTARGET,
                '__EVENTARGUMENT': EVENTARGUMENT,
                '__VIEWSTATE': VIEWSTATE,
                '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                '__EVENTVALIDATION': EVENTVALIDATION}
            frm_data.update({multas_btn_name: multas_btn_value})

            multas_url = "https://www.ipva.fazenda.sp.gov.br/IPVANET_Consulta/Pages/Aviso.aspx"
            yield SplashFormRequest(multas_url, formdata=frm_data,
                                    callback=self.multas_table,
                                    errback=self.errback_func,
                                    endpoint='execute',
                                    cache_args=['lua_source'],
                                    args={'lua_source': script_10_sec_wait,
                                          'cookies': response.data['cookies']}, dont_filter=True)
        else:
            self.result['multas'] = []

    def redirect_me_to_multas(self, response):
        multas_url = "https://www.ipva.fazenda.sp.gov.br/IPVANET_Consulta/Pages/AITdetalhe.aspx"
        yield SplashRequest(multas_url, callback=self.multas_table,
                            errback=self.errback_func,
                            args={'wait': 10}, dont_filter=True)

    def multas_table(self, response):
        multas_table = response.selector.xpath(".//div[@id='conteudoPaginaPlaceHolder_pnlMultasDet']")
        infracao_list = multas_table.xpath(".//span[contains(text(),'Infração')]/following::span[1]")
        municipio_list = multas_table.xpath(".//span[contains(text(),'Município')]/following::span[1]")
        local_list = multas_table.xpath(".//span[contains(text(),'Local')]/following::span[1]")
        data_hora_list = multas_table.xpath(".//span[contains(text(),'Data/Hora')]/following::span[1]")
        ait_list = multas_table.xpath(".//span[contains(text(),'Nº do A.I.T.')]/following::span[1]")
        guia_list = multas_table.xpath(".//span[contains(text(),'Nº da Guia')]/following::span[1]")
        receita_list = multas_table.xpath(".//span[contains(text(),'Receita')]/following::span[1]")
        vencimento_list = multas_table.xpath(".//span[contains(text(),'Vencimento')]/following::span[1]")
        valor_list = multas_table.xpath(".//span[contains(text(),'Valor')]/following::span[1]")

        multas = []
        regex = re.compile(r'\s+')
        for i in range(len(infracao_list)):
            infracao = regex.sub(" ", infracao_list[i].xpath("./text()").get("").strip())
            municipio = regex.sub(" ", municipio_list[i].xpath("./text()").get("").strip())
            local = regex.sub(" ", local_list[i].xpath("./text()").get("").strip())
            data_hora = regex.sub(" ", data_hora_list[i].xpath("./text()").get("").strip())
            ait = regex.sub(" ", ait_list[i].xpath("./text()").get("").strip())
            guia = regex.sub(" ", guia_list[i].xpath("./text()").get("").strip())
            receita = regex.sub(" ", receita_list[i].xpath("./text()").get("").strip())
            vencimento = regex.sub(" ", vencimento_list[i].xpath("./text()").get("").strip())
            valor = regex.sub(" ", valor_list[i].xpath("./text()").get("").strip())
            multas.append({'infracao': infracao,
                           'municipio': municipio,
                           'local': local,
                           'data_hora': data_hora,
                           'nº_do_a_i_t': ait,
                           'nº_da_guia': guia,
                           'receita': receita,
                           'vencimento': vencimento,
                           'valor': valor})
        self.result.update({'multas': multas})

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
