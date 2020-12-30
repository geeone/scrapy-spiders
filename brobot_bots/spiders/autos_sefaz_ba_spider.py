# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import os
import re
import sys
import time
import urllib.parse
import uuid

import pdfkit
from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


def _click_on_invisible_element(driver, element):
    driver.execute_script("(arguments[0]).click();", element)


def _check_by_xpath(driver, xpath):
    try:
        t = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, xpath)))
        return t
    except NoSuchElementException:
        return None


class autos_sefaz_ba_spider(CustomSpider):
    # required scraper name
    name = "autos_sefaz_ba"

    # initial urls
    start_url = 'http://www.sefaz.ba.gov.br/scripts/ipva/dae/veiculocadastrado/avulso_ipva.asp?receita=0644'

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_sefaz_ba_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        self.regex = re.compile(r'\s+')
        self.exercicio_notificado = False

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        #yield Request(self.start_url, callback=self.login_me,
        #              errback=self.errback_func, dont_filter=True)
        yield SplashRequest(self.start_url,
                            callback=self.login_me,
                            errback=self.errback_func,
                            endpoint='execute', cache_args=['lua_source'],
                            args={'lua_source': script}, dont_filter=True)

    def login_me(self, response):
        url = 'http://www.sefaz.ba.gov.br/scripts/ipva/dae/VeiculoCadastrado/ipva_texto_obter_desconto200.asp'
        frm_data = {'txt_renavam': self.renavam, 'txt_renavam1': ''}
        #yield FormRequest(url, formdata=frm_data, callback=self.get_main_page,
        #                  errback=self.errback_func, dont_filter=True)
        yield SplashFormRequest(url, formdata=frm_data,
                                callback=self.get_main_page,
                                errback=self.errback_func,
                                endpoint='execute', cache_args=['lua_source'],
                                args={'lua_source': script,
                                      'cookies': response.data['cookies']}, dont_filter=True)

    def get_main_page(self, response):

        if self.capture_screenshot:
            self.take_screenshot(response)

        error_message = response.selector.xpath(
            "//b[contains(text(),'Renavam fornecido não cadastrado']/text()").get("").strip()
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return
        sem_debitos = response.selector.xpath(
            "//b[contains(text(),'Não há débito para o código do renavam informado')]/text()").get("").strip()
        if sem_debitos:
            self.result['sem_debitos'] = sem_debitos
            return

        placa = response.selector.xpath("//b[contains(.,'veículo de placa')]/../font/text()").get("").strip()
        modelo_do_veiculo = response.selector.xpath("//b[contains(.,'Modelo do veículo')]/../font/text()").get("").strip()
        self.result.update({
            'placa': placa,
            'modelo_do_veiculo': modelo_do_veiculo})
        print(placa)

        table_rows = response.selector.xpath(
            "//div[@align='center']/table[@class='conteudo' and .//b[contains(text(),'Ano Exercicio')]]//tr[not(@bgcolor)]")
        for row in table_rows:
            valor_real = row.xpath(".//td[2]//b/text()").get("").strip()
            if "exercicio notificado" in valor_real:
                self.exercicio_notificado = True
            link = row.xpath(".//td[1]/a/text()").get("").strip()
            if link:
                url = "http://www.sefaz.ba.gov.br/scripts/ipva/dae/VeiculoCadastrado/result_debito_ipva.asp?ano_exercicio={}".format(link)
                #yield Request(url, callback=self.get_result_debito_ipva,
                #              errback=self.errback_func, dont_filter=True)
                yield SplashRequest(url, callback=self.get_result_debito_ipva,
                                    errback=self.errback_func,
                                    endpoint='execute', cache_args=['lua_source'],
                                    args={'lua_source': script,
                                          'cookies': response.data['cookies']}, dont_filter=True)

        sistemas_url = response.selector.xpath("//a[contains(text(),'aqui')]/@href").get("")
        if self.exercicio_notificado:
            yield self.get_listagem_de_paf(sistemas_url)

    def create_chrome_session(self, url):
        download_dir = os.path.join(path, "downloads", self.scrape_id, "temp")
        self.create_folder(download_dir)
        print("Downloads directory:", download_dir)
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--disable-gpu")
        # chrome_options.add_argument("--headless")

        settings = {
               "recentDestinations": [{
                    "id": "Save as PDF",
                    "origin": "local",
                    "account": "",
                }],
                "selectedDestinationId": "Save as PDF",
                "version": 2
            }
        prefs = {'printing.print_preview_sticky_settings.appState': json.dumps(settings),
                 'savefile.default_directory': download_dir}
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_argument('--kiosk-printing')
        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.get(url)
        return driver

    def take_selenium_screenshot(self, driver):
        def body_size(x): return driver.execute_script(
            'return document.body.parentNode.scroll' + x)
        driver.set_window_size(body_size('Width'), body_size('Height'))
        body_elem = _check_by_xpath(driver, "//body")
        png_screenshot = body_elem.screenshot_as_png

        file_id = str(uuid.uuid4())
        filename = "{file_id}.png".format(file_id=file_id)
        file_path = os.path.join(
            path, "screenshots", self.scrape_id, filename)
        with open(file_path, "wb") as png_file:
            png_file.write(png_screenshot)

    def get_listagem_de_paf(self, url):
        try:
            driver = self.create_chrome_session(url)
            renavam = _check_by_xpath(driver, "//input[@id='_ctl1__ctl1_numero_renavam']")
            renavam.send_keys(self.renavam)
            apply_filter = _check_by_xpath(driver, "//input[@id='_ctl1__ctl1_Filtrar']")
            apply_filter.click()
            self.take_selenium_screenshot(driver)

            rows = driver.find_elements_by_xpath(
                "//table[@id='_ctl1__ctl1_SimpleUniDataGrid']//tr[@class='EstiloItemDatagrid']")
            listagem_de_paf = []
            for row in rows:
                numero_a = _check_by_xpath(row, "./td[2]//a")
                if numero_a:
                    numero = numero_a.text.strip()
                    fluxo = _check_by_xpath(row, "./td[3]").text.strip()
                    razao_social = _check_by_xpath(row, "./td[4]").text.strip()
                    i_estadual = _check_by_xpath(row, "./td[5]").text.strip()
                    cnpj_cpf = _check_by_xpath(row, "./td[6]").text.strip()
                    fase = _check_by_xpath(row, "./td[7]").text.strip()
                    situacao = _check_by_xpath(row, "./td[8]").text.strip()
                    saldo = _check_by_xpath(row, "./td[9]").text.strip()
                    row_data = {
                        'numero': numero,
                        'fluxo': fluxo,
                        'razao_social': razao_social,
                        'i_estadual': i_estadual,
                        'cnpj_cpf': cnpj_cpf,
                        'fase': fase,
                        'situacao': situacao,
                        'saldo': saldo}
                    print(row_data)
                    listagem_de_paf.append(row_data)
                    # go next page
                    numero_a.click()
                    rows = driver.find_elements_by_xpath(
                        "//table[@id='Table1']/following::table[.//*[contains(text(),'DADOS DO PAF')]]/following::table[1]//tr[./td[@class='EstiloColunaDadoFundo']]")
                    detalhamento = {}
                    for row in rows:
                        name = self.remove_diacritics(
                            _check_by_xpath(row, "./td[1]").text.strip())
                        value = _check_by_xpath(row, "./td[2]").text.strip()
                        detalhamento.update({name: value})
                    print("detalhamento:", detalhamento)

                    if self.get_files:
                        print_pagomento = _check_by_xpath(driver, "//a[normalize-space()='Efetuar Pagamento']")
                        if print_pagomento:
                            print_pagomento.click()
                            avancar_btn = _check_by_xpath(driver, "//input[@id='_ctl1__ctl1_btnAvancar']")
                            if avancar_btn:
                                avancar_btn.click()
                                emitir_btn = _check_by_xpath(driver, "//a[normalize-space()='Emitir DAE']")
                                if emitir_btn:
                                    emitir_btn.click()
                                    time.sleep(10)
                                    self.save_page_as_pdf(row_data)
                    voltar = _check_by_xpath(driver, "//a[@title='Voltar']")
                    if voltar:
                        voltar.click()
                        renavam = _check_by_xpath(driver, "//input[@id='_ctl1__ctl1_numero_renavam']")
                        renavam.send_keys(self.renavam)
                        apply_filter = _check_by_xpath(driver, "//input[@id='_ctl1__ctl1_Filtrar']")
                        apply_filter.click()
                        rows = driver.find_elements_by_xpath(
                            "//table[@id='_ctl1__ctl1_SimpleUniDataGrid']//tr[@class='EstiloItemDatagrid']")
            if not self.get_files:
                self.result.update({'listagem_de_paf': listagem_de_paf})
        except Exception as exc:
            print("Browser crashed:", exc)
            error_msg = {"error_type": "BROWSER_CRASHED", "details": str(exc)}
            self.errors.append(error_msg)
            self.logger.error(error_msg)
        finally:
            temp_dir = os.path.join(path, "downloads", self.scrape_id, "temp")
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
            driver.close()
            driver.quit()

    def save_page_as_pdf(self, row_data):
        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        try:
            temp_dir = os.path.join(path, "downloads", self.scrape_id, "temp")
            for filename in os.listdir(temp_dir):
                print(filename)
                if filename.endswith(".pdf"):
                    os.replace(os.path.join(temp_dir, filename), file_path)
            self.upload_file(file_id)
            # update values in result
            listagem_de_paf = self.result.get("listagem_de_paf", [])
            row_data.update({
                "__boleto__": {
                    "file_id": file_id}
                })
            listagem_de_paf.append(row_data)
            self.result.update({'listagem_de_paf': listagem_de_paf})
        except Exception as exc:
            error_msg = {"error_type": "FILE_NOT_SAVED", "file": filename, "details": str(exc)}
            self.errors.append(error_msg)
            self.logger.error(error_msg)

    def get_result_debito_ipva(self, response):
        cota3 = response.url.split("cota3=")[-1]
        url = "http://www.sefaz.ba.gov.br/scripts/ipva/dae/VeiculoCadastrado/debito_calculado_ipva.asp?cota3={}".format(cota3)
        today = dt.now().strftime("%d/%m/%Y")
        frm_data = {'txt_dtc_pagamento': today}
        #yield FormRequest(url, formdata=frm_data, callback=self.get_debito_calculado_ipva,
        #                  errback=self.errback_func, dont_filter=True)
        yield SplashFormRequest(url, formdata=frm_data,
                                callback=self.get_debito_calculado_ipva,
                                errback=self.errback_func,
                                endpoint='execute', cache_args=['lua_source'],
                                args={'lua_source': script,
                                      'cookies': response.data['cookies']}, dont_filter=True)

    def get_debito_calculado_ipva(self, response):
        valor_do_ipva = response.selector.xpath("//font[contains(.,'Pagamento de cota')]/text()").get("").strip()
        ano_exercicio = response.selector.xpath("//span[contains(.,'Ano Exercício')]/../input/@value").get("").strip()
        data_do_vencimento = response.selector.xpath("//span[contains(.,'Data do Vencimento')]/../input/@value").get("").strip()
        valor_da_cota_unica = response.selector.xpath("//span[contains(.,'Valor da Cota única')]/../input/@value").get("").strip()
        print(valor_da_cota_unica)
        if valor_da_cota_unica:
            row_data = {
                'valor_do_ipva': valor_do_ipva,
                'ano_exercicio': ano_exercicio,
                'data_do_vencimento': data_do_vencimento,
                'valor_da_cota_unica': valor_da_cota_unica}

            if self.get_files:
                url = "http://www.sefaz.ba.gov.br/scripts/ipva/dae/VeiculoCadastrado/result_dae_avulso_ipva.asp"
                frm_data = {'Lnum_cnpj_cpf_base': '', 'Lnum_cnpj_cpf_filial': '', 'Lnum_cnpj_cpf_digito': ''}
                #yield FormRequest(url, callback=self.test_file,
                #                  errback=self.errback_func, dont_filter=True)
                yield SplashFormRequest(url, formdata=frm_data,
                                        callback=self.print_html_to_pdf,
                                        #errback=self.errback_func,
                                        endpoint='execute', cache_args=['lua_source'],
                                        args={'lua_source': script,
                                              'cookies': response.data['cookies']},
                                        meta={'row_data': row_data}, dont_filter=True)
            else:
                ipva_do_veiculo = self.result.get('ipva_do_veiculo', [])
                ipva_do_veiculo.append(row_data)
                self.result.update({'ipva_do_veiculo': ipva_do_veiculo})

    def print_html_to_pdf(self, response):
        row_data = response.meta['row_data']
        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        try:
            options = {
                'page-size': 'A4',
                'encoding': "UTF-8"
            }
            html_text = self.decode_response_to_utf8(response, "utf-8")
            url_info = urllib.parse.urlsplit(response.url)
            domain = "{}://{}".format(url_info.scheme, url_info.netloc)
            html_text = re.sub(
                r"(<(LINK|link|img|script|a)\s+(?:[^>]*?\s+)?(src|href)=)(\"|\')\.{0,2}(/" +
                r")?(/.*?)\4", r'\1\4{}\6\4'.format(domain), html_text)
            pdfkit.from_string(html_text, file_path, options=options)
            # upload pdf to s3 and call the webhook
            self.upload_file(file_id)
            # update values in result
            ipva_do_veiculo = self.result.get('ipva_do_veiculo', [])
            row_data.update({
                "__boleto__": {
                    "file_id": file_id}
                })
            ipva_do_veiculo.append(row_data)
            self.result.update({'ipva_do_veiculo': ipva_do_veiculo})
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
