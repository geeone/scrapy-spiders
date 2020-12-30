# -*- coding: utf-8 -*-

import base64
from datetime import datetime as dt
from http.cookies import SimpleCookie
import os
import sys
import uuid
import re

import pdfkit
import requests
from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class autos_prefeitura_sp_spider(CustomSpider):
    # required scraper name
    name = "autos_prefeitura_sp"

    # initial urls
    start_url = 'https://meuveiculo.prefeitura.sp.gov.br/forms/frmPesquisarRenavam.aspx'

    urls = {'traffic_fines': "https://meuveiculo.prefeitura.sp.gov.br/forms/frmResumoMultas.aspx",
            'AIT': "https://meuveiculo.prefeitura.sp.gov.br/forms/frmConsultaAutosInfracaoTransito.aspx"}

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_prefeitura_sp_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        yield Request(self.start_url, callback=self.get_login_page,
                      errback=self.errback_func, dont_filter=True)

    def solve_captcha(self, sitekey, captcha_url, **kwargs):
        """Function to solve Captcha's."""

        try:
            # set default value
            imgcaptcha_txt, gcaptcha_txt = None, None
            # SOLVE IMAGE CAPTCHA
            attempts = 0
            while 1:
                # check attempts count to avoid cycled solving
                if attempts < self.captcha_retries:
                    attempts += 1
                    self.img_captcha_id, imgcaptcha_txt = self.captcha_solver(
                        "2Captcha", captcha_img="captcha.jpg")
                    if imgcaptcha_txt and (" " not in imgcaptcha_txt):  # check for two words (DBC issue)
                        print("IMAGE Captcha:", imgcaptcha_txt)
                        break
                else:
                    break
            # SOLVE RECAPTCHA THEN
            attempts = 0
            while 1:
                # check attempts count to avoid cycled solving
                if attempts < self.captcha_retries:
                    attempts += 1
                    self.g_recaptcha_id, gcaptcha_txt = self.captcha_solver(
                        self.captcha_service,
                        sitekey=sitekey, captcha_url=captcha_url)
                    if gcaptcha_txt:
                        print("ReCaptcha:", gcaptcha_txt)
                        break
                else:
                    break
            # check if captcha was solved
            if not imgcaptcha_txt or not gcaptcha_txt:
                details_msg = "Failed to solve captcha for {} times.".format(self.captcha_retries)
                error_msg = {"error_type": "CAPTCHA_NOT_SOLVED",
                             "captcha_service": self.captcha_service, "details": details_msg}
                raise Exception(error_msg)
        except Exception as exc:
            error_msg = exc.args[0]
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
        finally:
            return imgcaptcha_txt, gcaptcha_txt

    def get_login_page(self, response):
        """Function to get request options to login.
        Used to get ReCaptcha token; image captcha value."""

        # get the Captcha's options
        sitekey = response.selector.xpath(
            "//div[@class='g-recaptcha']/@data-sitekey").get("")
        imgcaptcha = response.selector.xpath(
            "//img[@id='imgCaptcha']/@src").get("")
        img_url = "https://meuveiculo.prefeitura.sp.gov.br" + \
            imgcaptcha.replace("..", "")

        # get cookies to download captcha image
        cookies = response.headers.getlist('Set-Cookie')
        c = SimpleCookie()
        for cookie in cookies:
            c.load(cookie.decode("utf-8"))
        cookies_list = [{"name": key, "value": c[key].value} for key in c]

        # set cookies to current session
        session = requests.Session()
        for cookie in cookies_list:
            # print(cookie)
            session.cookies.set(**cookie)

        # save captcha image
        r = session.get(img_url, stream=True)
        with open("captcha.jpg", 'wb') as f:
            f.write(r.content)

        imgcaptcha_txt, gcaptcha_txt = self.solve_captcha(
            sitekey, response.url)
        if not imgcaptcha_txt or not gcaptcha_txt:
            return

        # Get options for request
        EVENTTARGET = response.selector.xpath(
            "//input[@id='__EVENTTARGET']/@value").get("")
        EVENTARGUMENT = response.selector.xpath(
            "//input[@id='__EVENTARGUMENT']/@value").get("")
        LASTFOCUS = response.selector.xpath(
            "//input[@id='__LASTFOCUS']/@value").get("")
        PageProdamSPOnChange = response.selector.xpath(
            "//input[@id='PageProdamSPOnChange']/@value").get("")
        PageProdamSPPosicao = response.selector.xpath(
            "//input[@id='PageProdamSPPosicao']/@value").get("")
        PageProdamSPFocado = response.selector.xpath(
            "//input[@id='PageProdamSPFocado']/@value").get("")
        VIEWSTATE = response.selector.xpath(
            "//input[@id='__VIEWSTATE']/@value").get("")
        VIEWSTATEGENERATOR = response.selector.xpath(
            "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
        EVENTVALIDATION = response.selector.xpath(
            "//input[@id='__EVENTVALIDATION']/@value").get("")
        tpAudio = response.selector.xpath(
            "//input[@id='__tpAudio']/@value").get("")
        strVal = response.selector.xpath(
            "//input[@id='__strVal']/@value").get("")

        frm_data = {
            '__EVENTTARGET': EVENTTARGET,
            '__EVENTARGUMENT': EVENTARGUMENT,
            '__LASTFOCUS': LASTFOCUS,
            'PageProdamSPOnChange': PageProdamSPOnChange,
            'PageProdamSPPosicao': PageProdamSPPosicao,
            'PageProdamSPFocado': PageProdamSPFocado,
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': EVENTVALIDATION,
            'txtRenavam': self.renavam,
            'txtplaca': self.placa,
            '__tpAudio': tpAudio,
            '__strVal': strVal,
            'txtValidacao': imgcaptcha_txt,
            'g-recaptcha-response': gcaptcha_txt,
            'btnMultas': 'Consultar'}

        login_url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmPesquisarRenavam.aspx"
        yield FormRequest(url=login_url, formdata=frm_data,
                          callback=self.login_me,
                          errback=self.errback_func, dont_filter=True)

    def login_me(self, response):
        """Redirect to main page."""

        error_message = response.selector.xpath("//span[@id='lblMensagem']/text()").get("")
        if "Placa/Renavan não localizados" in error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return
        elif "Código de imagem inválido. Favor digitá-lo corretamente." in error_message:
            self.incorrect_captcha_report(
                '2Captcha', self.img_captcha_id)
        elif "Favor confirmar reCAPTCHA" in error_message:
            self.incorrect_captcha_report(
                self.captcha_service, self.g_recaptcha_id)

        url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmConsultarMultas.aspx"
        # yield SplashRequest(url, callback=self.redirect_me,
        #                     endpoint='render.json', args=self.splash_args, dont_filter=True)
        yield Request(url, callback=self.redirect_me,
                      errback=self.errback_func, dont_filter=True)

    def redirect_me(self, response):
        """Function to check if captcha solved correctly
        If there're no errors then go to URLS."""

        renavam = response.selector.xpath(
            "//span[@id='lblRenavam']/text()").get("").strip()
        placa = response.selector.xpath(
            "//span[@id='lblPlaca']/text()").get("").strip()
        print("renavam:", renavam)
        print("placa:", placa)

        # create screenshot using SPLASH response
        # imgdata = base64.b64decode(response.data['png'])
        # with open(file_path, 'wb') as f:
        #     f.write(imgdata)

        # check if captcha solved correctly, retry otherwise
        if not placa:
            if self.incorrect_captcha_retries > 0:
                self.incorrect_captcha_retries -= 1
                yield Request(self.start_url, callback=self.get_login_page,
                              meta={'dont_merge_cookies': True}, dont_filter=True)
            return

        # go to URLS
        for key, url in self.urls.items():
            if key == "traffic_fines":
                yield Request(url, callback=self.traffic_fines_main,
                              errback=self.errback_func, dont_filter=True)
            elif key == "AIT":
                yield Request(url, callback=self.ait_main,
                              errback=self.errback_func, dont_filter=True)

    def traffic_fines_main(self, response):
        """Summary fines page
        Check if there are records with qtde > 0; Click on them to get the details."""

        # create screenshot using imgkit
        if self.capture_screenshot:
            self.take_screenshot(response, encoding='latin-1')

        # Get options for request
        PageProdamSPOnChange = response.selector.xpath(
            "//input[@id='PageProdamSPOnChange']/@value").get("")
        PageProdamSPPosicao = response.selector.xpath(
            "//input[@id='PageProdamSPPosicao']/@value").get("")
        PageProdamSPFocado = response.selector.xpath(
            "//input[@id='PageProdamSPFocado']/@value").get("")
        VIEWSTATE = response.selector.xpath(
            "//input[@id='__VIEWSTATE']/@value").get("")
        VIEWSTATEGENERATOR = response.selector.xpath(
            "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
        EVENTVALIDATION = response.selector.xpath(
            "//input[@id='__EVENTVALIDATION']/@value").get("")
        txthvalor_total = response.selector.xpath(
            "//input[@id='txthvalor_total']/@value").get("")
        txthqtd_total = response.selector.xpath(
            "//input[@id='txthqtd_total']/@value").get("")

        frm_data = {
            'PageProdamSPOnChange': PageProdamSPOnChange,
            'PageProdamSPPosicao': PageProdamSPPosicao,
            'PageProdamSPFocado': PageProdamSPFocado,
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': EVENTVALIDATION,
            'txthvalor_total': txthvalor_total,
            'txthqtd_total': txthqtd_total}

        frm_url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmResumoMultas.aspx"

        rows = response.selector.xpath(
            "//table[@id='grdDados']//tr[@class and .//input]")
        for row in rows:
            qtde = row.xpath(".//td[2]/text()").get("")
            if int(qtde) > 0:
                input_btn = row.xpath(".//td[4]/input")
                if input_btn:
                    btn_id = input_btn.xpath("./@id").get("")
                    btn_name = input_btn.xpath("./@name").get("")
                    btn_value = input_btn.xpath("./@value").get("")
                    frm_data_copy = frm_data.copy()
                    frm_data_copy.update(
                        {btn_name: btn_value, "PageProdamSPFocado": btn_id})
                    yield FormRequest(url=frm_url, formdata=frm_data_copy,
                                      callback=self.traffic_fines_details,
                                      errback=self.errback_func, dont_filter=True)

    def traffic_fines_details(self, response):
        """Fines page with details
        Chose the records between start_date and end_date
        If not specified then choose all reqords."""

        renavam = response.selector.xpath(
            "//span[@id='lblRenavam']/text()").get("").strip()
        placa = response.selector.xpath(
            "//span[@id='lblPlaca']/text()").get("").strip()
        file_type = self.remove_diacritics(response.selector.xpath(
            "//span[@id='LblCabecalho01']/text()").get("").strip())
        print("renavam:", renavam)
        print("placa:", placa)
        print("file_type:", file_type)

        # Get options for request
        EVENTTARGET = response.selector.xpath(
            "//input[@id='__EVENTTARGET']/@value").get("")
        EVENTARGUMENT = response.selector.xpath(
            "//input[@id='__EVENTARGUMENT']/@value").get("")
        PageProdamSPOnChange = response.selector.xpath(
            "//input[@id='PageProdamSPOnChange']/@value").get("")
        PageProdamSPPosicao = response.selector.xpath(
            "//input[@id='PageProdamSPPosicao']/@value").get("")
        PageProdamSPFocado = response.selector.xpath(
            "//input[@id='PageProdamSPFocado']/@value").get("")
        VIEWSTATE = response.selector.xpath(
            "//input[@id='__VIEWSTATE']/@value").get("")
        VIEWSTATEGENERATOR = response.selector.xpath(
            "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
        EVENTVALIDATION = response.selector.xpath(
            "//input[@id='__EVENTVALIDATION']/@value").get("")
        btnGerarDocumento = response.selector.xpath(
            "//input[@id='btnGerarDocumento']/@value").get("")
        txthvalor_total = response.selector.xpath(
            "//input[@id='txthvalor_total']/@value").get("")
        txthqtd_total = response.selector.xpath(
            "//input[@id='txthqtd_total']/@value").get("")

        frm_data = {
            'PageProdamSPOnChange': PageProdamSPOnChange,
            'PageProdamSPPosicao': PageProdamSPPosicao,
            'PageProdamSPFocado': PageProdamSPFocado,
            '__EVENTTARGET': EVENTTARGET,
            '__EVENTARGUMENT': EVENTARGUMENT,
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': EVENTVALIDATION,
            'chkSelecionarTodos': 'on',
            'btnGerarDocumento': btnGerarDocumento,
            'txthvalor_total': txthvalor_total,
            'txthqtd_total': txthqtd_total}

        rows = response.selector.xpath("//table[@id='grdDados']//tr[@class]")
        all_rows_data = []
        for row in rows:
            # check if infringement_date between start_date and end_date
            # if they are not specified then get all records
            infringement_date = row.xpath(".//td[6]/text()").get("").strip()
            infringement_datetime = dt.strptime(infringement_date, "%d/%m/%Y")
            if self.start_date <= infringement_datetime <= self.end_date:

                # choose the record
                chkMulta = row.xpath(".//td[1]/span/input/@name").get("")
                hdnSituacaoPPM = row.xpath(".//td[1]/input/@name").get("")
                frm_data.update({chkMulta: "on", hdnSituacaoPPM: ""})

                # get fields
                notification = row.xpath(".//td[3]/text()").get("").strip()
                infringement = row.xpath(".//td[4]/text()").get("").strip()
                description = row.xpath(".//td[5]/text()").get("").strip()
                infringement_time = row.xpath(
                    ".//td[7]/text()").get("").strip()
                location = row.xpath(".//td[8]/text()").get("").strip()
                due_date = row.xpath(".//td[9]/text()").get("").strip()
                value = row.xpath(".//td[10]/span/text()").get("").strip()
                debt_situation = row.xpath(".//td[11]/text()").get("").strip()
                installment_code = row.xpath(
                    ".//td[12]/text()").get("").strip()
                situation_description = row.xpath(
                    ".//td[13]/text()").get("").strip()
                date = row.xpath(".//td[14]/text()").get("").strip()

                row_data = {
                    "notificacao": notification,
                    "auto_infracao": infringement,
                    "descricao": description,
                    "data_infracao": infringement_date,
                    "hora": infringement_time,
                    "local_da_infracao": location,
                    "vencimento": due_date,
                    "valor": value,
                    "situacao_na_divida_ativa": debt_situation,
                    "codigo_do_parcelamento": installment_code,
                    "descricao_da_situacao": situation_description,
                    "data": date
                }
                all_rows_data.append(row_data)
        # add data to result
        if all_rows_data:
            self.result.update({file_type: all_rows_data})
            # check if get_files is True
            if self.get_files:
                report_url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmResumoMultasDetalhe.aspx"
                yield FormRequest(url=report_url, formdata=frm_data,
                                  meta={"file_type": "boleto",
                                        "result_key": file_type,
                                        "notification": notification},
                                  callback=self.report_table, dont_filter=True)
        else:
            error_msg = "traffic_fines_details doesn't contain any data."
            self.logger.warning(error_msg)

    def report_table(self, response):
        """Page used to review report before print."""

        file_type = response.meta['file_type']
        # Get options for request
        PageProdamSPOnChange = response.selector.xpath(
            "//input[@id='PageProdamSPOnChange']/@value").get("")
        PageProdamSPPosicao = response.selector.xpath(
            "//input[@id='PageProdamSPPosicao']/@value").get("")
        PageProdamSPFocado = response.selector.xpath(
            "//input[@id='PageProdamSPFocado']/@value").get("")
        VIEWSTATE = response.selector.xpath(
            "//input[@id='__VIEWSTATE']/@value").get("")
        VIEWSTATEGENERATOR = response.selector.xpath(
            "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
        EVENTVALIDATION = response.selector.xpath(
            "//input[@id='__EVENTVALIDATION']/@value").get("")

        frm_data = {
            'PageProdamSPOnChange': PageProdamSPOnChange,
            'PageProdamSPPosicao': PageProdamSPPosicao,
            'PageProdamSPFocado': PageProdamSPFocado,
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': EVENTVALIDATION}

        pdf_url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmEmissaoDamsp.aspx"

        rows = response.selector.xpath("//table[@id='grdDados']//tr[@class]")
        for row in rows:
            input_btn = row.xpath(".//td[5]/input")
            if input_btn:
                btn_id = input_btn.xpath("./@id").get("")
                btn_name = input_btn.xpath("./@name").get("")
                btn_value = input_btn.xpath("./@value").get("")
                frm_data_copy = frm_data.copy()
                frm_data_copy.update(
                    {btn_name: btn_value, "PageProdamSPFocado": btn_id})
                yield FormRequest(url=pdf_url, formdata=frm_data_copy,
                                  meta={"file_type": file_type,
                                        "result_key": response.meta['result_key']},
                                  callback=self.get_traffic_fines_pdf, dont_filter=True)

    def get_traffic_fines_pdf(self, response):
        """Redirect to print page."""

        pdf_url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmGerarImpDamsp.aspx"
        yield Request(url=pdf_url,
                      meta={"file_type": response.meta['file_type'],
                            "result_key": response.meta['result_key']},
                      callback=self.save_traffic_fines_pdf, dont_filter=True)

    def save_traffic_fines_pdf(self, response):
        """Function to print HTML to PDF, save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = response.meta['result_key']
        file_type = "__{file_type}__".format(
            file_type=response.meta['file_type'])

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        try:
            options = {
                'page-size': 'A4',
                'encoding': "UTF-8"
            }
            html_text = self.decode_response_to_utf8(
                response, encoding='latin-1').replace(
                "../", "https://meuveiculo.prefeitura.sp.gov.br/")
            pdfkit.from_string(html_text, file_path, options=options)

            # upload pdf to s3 and call the webhook
            self.upload_file(file_id)

            # update values in result
            result_value = self.result.get(result_key, [])
            [item.update({
                file_type: {
                    "file_id": file_id}
                }) for item in result_value]
            self.result.update({result_key: result_value})
        except Exception as exc:
            error_msg = {"error_type": "FILE_NOT_SAVED", "file": filename, "details": str(exc)}
            self.errors.append(error_msg)
            self.logger.error(error_msg)

    def ait_main(self, response):
        """AIT summary page."""

        # create screenshot using imgkit
        if self.capture_screenshot:
            self.take_screenshot(response, encoding='latin-1')

        # Get options for request
        PageProdamSPOnChange = response.selector.xpath(
            "//input[@id='PageProdamSPOnChange']/@value").get("")
        PageProdamSPPosicao = response.selector.xpath(
            "//input[@id='PageProdamSPPosicao']/@value").get("")
        PageProdamSPFocado = response.selector.xpath(
            "//input[@id='PageProdamSPFocado']/@value").get("")
        VIEWSTATE = response.selector.xpath(
            "//input[@id='__VIEWSTATE']/@value").get("")
        VIEWSTATEGENERATOR = response.selector.xpath(
            "//input[@id='__VIEWSTATEGENERATOR']/@value").get("")
        EVENTVALIDATION = response.selector.xpath(
            "//input[@id='__EVENTVALIDATION']/@value").get("")

        frm_data = {
            'PageProdamSPOnChange': PageProdamSPOnChange,
            'PageProdamSPPosicao': PageProdamSPPosicao,
            'PageProdamSPFocado': PageProdamSPFocado,
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': EVENTVALIDATION}

        pdf_url = "https://meuveiculo.prefeitura.sp.gov.br/forms/frmConsultaAutosInfracaoTransito.aspx"

        renavam = response.selector.xpath(
            "//span[@id='lblRenavam']/text()").get("").strip()
        placa = response.selector.xpath(
            "//span[@id='lblPlaca']/text()").get("").strip()
        file_type = self.remove_diacritics(response.selector.xpath(
            "//table[@id='grdDados']//tr[1]/td[3]/text()").get("").strip())
        # print("renavam:", renavam)
        # print("placa:", placa)
        # print("file_type:", file_type)

        rows = response.selector.xpath("//table[@id='grdDados']//tr[@class]")
        all_rows_data = []
        for row in rows:
            # get fields
            ait_code = row.xpath(".//td[2]/text()").get("").strip()
            infringement_date = row.xpath(".//td[3]/text()").get("").strip()
            infringement_time = row.xpath(".//td[4]/text()").get("").strip()
            local = row.xpath(".//td[5]/text()").get("").strip()
            description = row.xpath(".//td[6]/text()").get("").strip()
            ait_btn = row.xpath(".//td[7]/input")
            notification_btn = row.xpath(".//td[8]/input")

            row_data = {
                "a_i_t": ait_code,
                "data": infringement_date,
                "hora": infringement_time,
                "local": local,
                "descricao": description
            }
            all_rows_data.append(row_data)

            # check if get_files is True
            if self.get_files:
                # download ait file
                if ait_btn:
                    btn_id = ait_btn.xpath("./@id").get("")
                    btn_name = ait_btn.xpath("./@name").get("")
                    btn_value = ait_btn.xpath("./@value").get("")
                    frm_data_copy = frm_data.copy()
                    frm_data_copy.update(
                        {btn_name: btn_value, "PageProdamSPFocado": btn_id})
                    yield FormRequest(url=pdf_url, formdata=frm_data_copy,
                                      meta={"ait_code": ait_code,
                                            "result_key": file_type,
                                            "file_type": "ait"},
                                      callback=self.save_ait_pdf, dont_filter=True)

                # download notificacao file
                if notification_btn:
                    btn_id = notification_btn.xpath("./@id").get("")
                    btn_name = notification_btn.xpath("./@name").get("")
                    btn_value = notification_btn.xpath("./@value").get("")
                    frm_data_copy = frm_data.copy()
                    frm_data_copy.update(
                        {btn_name: btn_value, "PageProdamSPFocado": btn_id})
                    yield FormRequest(url=pdf_url, formdata=frm_data_copy,
                                      meta={"ait_code": ait_code,
                                            "result_key": file_type,
                                            "file_type": "notificacao"},
                                      callback=self.save_ait_pdf, dont_filter=True)
        # add data to result
        if all_rows_data:
            self.result.update({file_type: all_rows_data})

    def save_ait_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = response.meta['result_key']
        file_type = "__{file_type}__".format(
            file_type=response.meta['file_type'])

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
            }) for item in result_value]
        self.result.update({result_key: result_value})

    def get_final_result(self, spider):
        """Will be called before spider closed
        Used to save data_collected result."""

        # stop crawling after yeild_item called
        if not self.result_received:
            # push to webhook
            if self.screenshots_ids:
                self.result['__screenshots_ids__'] = self.screenshots_ids
            self.result.update({
                'renavam': self.renavam,
                'placa': self.placa})
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
                '{renavam}-data_collected.json'.format(renavam=self.renavam))
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
