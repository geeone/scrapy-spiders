# -*- coding: utf-8 -*-

from datetime import datetime as dt
from http.cookies import SimpleCookie
import json
import os
import sys
import urllib.parse
import uuid
import re
import random

from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
#del path


class telecom_vivo_legacy_spider(CustomSpider):
    # required scraper name
    name = "telecom_vivo_legacy"

    start_url = 'https://login.vivo.com.br/loginmarca/appmanager/marca/publico?'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(telecom_vivo_legacy_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # internal arguments
        self.navigation_constraints = [
            item['cnpj'] for item in self.navigation_constraints] \
            if self.navigation_constraints else []

    def start_requests(self):
        query_str = {
            'acesso': 'empresas',
            'documento': "{}.{}.{}-{}".format(
                self.cpf_cnpj[:2],
                self.cpf_cnpj[2:5],
                self.cpf_cnpj[5:12],
                self.cpf_cnpj[-2:])
            }

        login_url = self.start_url + urllib.parse.urlencode(query_str)
        print("login url:", login_url)
        yield Request(login_url, callback=self.check_cpf,
                      errback=self.errback_func, dont_filter=True)

    def check_cpf(self, response):
        print("current url:", response.url)

        frm_data = {
            'nroDocumento': "{}.{}.{}/{}-{}".format(
                self.cpf_cnpj[:2],
                self.cpf_cnpj[2:5],
                self.cpf_cnpj[5:8],
                self.cpf_cnpj[8:12],
                self.cpf_cnpj[-2:]),
        }
        login_url = "https://login.vivo.com.br/loginmarca/br/com/vivo/marca/portlets/loginunificado/verificaTipoLoginPJ.do"
        yield FormRequest(login_url, formdata=frm_data,
                          callback=self.login_me,
                          errback=self.errback_func, dont_filter=True)

    def login_me(self, response):
        print(response.text)

        frm_data = {
            'cpf': "{}.{}.{}/{}-{}".format(
                self.cpf_cnpj[:2],
                self.cpf_cnpj[2:5],
                self.cpf_cnpj[5:8],
                self.cpf_cnpj[8:12],
                self.cpf_cnpj[-2:]),
            'senha': self.senha,
            'tipoPerfil': 'F',
            'origem': 'null',
        }

        print(frm_data)
        login_url = "https://login.vivo.com.br/loginmarca/br/com/vivo/marca/portlets/loginunificado/doLoginConvergente.do"
        yield FormRequest(login_url, formdata=frm_data,
                          callback=self.get_initiator,
                          errback=self.errback_func, dont_filter=True)

    def get_initiator(self, response):
        json_response = json.loads(response.text)
        error_message = json_response['message']
        if error_message != 'SUCCESS':
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        SPName = json_response['spName']
        RequestURL = json_response['requestURL']
        frm_data = {
            'SPName': SPName,
            'RequestURL': RequestURL}
        initiator_url = "https://login.vivo.com.br/saml2/idp/sso/initiator"
        yield FormRequest(initiator_url, formdata=frm_data,
                          callback=self.get_SAMLResponse,
                          errback=self.errback_func, dont_filter=True)

    def get_SAMLResponse(self, response):
        # print("SAMLResponse: ", response.request.headers['Cookie'])
        cookies = response.request.headers.getlist('Cookie')
        c = SimpleCookie()
        for cookie in cookies:
            c.load(cookie.decode("utf-8"))
        self.initiator_cookies = [{"name": key, "value": c[key].value} for key in c]
        self.acs_post_cookies = [{"name": key, "value": c[key].value} for key in c
                                 if not (key == "JSESSIONID" or key == "_WL_AUTHCOOKIE_JSESSIONID")]

        RelayState = response.selector.xpath("//input[@name='RelayState']/@value").get("")
        SAMLResponse = response.selector.xpath("//input[@name='SAMLResponse']/@value").get("")
        SPName = response.selector.xpath("//input[@name='SPName']/@value").get("")
        frm_data = {
            'RelayState': RelayState,
            'SAMLResponse': SAMLResponse,
            'SPName': SPName}
        meuvivoempresas_url = "https://meuvivoempresas.vivo.com.br/saml2/sp/acs/post"
        yield FormRequest(meuvivoempresas_url, formdata=frm_data,
                          meta={'is_authorized': response.meta.get("is_authorized", False),
                                'cookiejar': random.randint(50, 999)},
                          cookies=self.acs_post_cookies,
                          callback=self.create_SAMLRequest, dont_filter=True)

    def create_SAMLRequest(self, response):
        print("SAMLRequest: ", response.request.headers['Cookie'])

        is_authorized = response.meta.get("is_authorized", False)
        if is_authorized:
            cookies = response.request.headers.getlist('Cookie')
            c = SimpleCookie()
            for cookie in cookies:
                c.load(cookie.decode("utf-8"))
            self.updated_cookies = [{"name": key, "value": c[key].value} for key in c]
            # print(self.updated_cookies)
            url = response.selector.xpath("//iframe[@id='iframeParceiroExternoMarca']/@src").get("")
            # url = "https://legado.vivo.com.br/portal/site/meuvivo/segundaViaConta?segundaViaConta=sVConta"
            yield Request(url, callback=self.action_login,
                          meta={'cookiejar': random.randint(50, 999)},
                          cookies=self.updated_cookies,
                          dont_filter=True)
        else:
            cookies = response.headers.getlist('Set-Cookie')
            c = SimpleCookie()
            for cookie in cookies:
                c.load(cookie.decode("utf-8"))
            for key in c:
                if key == "dtCookie":
                    [item.update(
                        {'dtCookie': c[key].value}) for item in self.initiator_cookies
                        if item['name'] == 'dtCookie']
                    [item.update(
                        {'dtCookie': c[key].value}) for item in self.acs_post_cookies
                        if item['name'] == 'dtCookie']

            SAMLRequest = response.selector.xpath("//input[@name='SAMLRequest']/@value").get("")
            frm_data = {
                'SAMLRequest': SAMLRequest}
            login_url = "https://login.vivo.com.br/saml2/idp/sso/post"
            yield FormRequest(login_url, formdata=frm_data,
                              meta={'is_authorized': True,
                                    'cookiejar': random.randint(50, 999)},
                              cookies=self.initiator_cookies,
                              callback=self.get_SAMLResponse, dont_filter=True)

    def action_login(self, response):
        cookies = response.headers.getlist('Set-Cookie')
        c = SimpleCookie()
        for cookie in cookies:
            c.load(cookie.decode("utf-8"))
        for key in c:
            updated = False
            for item in self.updated_cookies:
                if key == item['name']:
                    item.update({
                        "name": key,
                        "value": c[key].value})
                    updated = True
                if item['name'] == "_WL_AUTHCOOKIE_JSESSIONID":
                    self.updated_cookies.remove(item)
            if not updated:
                self.updated_cookies.append({
                    "name": key,
                    "value": c[key].value})

        product_id = re.search("productId=(.*?)$", response.url)
        if product_id:
            product_id = product_id.group(1)
        token = re.search("token=(.*?)&", response.url)
        if token:
            token = token.group(1)

        VGN_NONCE = response.selector.xpath("//input[@name='VGN_NONCE']/@value").get("")

        frm_data = {
            'ckcTipoLogin': '2',
            'txtLoginCPFCNPJ': '',
            'txtLoginTelefone': product_id,
            'pswSenha': '',
            'VGN_NONCE': VGN_NONCE,
            'token': token,
            'product': product_id,
            'dependent': '',
            'redirect': '/portal/site/meuvivo/segundaViaConta?segundaViaConta=sVConta ',
            'logon': '',
            'password': '',
            'realm': 'realm1'
        }
        url = "https://legado.vivo.com.br/portal/site/meuvivo/template.LOGIN/action.process/"
        yield FormRequest(url, formdata=frm_data,
                          callback=self.get_segunda_via_conta,
                          meta={'cookiejar': random.randint(50, 999)},
                          cookies=self.updated_cookies,
                          dont_filter=True)

    def keymap_replace(self, string, mappings):
        for key, value in mappings.items():
            if key in string:
                string = string.replace(key, value)
                break
        return string

    def get_segunda_via_conta(self, response):
        print("segunda_via_conta:", response.request.headers['Cookie'])

        javax_portlet_sync = re.search(
            "javax.portlet.sync=(.*?)&", response.text)
        if javax_portlet_sync:
            javax_portlet_sync = javax_portlet_sync.group(1)

        javax_portlet_tpst = re.search(
            "javax.portlet.tpst=(.*?)&", response.text)
        if javax_portlet_tpst:
            javax_portlet_tpst = javax_portlet_tpst.group(1)

        status_replacement = {
            'statusIsenta': 'Isenta',
            'statusAguardando': 'Aguardando',
            'statusAberta': 'Aberta',
            'statusNegociada': 'Negociada',
            'statusPaga': 'Paga',
            'statusAguardando': 'Aguardando',
            'statusAtrasada': 'Atrasada'
        }

        debito_automatico = re.search('temDebitoAutomatico = (.*?);', response.text)
        if debito_automatico:
            debito_automatico = json.loads(debito_automatico.group(1))
            self.result['debito_automatico'] = debito_automatico

        status_list = re.findall(r"if\('(.+)' == '\1'\){(\s|.)+?lista_status\[\d+\] = (\w+);", response.text)
        status_list = [self.keymap_replace(status[2], status_replacement) for status in status_list]

        items = re.findall("listaDadosFatura\[\d+\] = ({(\s|.)*?});", response.text)
        minhas_contas = []
        files_list = []
        for i in range(len(items)):
            item = re.sub('\s+', " ", items[i][0])
            item = re.sub(": '?(.*?)'?,", r': "\1",', item)
            item = re.sub("(\w+):", r'"\1":', item)
            item = json.loads(item)
            # check if date in range
            vencimento_datetime = dt.strptime(item['dataVencimentoConta'], "%d/%m/%Y")
            if self.start_date <= vencimento_datetime <= self.end_date:
                valor = "{:.2f}".format(float(item['valorFaturaOrigem']))
                file_data = {
                    'status': status_list[i],
                    'valor': "R$ {}".format(valor.replace('.', ',')),
                    'vencimento': item['dataVencimentoConta']}
                minhas_contas.append(file_data)

                if self.get_files and status_list[i] != 'Paga':
                    frm_data = {
                        'idFatura': item['numeroFatura'],
                        'networkOwner': item['networkOwner']}
                    file_data_copy = file_data
                    file_data_copy.update(frm_data)
                    files_list.append(file_data_copy)

        self.result['minhas_contas'] = minhas_contas

        for file_data in files_list:
            pdf_url = "https://legado.vivo.com.br/portal/site/meuvivo/template.BINARYPORTLET/segundaViaConta/resource.process/?" + \
                "javax.portlet.sync={javax_portlet_sync}&javax.portlet.tpst={javax_portlet_tpst}&javax.portlet.rid_{javax_portlet_tpst}=baixarFatura&javax.portlet.rcl_{javax_portlet_tpst}=cacheLevelPage&javax.portlet.begCacheTok=com.vignette.cachetoken&javax.portlet.endCacheTok=com.vignette.cachetoken"
            pdf_url = pdf_url.format(javax_portlet_sync=javax_portlet_sync,
                                     javax_portlet_tpst=javax_portlet_tpst)
            idFatura = file_data.pop('idFatura')
            networkOwner = file_data.pop('networkOwner')
            frm_data = {
                'idFatura': idFatura,
                'networkOwner': networkOwner}

            yield FormRequest(pdf_url, formdata=frm_data,
                              callback=self.process_pdf,
                              meta={'cookiejar': random.randint(50, 999),
                                    'idFatura': idFatura,
                                    'networkOwner': networkOwner,
                                    'file_data': file_data},
                              cookies=self.updated_cookies,
                              dont_filter=True)

    def process_pdf(self, response):
        query_str = {
            'idFatura': response.meta['idFatura'],
            'networkOwner': response.meta['networkOwner']}
        url = "https://legado.vivo.com.br/VIVOSegundaViaFaturaPortlet/segundaViaFatura.jsp?"
        pdf_url = url + urllib.parse.urlencode(query_str)
        yield Request(pdf_url, callback=self.save_pdf,
                      meta={'cookiejar': random.randint(50, 999),
                            'file_data': response.meta['file_data']},
                      cookies=self.updated_cookies,
                      dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        file_type = "__boleto__"
        file_data = response.meta['file_data']

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
        with open(file_path, 'wb') as f:
            f.write(response.body)

        # upload pdf to s3 and call the webhook
        self.upload_file(file_id)

        # update values in result
        minhas_contas = self.result['minhas_contas']
        [item.update({
            file_type: {
                "file_id": file_id}
            }) for item in minhas_contas if item == file_data]
        self.result.update({'minhas_contas': minhas_contas})

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
                '{cnpj}-data_collected.json'.format(cnpj=self.cpf_cnpj))
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
