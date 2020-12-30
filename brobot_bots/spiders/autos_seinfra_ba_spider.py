# -*- coding: utf-8 -*-

import os
import re
import sys
import uuid

import pdfkit
from scrapy import signals
from scrapy.http import FormRequest, Request

from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.items import BrobotBotsItem


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
#del path


class autos_seinfra_ba_spider(CustomSpider):
    # required scraper name
    name = "autos_seinfra_ba"

    # initial urls
    start_url = 'http://smt.derba.ba.gov.br:8180/smt/home.action'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(autos_seinfra_ba_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # self.strip_extra to remove extra data
        self.regex = re.compile(r'\s+')
        self.files_to_be_scraped = []

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        yield Request(self.start_url, callback=self.login_me,
                      errback=self.errback_func, dont_filter=True)

    def login_me(self, response):
        token_name = response.selector.xpath("//form[@id='form1']//input[@name='struts.token.name']/@value").get("")
        token_value = response.selector.xpath("//form[@id='form1']//input[@name='struts.token']/@value").get("")

        frm_data = {'veiculo.nuRenavam': self.renavam,
                    'veiculo.nuPlaca': self.placa,
                    'acao': 'Consultar',
                    'struts.token.name': token_name,
                    'struts.token': token_value}

        url = "http://smt.derba.ba.gov.br:8180/smt/consulta.action"
        yield FormRequest(url, formdata=frm_data,
                          callback=self.redirect_to_main,
                          errback=self.errback_func, dont_filter=True)

    def redirect_to_main(self, response):
        url = "http://smt.derba.ba.gov.br:8180/smt/consulta.action"
        yield Request(url, callback=self.get_main_page,
                      errback=self.errback_func, dont_filter=True)

    def get_main_page(self, response):

        if self.capture_screenshot or self.get_files:
            html_text = response.text.replace(
                '"css/', '"http://smt.derba.ba.gov.br:8180/smt/css/').replace(
                '"js/', '"http://smt.derba.ba.gov.br:8180/smt/js/').replace(
                '"img/', '"http://smt.derba.ba.gov.br:8180/smt/img/').replace(
                '(img/bgbanner2.png)', '(http://smt.derba.ba.gov.br:8180/smt/img/bgbanner2.png)')

        if self.capture_screenshot:
            self.take_screenshot(response, html_text=html_text)

        dados_do_vaiculo_rows = response.selector.xpath("//table[.//span[normalize-space()='Dados do Veículo']]/following::table[1]//tr")
        dados_do_vaiculo_rows += response.selector.xpath("//table[.//span[normalize-space()='Dados do Veículo']]/following::table[2]//tr")
        for row in dados_do_vaiculo_rows:
            items = row.xpath("./td")
            for i in range(1, len(items), 2):
                name = self.remove_diacritics(row.xpath("./td[{}]//text()".format(i)).get("").strip())
                value = self.regex.sub(
                    " ", "".join(row.xpath("./td[{}]//text()".format(i + 1)).extract()).strip())
                self.result[name] = value

        nadacosta = self.regex.sub(
            " ", "".join(response.selector.xpath("//div[@class='textoNadaConsta']/text()").extract()).strip())
        if nadacosta:
            self.result['sem_infracoes'] = nadacosta
        else:
            ait_tables = response.selector.xpath("//div[@id='paginacao']/div[contains(@class,'descricao')]")
            auto_s_de_infracao = []
            for item in ait_tables:
                numero_ait = self.regex.sub(" ", item.xpath("./span[@class='numeroAIT']/text()").get("").strip())
                rows = item.xpath("./table//tr")
                row_data = {'numero_ait': numero_ait}
                for row in rows:
                    name = self.remove_diacritics(row.xpath("./td[1]//text()").get("").strip(""))
                    value = self.regex.sub(
                        " ", "".join(row.xpath("./td[2]//text()").extract()).strip())
                    if name:
                        row_data[name] = value
                auto_s_de_infracao.append(row_data)

                if self.get_files and row_data['situacao'] == 'Penalizado':
                    file_url = "http://smt.derba.ba.gov.br:8180/{}".format(
                        item.xpath(".//div[@class='botoesAIT']//a/@href").get(""))
                    self.files_to_be_scraped.append({
                        'file_url': file_url,
                        'row_data': row_data})
            self.result['auto_s_de_infracao'] = auto_s_de_infracao

        if self.get_files:
            file_id = str(uuid.uuid4())
            filename = "{file_id}.pdf".format(file_id=file_id)
            file_path = os.path.join(path, "downloads", self.scrape_id, filename)
            options = {
                'page-size': 'A4',
                'encoding': "UTF-8",
            }
            try:
                try:
                    pdfkit.from_string(html_text, file_path, options=options)
                except:
                    pass

                # upload pdf to s3 and call the webhook
                self.upload_file(file_id)

                # update values in result
                self.result["__comprovante_de_acesso__"] = {"file_id": file_id}
            except Exception as exc:
                error_msg = {"error_type": "FILE_NOT_SAVED",
                             "file": filename, "details": str(exc)}
                self.errors.append(error_msg)
                self.logger.error(error_msg)

            for item in self.files_to_be_scraped:
                yield Request(item['file_url'], callback=self.save_pdf,
                              meta={'row_data': item['row_data']}, dont_filter=True)

    def save_pdf(self, response):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        result_key = "auto_s_de_infracao"
        file_type = "__boleto__"
        row_data = response.meta['row_data']

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
            }) for item in result_value if item == row_data]
        self.result.update({result_key: result_value})

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
