# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import os
import shutil
import sys
import time
import uuid

from scrapy import signals
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest, SplashFormRequest
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from brobot_bots.external_modules.config import access_settings as config
from brobot_bots.external_modules.external_functions import CustomSpider
from brobot_bots.external_modules.lua_script import script, script_30_sec_wait
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


class telecom_net_claro_spider(CustomSpider):
    # required scraper name
    name = "telecom_net_claro"

    # initial urls
    start_url = 'https://minhanet.net.com.br/webcenter/portal/NETAutoAtendimento'

    urls = {'customers_list': "https://minhanet.net.com.br/ecare-api-netuno-customer/api/v1/client/netuno/customer/listByTokenIdm?type=CUSTOM_KEY_IDM",
            'payment_history': "https://minhanet.net.com.br/ecare-api-payment-history/api/v1/client/payment/history/NET/{contract}/003/12",
            'pages_faturanet': "https://minhanet.net.com.br/webcenter/portal/MinhaNet/pages_faturanet"}

    # user and password for splash
    http_user = config['SPLASH_USERNAME']
    http_pass = config['SPLASH_PASSWORD']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Rewriting of the spider_idle function to yield result after spider closed."""

        spider = super(telecom_net_claro_spider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.get_final_result, signals.spider_idle)
        return spider

    def __init__(self, *args, **kwargs):
        # get variables from CustomSpider
        super().__init__(*args, **kwargs)
        # internal arguments
        self.payment_history = {}
        self.driver = None

    def start_requests(self):
        print("The module PATH is", os.path.dirname(__file__))
        yield SplashRequest(self.start_url, callback=self.login_me,
                            errback=self.errback_func,
                            endpoint='execute',
                            cache_args=['lua_source'],
                            args={'lua_source': script}, dont_filter=True)

    def login_me(self, response):
        login_url = "https://auth.netcombo.com.br/login"

        client_id = response.selector.xpath(
            "//input[@name='client_id']/@value").get("")
        redirect_uri = response.selector.xpath(
            "//input[@name='redirect_uri']/@value").get("")
        response_type = response.selector.xpath(
            "//input[@name='response_type']/@value").get("")
        scope = response.selector.xpath(
            "//input[@name='scope']/@value").get("")
        state = response.selector.xpath(
            "//input[@name='state']/@value").get("")
        authMs = response.selector.xpath(
            "//input[@name='authMs']/@value").get("")

        frm_data = {'Username': self.login,
                    'password': self.senha,
                    'client_id': client_id,
                    'redirect_uri': redirect_uri,
                    'response_type': response_type,
                    'scope': scope,
                    'state': state,
                    'authMs': authMs,
                    'Auth_method': 'UP'}
        print(frm_data)

        yield SplashFormRequest(login_url, formdata=frm_data,
                                callback=self.select_contract,
                                errback=self.errback_func,
                                endpoint='execute',
                                cache_args=['lua_source'],
                                args={'lua_source': script_30_sec_wait,
                                      'cookies': response.data['cookies'],
                                      'timeout': 60,
                                      'images': 0}, dont_filter=True)

    def select_contract(self, response):
        print(response.url)
        self.cookies = response.data['cookies']

        error_message = response.selector.xpath(
            "//p[contains(text(),'Usuário e/ou senha inválido(s)!')]/text()").get("")
        if error_message:
            error_msg = {"error_type": "WRONG_CREDENTIALS",
                         "details": error_message}
            self.errors.append(error_msg)
            self.logger.warning(error_msg)
            return

        yield Request(self.urls['customers_list'],
                      callback=self.get_json_response,
                      meta={'file_type': 'customers_list'},
                      cookies=self.cookies, dont_filter=True)

    def reset_cookies(self, contract):
        print("here")
        url = "https://minhanet.net.com.br/security-api/generateTokenSemRemoverCookie?contrato={contract}".format(
            contract=contract['contractNumber'])
        yield SplashRequest(url, callback=self.get_payment_history,
                            endpoint='execute',
                            cache_args=['lua_source'],
                            args={'lua_source': script_30_sec_wait,
                                  'cookies': self.cookies,
                                  'timeout': 60,
                                  'images': 0},
                            meta={'contract': contract}, dont_filter=True)

    def get_payment_history(self, response):
        # reset cookies
        self.cookies = response.data['cookies']
        contract = response.meta['contract']
        payment_url = self.urls['payment_history'].format(
            contract=contract['contractNumber'])
        yield Request(payment_url, callback=self.get_json_response,
                      meta={'file_type': contract['contractNumber']},
                      cookies=self.cookies, dont_filter=True)

    def get_json_response(self, response):
        file_type = response.meta['file_type']
        json_response = json.loads(response.text)
        listOfResult = json_response['listOfResult']

        if listOfResult:
            if file_type == 'customers_list':
                self.result.update({file_type: listOfResult})
                self.contracts_list = listOfResult.copy()
                contract = self.contracts_list.pop()
                # reset cookies
                print("customers_list: reset cookies")
                yield from self.reset_cookies(contract)
            else:
                print(listOfResult)
                self.payment_history.update(
                    {file_type: listOfResult})
                yield from self.get_pages_faturanet_with_selenium(self.urls['pages_faturanet'], file_type)

    def create_browser_session(self, url):
        download_dir = os.path.join(path, "downloads", self.scrape_id)
        print("Downloads directory:", download_dir)

        mime_types = "application/pdf,application/vnd.adobe.xfdf,application/vnd.fdf,application/vnd.adobe.xdp+xml"
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.link.open_newwindow", 3)
        fp.set_preference("browser.link.open_newwindow.restriction", 2)
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", download_dir)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk", mime_types)
        fp.set_preference("plugin.disable_full_page_plugin_for_types", mime_types)
        fp.set_preference("pdfjs.disabled", True)
        firefox_capabilities = DesiredCapabilities.FIREFOX
        firefox_capabilities['marionette'] = True

        options = FirefoxOptions()
        options.profile = fp
        options.headless = True
        driver = webdriver.Firefox(options=options, capabilities=firefox_capabilities)
        # driver.maximize_window()
        driver.get(url)
        return driver

    def update_browser_session(self, url):
        if not self.driver:
            self.driver = self.create_browser_session(url)
        # create local driver
        driver = self.driver
        driver.delete_all_cookies()

        # update session
        for cookie in self.cookies:
            cookie = {'name': cookie['name'],
                      'value': cookie['value']}
            driver.add_cookie(cookie)

        print('Session successfully created.')
        return driver

    def get_downloaded_filename(self, waitTime):
        endTime = time.time() + waitTime
        while True:
            lastest_file = os.path.join(
                path, "downloads", self.scrape_id, "fatura.pdf")
            if os.path.exists(lastest_file):
                return lastest_file
            time.sleep(1)
            if time.time() > endTime:
                break
        return

    def get_pages_faturanet_with_selenium(self, url, contract):
        try:
            # update session
            driver = self.update_browser_session(url)
            # create local driver
            driver = self.driver
            driver.get(url)
            # select contract
            contract_input_xpath = "//input[@value='{contract}']".format(contract=contract)
            contract_input = _check_by_xpath(driver, contract_input_xpath)
            _click_on_invisible_element(driver, contract_input)
            # continue
            continue_btn = _check_by_xpath(driver, "//div[@class='mcr-select-contract-list-action']/button")
            _click_on_invisible_element(driver, continue_btn)
            # wait for payments table
            payments_table_xpath = "//table[contains(@class,'payments')]"
            payments_table = _check_by_xpath(driver, payments_table_xpath)
            table_rows = payments_table.find_elements_by_xpath("./tbody/tr[@role='row']")
            for row in table_rows:
                status_not_paga = _check_by_xpath(row, "./td[@data-title='Status' and not(contains(text(),'Paga'))]")
                vencimento = _check_by_xpath(row, "./td[@data-title='Vencimento']").text.strip()
                vencimento_datetime = dt.strptime(vencimento, "%d/%m/%Y")
                if (self.start_date <= vencimento_datetime <= self.end_date) and status_not_paga:
                    nota_fiscal = _check_by_xpath(row, "./td[@data-title='Nota Fiscal']").text.strip()
                    baixar = _check_by_xpath(row, "./td/a[@data-gtm-event-action='clique:baixar-fatura']")
                    _click_on_invisible_element(driver, baixar)
                    # waiting 3 minutes to complete the download
                    lastest_file = self.get_downloaded_filename(180)
                    if lastest_file:
                        print("File successfully downloaded:", nota_fiscal)
                        file_id = str(uuid.uuid4())
                        filename = "{file_id}.pdf".format(file_id=file_id)
                        file_path = os.path.join(path, "downloads", self.scrape_id, filename)
                        shutil.move(lastest_file, file_path)
                        # upload to s3
                        self.save_pdf_postaction(file_id=file_id,
                                                 result_key=contract,
                                                 document_id=nota_fiscal)
        except Exception as exc:
            print("Browser crashed:", exc)
        finally:
            if self.contracts_list:
                contract = self.contracts_list.pop()
                # reset cookies
                print("payment_history: reset cookies")
                yield from self.reset_cookies(contract)
            elif driver:
                driver.close()
                driver.quit()

    def save_pdf_postaction(self, **kwargs):
        """Function to save PDF for uploading to s3 bucket."""

        # get metadata
        file_id = kwargs['file_id']
        result_key = kwargs['result_key']
        document_id = kwargs['document_id']
        file_type = "__boleto__"

        # upload pdf to s3 and call the webhook
        self.upload_file(file_id)

        # update values in result
        result_value = self.payment_history.get(result_key, [])
        [item.update({
            file_type: {
                "file_id": file_id}
        }) for item in result_value
            if item['documentNumber'] == document_id]
        self.payment_history.update({result_key: result_value})

    def get_final_result(self, spider):
        """Will be called before spider closed
        Used to save data_collected result."""

        # stop crawling after yeild_item called
        if not self.result_received:
            # push to webhook
            if self.payment_history:
                self.result.update(
                    {'payment_history': self.payment_history})
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
                path, "downloads", self.scrape_id, '{login}-data_collected.json'.format(
                    login=self.login))
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
