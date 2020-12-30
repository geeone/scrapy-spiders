# -*- coding: utf-8 -*-

from http.cookies import SimpleCookie
from datetime import datetime as dt
import json
import os
import re
import sys
import unicodedata
import uuid
import urllib.parse

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from botocore.config import Config
import imgkit
import requests
import scrapy
from twocaptcha import TwoCaptcha

import boto3 as bt3
from brobot_bots.external_modules import deathbycaptcha
from brobot_bots.external_modules.config import access_settings as config


path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if path not in sys.path:
    sys.path.insert(1, path)
#del path


class BytesDump(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode()
        return json.JSONEncoder.default(self, obj)


class CustomSpider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        self.result = {}
        self.warnings = []
        self.errors = []
        # initialize request_params defaults
        self.cnpj = ''
        self.renavam = ''
        self.start_date = None
        self.end_date = None
        self.get_files = False
        self.use_proxy = False
        self.capture_screenshot = False
        self.navigation_constraints = None
        self.scraping_scope = 'FULL'
        self.captcha_service = '2Captcha'  # DBC
        # decode and spread all received request_params into self
        request_params = kwargs.get('request_params')
        if request_params:
            request_dict = json.loads(request_params)
            for param_key in request_dict:
                setattr(self, param_key, request_dict[param_key])
        # convert to datetime
        if self.start_date:
            self.start_date = dt.strptime(self.start_date, "%Y-%m-%d")
        else:
            self.start_date = dt.min
        if self.end_date:
            self.end_date = dt.strptime(self.end_date, "%Y-%m-%d")
        else:
            self.end_date = dt.max
        # create folders (test scrape_id to not raise errors on legacy init spiders)
        if hasattr(self, 'scrape_id'):
            self.create_folder(os.path.join(path, "downloads", self.scrape_id))
            if self.capture_screenshot:
                self.create_folder(os.path.join(path, "screenshots", self.scrape_id))
        # workaround
        # used to avoid cycling after spider finished instead of the fake request
        self.result_received = False
        # internal arguments
        self.file_ids = {}
        self.img_captcha_id = None
        self.g_recaptcha_id = None
        self.captcha_retries = 3
        self.incorrect_captcha_retries = 3
        self.download_retries = 3
        self.files_count = 0
        self.screenshots_count = 0
        self.screenshots_ids = []
        self.bucket_name = config['S3_BUCKET_NAME']
        # splash arguments to save screenshots
        self.splash_args = {
            'html': 1,
            'png': 1,
            'render_all': 1,
            'wait': 0.5
        }

    def decode_response_to_utf8(self, response, encoding):
        html_text = unicodedata.normalize(
            'NFKD', response.body.decode(encoding)
            ).encode('ascii', 'ignore').decode('utf-8')
        return html_text

    def keys_string(self, d):
        """recursive key as string conversion for byte keys"""

        rval = {}
        if not isinstance(d, dict):
            if isinstance(d, (tuple, list, set)):
                v = [self.keys_string(x) for x in d]
                return v
            else:
                return d

        for k, v in d.items():
            if isinstance(k, bytes):
                k = k.decode()
            if isinstance(v, dict):
                v = self.keys_string(v)
            elif isinstance(v, (tuple, list, set)):
                v = [self.keys_string(x) for x in v]
            rval[k] = v
        return rval

    def unique_list(self, errors):
        errors_dumped = [json.dumps(self.keys_string(item), cls=BytesDump) for item in errors]
        errors_filtered = [json.loads(item) for item in list(set(errors_dumped))]
        return errors_filtered

    def create_folder(self, dir_name):
        """Function to check if folder exist
        If not then create recursively.
        """

        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)

    def errback_func(self, failure):
        """Function to handle errors of failed requests."""

        # get the type of failure
        failure_type = response_status = None
        if failure.check(HttpError):
            failure_type = 'HttpError'
            response_status = failure.value.response.status
        elif failure.check(DNSLookupError):
            failure_type = 'DNSLookupError'
        elif failure.check(TimeoutError, TCPTimedOutError):
            failure_type = 'TimeoutError'

        error_msg = {
            'error_type': 'SCRAPY_ERROR',
            'url': failure.request._url,
            'cookies': failure.request.cookies,
            'headers': failure.request.headers,
            'meta': failure.request._meta,
            'error_message': failure.getErrorMessage(),
            'response_status': response_status,
            'failure_type': failure_type
        }
        # fix bytes keys
        error_msg = json.loads(json.dumps(self.keys_string(error_msg), cls=BytesDump))
        self.errors.append(error_msg)
        self.logger.error(error_msg)

    def upload_to_s3(self, file_path):
        """Function to upload file to S3 bucket."""

        try:
            session = bt3.Session(
                aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
            )
            s3_setting = Config(
                retries={
                    'max_attempts': 3,
                    'mode': 'standard'
                }
            )
            s3_client = session.client('s3', config=s3_setting)
            filename = os.path.split(file_path)[-1]
            s3_client.upload_file(file_path, self.bucket_name, filename)
        except Exception as exc:
            error_msg = {"error_type": "UPLOAD_FAILED", "url": self.bucket_name,
                         "data": filename, "details": str(exc)}
            return error_msg
        finally:
            if '.pdf' in file_path:
                self.files_count += 1
            elif '.png' in file_path:
                self.screenshots_count += 1

    def push_to_webhook(self, webhook_url, data):
        """Function to call the webhook
        To get the APP know when file is uploaded.
        """

        try:
            response = requests.post(webhook_url, json=data)
            print("{}: {}".format(response.status_code, webhook_url))
        except Exception as exc:
            raise Exception(exc)

    def upload_file(self, file_id):
        """Function to upload PDF to S3 bucket and call the webhook then."""

        try:
            # initialization
            webhook_url = config['UPLOAD_COMPLETED_URL']
            filename = "{file_id}.pdf".format(file_id=file_id)
            file_path = os.path.join(
                path, "downloads", self.scrape_id, filename)
            webhook_file_path = os.path.join(
                path, "downloads", self.scrape_id,
                '{file_id}-upload_completed.json'.format(file_id=file_id))
            # payload
            data = {
                "scraper_name": self.name,
                "scrape_id": self.scrape_id,
                "cnpj": self.cnpj,
                "result": {
                    "file_id": file_id,
                    "file_name": filename,
                    "s3_key": filename,
                    "s3_bucket": self.bucket_name,
                    "utc_time_of_capture": dt.utcnow(
                        ).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "content_type": "application/pdf"
                }
            }
            # upload file to s3
            error = self.upload_to_s3(file_path)
            # add errors to result data
            if error:
                data.update({'errors': [error]})
            # push to webhook
            if webhook_url:
                self.push_to_webhook(webhook_url, data)
            else:
                # create local json request
                with open(webhook_file_path, 'wb') as json_file:
                    json_file.write(json.dumps(
                        data, indent=4, sort_keys=True, ensure_ascii=False).encode("utf-8"))
        except Exception as exc:
            error_msg = {"error_type": "UPLOAD_FAILED", "url": webhook_url,
                         "data": data, "details": str(exc)}
            self.errors.append(error_msg)
            self.logger.error(error_msg)

    def data_collected(self, data, webhook_file_path):
        """Function to send the collected data to webhook."""

        # ensure_renavam on payload root
        if type(data) is dict and 'renavam' not in data:
            data.update({'renavam': self.renavam})
        # ensure_cnpj on payload root
        if type(data) is dict and 'cnpj 'not in data:
            data.update({'cnpj': self.cnpj})

        webhook_url = config['DATA_COLLECTED_URL']
        try:
            if webhook_url:
                self.push_to_webhook(webhook_url, data)
            else:
                # create local json request
                with open(webhook_file_path, 'wb') as json_file:
                    json_file.write(json.dumps(
                        data, indent=4, sort_keys=True, ensure_ascii=False).encode("utf-8"))
        except Exception as exc:
            error_msg = {"error_type": "UPLOAD_FAILED", "url": webhook_url,
                         "data": data, "details": str(exc)}
            self.errors.append(error_msg)
            self.logger.error(error_msg)

    def upload_screenshot(self, file_id):
        """Function to upload Screenshots to S3 bucket and call the webhook then."""

        try:
            # initialization
            webhook_url = config['UPLOAD_COMPLETED_URL']
            filename = "{file_id}.png".format(file_id=file_id)
            file_path = os.path.join(
                path, "screenshots", self.scrape_id, filename)
            webhook_file_path = os.path.join(
                path, "screenshots", self.scrape_id,
                '{file_id}-upload_completed.json'.format(file_id=file_id))
            # payload
            data = {
                "scraper_name": self.name,
                "scrape_id": self.scrape_id,
                "cnpj": self.cnpj,
                "result": {
                    "file_id": file_id,
                    "file_name": filename,
                    "s3_key": filename,
                    "s3_bucket": self.bucket_name,
                    "utc_time_of_capture": dt.utcnow(
                        ).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "content_type": "image/png"
                }
            }
            # upload file to s3
            error = self.upload_to_s3(file_path)
            # add errors to result data
            if error:
                data.update({'errors': [error]})
            # push to webhook
            if webhook_url:
                self.push_to_webhook(webhook_url, data)
            else:
                # create local json request
                with open(webhook_file_path, 'wb') as json_file:
                    json_file.write(json.dumps(
                        data, indent=4, sort_keys=True, ensure_ascii=False).encode("utf-8"))
        except Exception as exc:
            error_msg = {"error_type": "UPLOAD_FAILED", "url": webhook_url,
                         "data": data, "details": str(exc)}
            self.errors.append(error_msg)
            self.logger.error(error_msg)

    def take_screenshot(self, response, **kwargs):
        """Function is used as a workaround
        In case of Splash screenshots do not work."""

        exc_msg = ''
        try:
            html_text = kwargs.get('html_text', '')
            url_path = kwargs.get('url_path', '')
            encoding = kwargs.get('encoding', 'utf-8')
            # initialization
            file_id = str(uuid.uuid4())
            filename = "{file_id}.png".format(file_id=file_id)
            file_path = os.path.join(
                path, "screenshots", self.scrape_id, filename)
            # data preparation
            if not html_text:
                html_text = self.decode_response_to_utf8(response, encoding)
            url_info = urllib.parse.urlsplit(response.url)
            domain = "{}://{}".format(url_info.scheme, url_info.netloc)
            if url_path:
                domain = urllib.parse.urljoin(domain, url_path)
            html_text = re.sub(
                r"(<(LINK|link|img|script|a)\s+(?:[^>]*?\s+)?(src|href)=)(\"|\')\.{0,2}(/" +
                url_path + r")?(/.*?)\4", r'\1\4{}\6\4'.format(domain), html_text)
            # imgkit processing
            options = {
                'format': 'png',
                'encoding': "UTF-8",
                'quiet': ''
            }
            imgkit.from_string(html_text, file_path, options=options)
        except Exception as exc:
            exc_msg = str(exc)
            error_msg = {"error_type": "SCREENSHOT_ISSUED",
                         "file_path": file_path,
                         "page": response.url, "details": exc_msg}
            self.warnings.append(error_msg)
            self.logger.warning(error_msg)
        finally:
            # upload file to s3
            if os.path.exists(file_path):
                self.screenshots_ids.append(file_id)
                self.upload_screenshot(file_id)
            else:
                error_msg = {"error_type": "SCREENSHOT_NOT_TAKEN",
                             "page": response.url, "details": exc_msg}
                self.errors.append(error_msg)
                self.logger.error(error_msg)

    def take_screenshot_from_url(self, response, **kwargs):
        """Function to get screenshot by directly page loading."""

        try:
            # get cookies to take a screenshot
            cookies_list = kwargs.get('cookies', '')
            if not cookies_list:
                cookies = response.headers.getlist('Set-Cookie')
                c = SimpleCookie()
                for cookie in cookies:
                    c.load(cookie.decode("utf-8"))
                cookies_list = [(key, c[key].value) for key in c]
            # initialization
            file_id = str(uuid.uuid4())
            filename = "{file_id}.png".format(file_id=file_id)
            file_path = os.path.join(
                path, "screenshots", self.scrape_id, filename)
            # imgkit processing
            options = {
                'format': 'png',
                'encoding': "UTF-8",
                'quiet': '',
                'cookie': cookies_list
            }
            imgkit.from_url(response.url, file_path, options=options)
        except Exception as exc:
            error_msg = {"error_type": "SCREENSHOT_ISSUED",
                         "page": file_path, "details": str(exc)}
            self.warnings.append(error_msg)
            self.logger.warning(error_msg)
        finally:
            # upload file to s3
            if os.path.exists(file_path):
                self.screenshots_ids.append(file_id)
                self.upload_screenshot(file_id)
            else:
                error_msg = {"error_type": "SCREENSHOT_NOT_TAKEN",
                             "page": file_path, "details": str(exc)}
                self.errors.append(error_msg)
                self.logger.error(error_msg)

    def captcha_solver(self, captcha_service, **kwargs):
        """Function to solve captcha using specific captcha solving service
        Type of captcha depends on image path was specified or not.
        """

        try:
            # initialization
            sitekey = kwargs.get('sitekey')
            captcha_url = kwargs.get('captcha_url')
            captcha_img = kwargs.get('captcha_img')
            captcha_type = kwargs.get('captcha_type', 4)
            captcha_action = kwargs.get('captcha_action')

            balance = None
            if captcha_service == "DBC":
                dbc_token = config['DBC_TOKEN']
                client = deathbycaptcha.HttpClient(None, None, dbc_token)
                try:
                    balance = client.get_balance()
                    print("current balance:", balance)
                except Exception as exc:
                    error_msg = {"error_type": "CAPTCHA_SOLVER",
                                 "details": str(exc)}
                    self.logger.warning(error_msg)

                if sitekey and captcha_url:
                    print("CAPTCHA key:", sitekey)
                    captcha_dict = {
                        'googlekey': sitekey,
                        'pageurl': captcha_url}
                    if captcha_type == 5:
                        captcha_dict.update({
                            'action': captcha_action,
                            'min_score': 0.3})
                    json_captcha = json.dumps(captcha_dict)
                    captcha = client.decode(type=captcha_type, token_params=json_captcha)
                elif captcha_img:
                    captcha = client.decode(captcha_img)

                if captcha:
                    if captcha['is_correct']:
                        print("CAPTCHA %s solved" % captcha["captcha"])
                        return (captcha['captcha'], captcha['text'])
                    else:
                        print("CAPTCHA %s solved incorrectly" % captcha["captcha"])
                        client.report(captcha["captcha"])
                else:
                    print("CAPTCHA doesn't exist.")

            elif captcha_service == "2Captcha":
                api_key = config['2CAPTCHA']
                client = TwoCaptcha(api_key)
                try:
                    balance = client.balance()
                    print("current balance:", balance)
                except Exception as exc:
                    error_msg = {"error_type": "CAPTCHA_SOLVER",
                                 "details": str(exc)}
                    self.logger.warning(error_msg)

                if sitekey and captcha_url:
                    captcha = client.recaptcha(sitekey=sitekey, url=captcha_url)
                elif captcha_img:
                    captcha = client.normal(captcha_img, numeric=4, language=2, lang='en')
                return captcha['captchaId'], captcha['code']

        except Exception as exc:
            error_msg = {
                "error_type": "CAPTCHA_FAILED", "captcha_service": captcha_service,
                "balance": balance, "details": str(exc)}
            raise Exception(error_msg)
        return (None, None)

    def incorrect_captcha_report(self, captcha_service, captcha_id):
        """Function to send report to specific captcha solving service
        Used in case of captcha was incorrectly solved.
        """

        try:
            if captcha_service == "DBC":
                dbc_token = config['DBC_TOKEN']
                client = deathbycaptcha.HttpClient(None, None, dbc_token)
                client.report(captcha_id)
            elif captcha_service == "2Captcha":
                api_key = config['2CAPTCHA']
                solver = TwoCaptcha(api_key)
                solver.report(captcha_id, False)
            is_reported = True
        except:
            is_reported = False
        finally:
            self.incorrect_captcha_retries -= 1
            print(self.incorrect_captcha_retries)
            error_msg = {
                "error_type": "CAPTCHA_INCORRECTLY_SOLVED",
                "captcha_service": captcha_service,
                "is_reported": is_reported}

            if self.incorrect_captcha_retries == 0:
                self.errors.append(error_msg)
                self.logger.error(error_msg)
            else:
                self.logger.warning(error_msg)

    # please do not change this function without first discussing with the 
    # architecture team as it affects the outputs of many spiders and may break 
    # compatibility with the backend
    def remove_diacritics(self, text):
        ''' Replace diacritics characters with 'normal' ones 
            also convert text to snake_case and convert to .lower() case'''

        text = text.replace('º', 'o').replace('ª', 'a')
        text = unicodedata.normalize('NFD', text).encode(
            'ascii', 'ignore').decode('utf8').strip().lower()
        # characters that will be replaced with nothing
        text = re.sub(r'[\$]+', "", text)
        text = re.sub(r'[\[\./|-–():+=#!@%&\'\*\?<>;,\]\"\s]+', "_", text)
        text = re.sub('_+', '_', text)
        # collapse _ from the end of string
        text = re.sub('_+$', '', text).strip()
        # collapse _ from the beginning of string
        text = re.sub('^_+', '', text).strip()
        return text

    def custom_requests(self, url=None, data=None, headers=None, method="GET", stream=False, allow_redirects=True):
        if method not in ['GET', 'POST']:
            error_msg = '{} is not a supported method. Supported methods are GET and POST'.format(
                method)
            raise ValueError(error_msg)

        response = None
        try:
            if method == "GET":
                response = requests.get(
                    url=url, headers=headers, stream=stream)
            elif method == "POST":
                response = requests.post(
                    url=url, data=data, headers=headers, allow_redirects=allow_redirects)
            response.raise_for_status()

        except requests.exceptions.ConnectionError as connection_error:
            error_msg = {
                'error_type': 'SCRAPY_ERROR',
                'url': url,
                'error_message': str(connection_error),
                'failure_type': 'ConnectionError'
            }
            self.logger.error(error_msg)
            self.errors.append(error_msg)

        except requests.exceptions.HTTPError as http_error:
            error_msg = {
                'error_type': 'SCRAPY_ERROR',
                'url': url,
                'error_message': str(http_error),
                'failure_type': 'HttpError'
            }
            self.logger.error(error_msg)
            self.errors.append(error_msg)

        except requests.exceptions.Timeout as timeout_error:
            error_msg = {
                'error_type': 'SCRAPY_ERROR',
                'url': url,
                'error_message': str(timeout_error),
                'failure_type': 'ConnectionTimeout'
            }
            self.logger.error(error_msg)
            self.errors.append(error_msg)

        except Exception as err:
            error_msg = {
                'error_type': 'SCRAPY_ERROR',
                'url': url,
                'error_message': str(err),
                'failure_type': 'UnexpectedError'
            }
            self.logger.error(error_msg)
            self.errors.append(error_msg)
        return response
