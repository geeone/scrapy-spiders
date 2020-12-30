# -*- coding: utf-8 -*-

import csv
import json
from multiprocessing import Process
import os
import uuid

from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings


def reactor_start(spider_name, init_data, file_path, ddelay, threads):
    s = get_project_settings()
    s.update({#'FEEDS': {file_path: {'format': 'csv'}},
              'LOG_LEVEL': 'DEBUG',
              'DOWNLOAD_DELAY': ddelay,
              'CONCURRENT_REQUESTS': threads})
    proc = CrawlerProcess(s)
    proc.crawl(spider_name, **init_data)
    proc.start()


def scrapy_crawl(spider_name, init_data, file_path=None, ddelay=0, threads=1):
    p = Process(target=reactor_start, args=(spider_name, init_data, file_path, ddelay, threads,))
    p.start()
    p.join()


def main():
    with open(os.path.join("bulk-run", "request_params.json"), 'rb') as json_file:
        json_config = json.loads(json_file.read())

    with open(os.path.join("bulk-run", 'credentials.csv'), 'r') as csv_file:
        cred_data = csv.DictReader(csv_file)

        for cred in list(cred_data)[:1]:
            json_config.update({'scrape_id': str(uuid.uuid4()),
                                'renavam': '00593672836',
                                'placa': cred['placa'],
                                'get_files': True,
                                'capture_screenshot': True})
            print(json_config)
            scrapy_crawl("autos_sefaz_ba", {'request_params': json.dumps(json_config)}, file_path='data_collected.csv')


if __name__ == '__main__':
    main()
