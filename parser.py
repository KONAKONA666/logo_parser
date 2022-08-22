from audioop import mul
from cgitb import html
from concurrent.futures import process
from dataclasses import dataclass
from typing import List
import scrapy
from scrapy.crawler import CrawlerProcess


import pandas as pd
import time
import lxml.html
import lxml.etree
import random
import re
import minify_html
import csv

import json

import html2text

import multiprocessing

custom_settings = {
    'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.DownloaderAwarePriorityQueue',
    'CONCURRENT_REQUESTS': 200,
   # 'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    'REACTOR_THREADPOOL_MAXSIZE': 100,
    'LOG_LEVEL': 'INFO',
    'COOKIES_ENABLED': True,
    'RETRY_ENABLED': False,
    'AJAXCRAWL_ENABLED': False,
    'DEPTH_PRIORITY': 1,
    'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'AUTOTHROTTLE_ENABLED': False,
    'DOWNLOAD_TIMEOUT': 60,
} 

def crawler_func(spider, urls, names, industries, n):
    crawler_process = CrawlerProcess(custom_settings)
    crawler_process.crawl(spider, urls=urls, names=names, industries=industries, n=n)
    crawler_process.start()

def get_src(img_tag: str) -> str:
    match = re.search("(?<=src=).+(?=)", img_tag+' ')
    if match is not None:
        return match.group().replace("'", "").replace('"', '')
    return None


def check_logo(img_tag: str) -> str:
    return img_tag.lower().find("logo") != -1


class HTMLSpider(scrapy.Spider):
    name = "HTMLSpider"
    def __init__(self, urls, names, industries, n, *args, **kwargs):
        super(HTMLSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls
        self.names = names
        self.industries = industries
        write_path = "parsed_img_urls_{}.csv".format(n)
        self.write_file = open(write_path, "w", encoding='latin-1', errors='ignore')
        self.img_pattern = re.compile("(?<=<img).*?(?=>)")
        self.svg_pattern = re.compile("(?<=<svg).*?(?=<\/svg>)")
        self.src_pattern = re.compile("(?<=src=).+(?= )", re.UNICODE)
        self.header_pattern = re.compile("(?<=<header).*?(?=<\/header>)")
        self.footer_pattern = re.compile("(?<=<footer).*?(?=<\/footer>)")
        self.fieldnames = [
            'name', 'url', 'industry', 'domain_name','imgs', 'svgs', 'text'
        ]
        self.writer = csv.DictWriter(self.write_file, fieldnames=self.fieldnames)
        self.writer.writeheader()

        self.to_text = html2text.HTML2Text()
        self.to_text.ignore_images = True
        self.to_text.ignore_tables = True
        self.to_text.ignore_links = True
        self.to_text.single_line_break = True


    def start_requests(self):
        for url, name, industry in zip(self.start_urls, self.names, self.industries):
            yield scrapy.Request(url, meta={
                'root_url': url,
                'name': name,
                'industry': industry
            })
    def parse(self, response):
        #hxs = scrapy.HtmlXPathSelector(response)
        
        html = response.body.decode('latin-1')
        url = response.request.url

        minified = html.replace('\n', '')
        imgs = self.img_pattern.findall(minified)
        svgs = self.svg_pattern.findall(minified)

        minified = self.header_pattern.sub(" ", minified)
        minified = self.footer_pattern.sub(" ", minified)
        text = self.to_text.handle(minified)

        curr_logos = []
        for img in imgs:
            if check_logo(img):
                src = get_src(img)
                if src is None:
                    continue
                if not src.startswith("http"):
                    src = "http://{}".format(url) + src
                curr_logos.append(src)
        curr_svgs = []
        for svg in svgs:
            if check_logo(svg):
                curr_svgs.append(svg)
        res =  {
                'name': response.meta['name'],
                'url': url,
                'domain_name': response.meta['root_url'],
                'industry': response.meta['industry'],
                'imgs': json.dumps(curr_logos),
                'svgs': json.dumps(curr_svgs),
                'text': text
            }
        self.writer.writerow(
           res
        )
        yield {
            'url': url,
            'status': True
        }


def start_spider(spider, urls, names, industries, n):
    process = multiprocessing.Process(target=crawler_func, args=(spider, urls, names, industries, n))
    process.start()
    return process


def main():
    ind = 0
    N = 500000
    start = time.time()
    df_clean = pd.read_csv('clean.csv')
    print("df LOAD TIME: {}".format(time.time()-start))
    domains = ['http://'+url for url in list(df_clean['domain'])][:N]
    names = list(df_clean['name'])[:N]
    industries = list(df_clean['industry'])[:N]

    n_processes = 8
    bs = len(df_clean) // n_processes
    batch_ind = [
        ((i-1)*bs, i*bs) for i in range(1, n_processes+1)
    ]

    start = time.time()

    processes = [
        start_spider(HTMLSpider, urls=domains[batch_ind[i][0]:batch_ind[i][1]], names=names[batch_ind[i][0]:batch_ind[i][1]], industries=industries[batch_ind[i][0]:batch_ind[i][1]], n=i)
        for i in range(n_processes)
    ]

    #process = CrawlerProcess(custom_settings)
    #process.crawl(HTMLSpider, urls=domains, names=names, industries=industries, n=1)
    #process.start()
    print(time.time() - start)


if __name__ == '__main__':
    main()
