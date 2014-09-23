#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import lxml.html
import HTMLParser

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('all', False, "update all shop")
gflags.DEFINE_integer('shopid', 0, "update shop id")
gflags.DEFINE_boolean('force', False, "is update offline shops?")
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl_wzdai():
    url = "https://www.wzdai.com/invest/index.html?status=1&page=1&order=-3"
    request_headers = {'Referee': "https://www.wzdai.com", 'User-Agent': DEFAULT_UA}

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    try:
        htm = download_page(url, request_headers)
        htm_obj = parse_html(htm)
        pages_obj = htm_obj.xpath("//div[@class='page']/div[@align='center']/span/text()")[0]
        page = int(str(pages_obj.encode("utf-8")).split("条")[1].split("页")[0])
        for p in range(1, page + 1):
            url = "https://www.wzdai.com/invest/index.html?status=1&page=" + str(p) + "&order=-3"
            logger.info(url)

            loan_htm = download_page(url, request_headers)
            loan_obj = parse_html(loan_htm)
            loans = loan_obj.xpath("//div[@class='invest_box']")
            if len(loans) > 0:
                for loan in loans:
                    href = "https://www.wzdai.com" + str(loan.xpath("h1/a[@class='del']/@href")[0])
                    title = loan.xpath("h1/a[@class='del']/text()")[0].strip().encode("UTF-8")
                    borrow_amount = str(loan.xpath("div[@class='invest_box_Info']/div[@class='prize']/span/b/text()")[0])
                    rate = str(loan.xpath("div[@class='invest_box_Info']/div[@class='prize']/font/b/text()")[0])
                    text = loan.xpath("div[@class='invest_box_Info']/div[@class='text']")
                    loan_period = ""
                    repayment_mothod = ""
                    for lp in text:
                        p = lxml.html.tostring(lp).strip().replace("\r\n", "").split("<br>")
                        html_parser = HTMLParser.HTMLParser()
                        loan_period = html_parser.unescape(p[0].replace('<div class="text">', "").strip()).encode("UTF-8").replace("借款期限：", "")
                        repayment_mothod = html_parser.unescape(p[1].strip()).encode("UTF-8").replace("还款方式：", "")

                    cast = loan.xpath("div[@class='invest_box_Info']/div[@class='text2']/text()")[0].strip()\
                        .encode("UTF-8").replace("已投：￥", "").replace("元","")
                    schedule = str(loan.xpath("div[@class='invest_box_Info']/div[@class='percent_big']/div[@class='percent_small']/font/text()")[0])

                    logger.info(href,title,borrow_amount,rate,cast,schedule,loan_period, repayment_mothod)

                    db = get_db_engine()
                    db.execute("insert into loan (company_id,url,title,borrow_amount,rate,loan_period,"
                              "repayment_mothod,cast,schedule,crawl_status,status,create_time,update_time) "
                               "values (1,%s,%s,%s,%s,%s,%s,%s,%s,0,0,now(),now())", href, title, borrow_amount,
                               rate,loan_period,repayment_mothod,cast,schedule)

    except:
        logger.error("url: %s xpath failed:%s", url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    crawl_wzdai()
