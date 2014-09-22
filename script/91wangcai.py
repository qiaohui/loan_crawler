#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import lxml.html
import HTMLParser

from pygaga.helpers.i18n import autodecode
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"

def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)

def crawl():
    company_id = 3
    url = "http://www.91wangcai.com/invest/index.html"
    request_headers = {'Referee': "http://www.91wangcai.com", 'User-Agent': DEFAULT_UA}

    db = get_db_engine()
    db_ids = list(db.execute("select original_id from loan where company_id=%s and status=0", company_id))
    # db all
    db_ids_set = set()
    # 在线的所有id
    online_ids_set = set()
    # new
    new_ids_set = set()
    # update
    update_ids_set = set()

    for id in db_ids:
        db_ids_set.add(id[0].encode("utf-8"))

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    try:
        loan_htm = download_page(url, request_headers)
        loan_htm_parse = parse_html(loan_htm, encoding="gb2312")
        loans = loan_htm_parse.xpath("//div[@class='proBoxNew']")

        if len(loans) > 0:
            for loan in loans:
                href = str(loan.xpath("div[@class='hd']/a/@href")[0])
                original_id = href.split(".")[0].split("/")[2].encode("utf-8")
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = autodecode(str(loan.xpath("div[@class='bd']/table/tr[2]/td[2]/text()")[0].encode("gb2312"))) \
                        .encode("utf-8").replace("融资进度：", "").strip()
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id)
                    loan_obj.original_id = original_id
                    loan_obj.href = "http://www.91wangcai.com" + href
                    loan_obj.title = autodecode(str(loan.xpath("div[@class='hd']/a/text()")[0].encode("gb2312"))).encode("utf-8")

                    loan_obj.borrow_amount = autodecode(str(loan.xpath("div[@class='bd']/table/tr[1]/td[1]/em/text()")[0].encode("gb2312"))) \
                        .encode("utf-8").replace("￥", "")

                    loan_obj.rate = str(loan.xpath("div[@class='bd']/table/tr[1]/td[2]/em/text()")[0]).strip().replace("%", "")

                    loan_period_text = lxml.html.tostring(loan.xpath("div[@class='bd']/table/tr[1]/td[3]/*")[0]) \
                        .replace("<em>", "").replace("</em>", "")
                    html_parser = HTMLParser.HTMLParser()
                    loan_obj.loan_period = html_parser.unescape(loan_period_text).encode("utf-8").strip()

                    loan_obj.repayment_mothod = autodecode(str(loan.xpath("div[@class='bd']/table/tr[2]/td[1]/text()")[0].encode("gb2312"))) \
                        .encode("utf-8").replace("还款方式：", "")

                    loan_obj.schedule = autodecode(str(loan.xpath("div[@class='bd']/table/tr[2]/td[2]/text()")[0].encode("gb2312"))) \
                        .encode("utf-8").replace("融资进度：", "").strip()

                    loan_obj.db_create(db)

            logger.info("company %s crawler loan: new size %s, update size %s", company_id, len(new_ids_set), len(update_ids_set))

        # db - 新抓取的 = 就是要下线的
        off_ids_set = db_ids_set - online_ids_set
        if off_ids_set:
            loan_obj = Loan(company_id)
            loan_obj.db_offline(db, off_ids_set)
            logger.info("company %s crawler loan: offline %s", company_id, len(off_ids_set))

    except:
        logger.error("url: %s xpath failed:%s", url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    crawl()
