#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import lxml

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
    company_id = 13
    url = "http://www.yirendai.com/loan/list/1?period=12&currRate=&amt=&progress="
    request_headers = {'Referee': "http://www.yirendai.com/loan/list/1", 'User-Agent': DEFAULT_UA}

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
        loan_htm_parse = parse_html(loan_htm, encoding="UTF-8")
        loans = loan_htm_parse.xpath("//ul[@class='bidList']/li")
        if len(loans) > 0:
            for loan in loans:
                if not loan.xpath("div[2]/div[2]/div/div[@class='bid_empty_errortip']"):
                    continue
                href = str(loan.xpath("div[2]/div/h3/a/@href")[0])
                original_id = href.split("/")[3]
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.borrow_amount = str(loan.xpath("div[2]/div/div[2]/h4/span/text()")[0].encode("utf-8"))\
                        .strip().replace(",", "")
                    loan_obj.cast = str(loan.xpath("div[2]/div/div[1]/p/text()")[0].encode("utf-8")).strip()\
                        .replace("元", "").split("已投")[1]
                    loan_obj.schedule = str(float(loan_obj.cast) / float(loan_obj.borrow_amount) * 100) + "%"
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = "http://www.yirendai.com" + href
                    loan_obj.title = str(loan.xpath("div[2]/div/h3/a/text()")[0].encode("utf-8")).strip()
                    loan_obj.borrow_amount = str(loan.xpath("div[2]/div/div[2]/h4/span/text()")[0].encode("utf-8"))\
                        .strip().replace(",", "")
                    loan_obj.rate = str(loan.xpath("div[2]/div/div[3]/h4/span/text()")[0].encode("utf-8")).strip()
                    loan_obj.loan_period = str(loan.xpath("div[2]/div/div[4]/h4/span/text()")[0].encode("utf-8")).strip() + "个月"
                    loan_obj.cast = str(loan.xpath("div[2]/div/div[1]/p/text()")[0].encode("utf-8")).strip()\
                        .replace("元", "").split("已投")[1]
                    loan_obj.schedule = str(float(loan_obj.cast) / float(loan_obj.borrow_amount) * 100) + "%"

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
