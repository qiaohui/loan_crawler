#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import re

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
REFEREE = "https://www.tzydb.com"
ID_RE = re.compile("\(([^)]*)\)")


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 11
    url = "https://www.tzydb.com"
    request_headers = {'User-Agent': DEFAULT_UA}

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
        htm = download_page(url, request_headers)
        htm_obj = parse_html(htm)
        loans = htm_obj.xpath("//div[@id='proList']/ul[@class='item_li']")
        if len(loans) > 0:
            for loan in loans:
                schedule = str(loan.xpath("li/div[last()]/div[1]/span[2]/strong/text()")[0].encode("UTF-8")).strip()
                if schedule == "100%" or schedule == "100.0%":
                    #放弃已经结束的
                    continue
                # link = https://www.tzydb.com/boot/lookup/971,1017
                a_script = str(loan.xpath("li/div[1]/div[1]/div/a/@href")[0].encode("utf-8"))
                o_id = ID_RE.findall(a_script)[0]
                original_id = o_id.replace(",", "-")
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = str(loan.xpath("li/div[last()]/div[1]/span[2]/strong/text()")[0].encode("UTF-8")).strip()
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)
                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = "https://www.tzydb.com/boot/lookup/" + o_id
                    loan_obj.title = str(loan.xpath("li/div[1]/div[1]/div/a/text()")[0].encode("utf-8"))
                    loan_obj.borrow_amount = str(loan.xpath("li/div[2]/div[1]/span/text()")[0].encode("utf-8")).strip()\
                        .replace(" ", "").replace(",", "")
                    loan_obj.loan_period = str(loan.xpath("li/div[2]/div[3]/span/text()")[0].encode("UTF-8")).strip() \
                                           + "个月"
                    loan_obj.rate = str(loan.xpath("li/div[2]/div[2]/span/text()")[0]).strip().replace("%", "")
                    loan_obj.schedule = str(loan.xpath("li/div[last()]/div[1]/span[2]/strong/text()")[0].encode("UTF-8")).strip()

                    loan_obj.db_create(db)

        logger.info("loan %s crawler: new size %s, update size %s", company_id, len(new_ids_set), len(update_ids_set))

        # db - 新抓取的 = 就是要下线的
        off_ids_set = db_ids_set - online_ids_set
        if off_ids_set:
            loan_obj = Loan(company_id)
            loan_obj.db_offline(db, off_ids_set)
            logger.info("loan %s crawler: offline %s", company_id, len(off_ids_set))

    except:
        logger.error("url: %s xpath failed:%s", url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    crawl()
