#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html
from pygaga.simplejson import loads
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 19
    url = "https://www.qian360.com/bq/queryProductList.html?currentPage=1&pernum=12&type=0"
    request_headers = {'Referee': "https://www.qian360.com/tl/select.html", 'User-Agent': DEFAULT_UA}

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
        loans_json = loads(loan_htm, encoding="UTF-8")

        if loans_json["list"]:
            for i in range(0, len(loans_json["list"])):
                if int(loans_json["list"][i]["status"]) != 1:
                    continue
                original_id = str(loans_json["list"][i]["borrowId"])
                online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = str(loans_json["list"][i]["percent"])
                    loan_obj.cast = str(int(loans_json["list"][i]["accountYes"]))
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = "https://www.qian360.com/ti/detail.html?borrowId=%s" % original_id
                    loan_obj.title = loans_json["list"][i]["name"]
                    loan_obj.rate = str(loans_json["list"][i]["apr"])
                    loan_obj.period = str(loans_json["list"][i]["totalPeriod"])
                    loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH
                    loan_obj.borrow_amount = str(int(loans_json["list"][i]["account"]))
                    loan_obj.schedule = str(loans_json["list"][i]["percent"])
                    loan_obj.cast = str(int(loans_json["list"][i]["accountYes"]))
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
