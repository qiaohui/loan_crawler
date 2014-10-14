#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download
from simplejson import loads
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
REFEREE = "http://www.he-pai.cn/investmentDetail/investmentDetails/index.do"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 24
    url = "http://www.he-pai.cn/investmentDetail/investmentDetails/ajaxInvmentList.do?pageNo=1"
    request_headers = {'Referee': REFEREE, 'User-Agent': DEFAULT_UA}

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
        htm_obj = loads(htm, encoding="utf-8")
        loans = htm_obj["list"]
        if len(loans) > 0:
            for loan in loans:
                if str(loan["bID_SCHD"]) == "100":
                    #放弃已经结束的
                    continue
                original_id = loan["lN_NO"]
                href = "http://www.he-pai.cn/investmentDetail/memberCenter/transferView.do?ln_no=" + original_id
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = str(loan["bID_SCHD"])
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = href
                    loan_obj.title = loan["lN_NM"]
                    loan_obj.borrow_amount = loan["lN_AMT"]
                    loan_obj.rate = loan["lN_RATE"]
                    loan_obj.period = loan["lN_TERM"]
                    loan_obj.period_unit = loan["lN_TERM_UNIT_DESC"]
                    loan_obj.schedule = str(loan["bID_SCHD"])
                    loan_obj.repayment = loan["pAY_METH_DESC"]

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
