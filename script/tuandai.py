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
REFEREE = "http://www.tuandai.com/pages/invest/invest_list.aspx"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 25
    url = "http://www.tuandai.com/pages/ajax/invest_list.ashx?Cmd=GetInvest_List" \
          "&RepaymentTypeId=0&pagesize=5&pageindex=%s&type=3&status=1&DeadLine=0" \
          "&beginDeadLine=0&endDeadLine=0&rate=0&beginRate=0&endRate=0&strkey=&orderby=0"
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
        htm = download_page((url % 1), request_headers)
        obj = loads(htm, encoding="utf-8")
        total = int(obj["total"])
        if total > 0:
            page = total / 5
            if total % 5 > 0:
                page += 1
            for p in range(1, page+1):
                htm = download_page((url % p), request_headers)
                htm_obj = loads(htm, encoding="utf-8")
                loans = htm_obj["projectList"]
                for loan in loans:
                    original_id = loan["ID"]
                    href = "http://www.tuandai.com/pages/invest/jing_detail.aspx?id=" + original_id
                    if original_id:
                        online_ids_set.add(original_id)

                    if original_id in db_ids_set:
                        update_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.schedule = str(loan["ProgressPercent"])
                        loan_obj.cast = loan["ProgressAmount"]
                        loan_obj.db_update(db)
                    else:
                        new_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.href = href
                        loan_obj.title = loan["Title"]
                        loan_obj.borrow_amount = loan["TotalAmount"]
                        loan_obj.rate = loan["YearRate"]
                        loan_obj.period = loan["Deadline"]
                        loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH
                        loan_obj.schedule = str(loan["ProgressPercent"])
                        loan_obj.cast = loan["ProgressAmount"]
                        loan_obj.repayment = loan["RepaymentTypeDesc"]

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
