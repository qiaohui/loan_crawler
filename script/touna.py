#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import time
from simplejson import loads

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
    company_id = 17
    #url = "http://www.touna.cn/borrow.do?method=list&borrowType=0&creditType=&timeLimit=&keyType=0&page=1&size=10&subtime=1411910662511&_=1411910662512"
    s = int(time.time() * 1000)
    e = s + 1
    url = "http://www.touna.cn/borrow.do?method=list&borrowType=0&creditType=&timeLimit=&keyType=0&page=%d&size=10&subtime=%d&_=%d"
    url_1 = url % (0, s, e)
    request_headers = {'Referee': "http://www.touna.cn/invest-list.html", 'User-Agent': DEFAULT_UA}

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
        htm = download_page(url_1, request_headers)
        htm_json = loads(htm, encoding="UTF-8")
        page_count = htm_json["result"]["pages"]["count"]
        page = page_count / 10
        if page_count % 10 > 0:
            page += 1
        if page > 0:
            for p in range(0, page):
                # 重新计算当前时间
                s = int(time.time() * 1000)
                e = s + 1
                page_url = url % (p, s, e)
                loan_htm = download_page(page_url, request_headers)
                loans_json = loads(loan_htm, encoding="UTF-8")
                loans = loans_json["result"]["list"]
                for loan in loans:
                    original_id = str(loan["id"])
                    if original_id:
                        online_ids_set.add(original_id)
                    if original_id in db_ids_set:
                        update_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.schedule = str(loan["score"])
                        loan_obj.db_update(db)
                    else:
                        new_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.href = "http://www.touna.cn/invest-page.html?id=%d" % int(original_id)
                        loan_obj.title = loan["name"]
                        loan_obj.borrow_amount = loan["account"]
                        loan_obj.rate = loan["apr"]
                        loan_obj.schedule = str(loan["score"])
                        loan_obj.repayment = loan["style_name"]
                        period = str(loan["time_limit_name"].encode("utf-8"))
                        if period.find(loan_obj.PERIOD_UNIT_DAY) > 0:
                            loan_obj.period = period.replace(loan_obj.PERIOD_UNIT_DAY, "")
                            loan_obj.period_unit = loan_obj.PERIOD_UNIT_DAY
                        else:
                            loan_obj.period = period.replace("个", "").replace(loan_obj.PERIOD_UNIT_MONTH, "")
                            loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH
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
