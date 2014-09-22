#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html
from simplejson import loads
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
REFEREE = "http://www.renrendai.com"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 12
    url = "http://www.renrendai.com/lend/loanList.action"
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
        htm_obj = parse_html(htm)
        loans_script = htm_obj.xpath("//script[@id='loan-list-rsp']/text()")[0].encode("utf-8")
        loans_json = loads(loans_script, encoding="UTF-8")
        loan_size = len(loans_json["data"]["loans"])
        if loan_size > 0:
            for i in range(0, loan_size):
                if loans_json["data"]["loans"][i]["status"] != "OPEN":
                    #放弃已经结束的
                    continue
                original_id = str(int(loans_json["data"]["loans"][i]["loanId"]))
                href = "http://www.renrendai.com/lend/detailPage.action?loanId=%s" % original_id
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = str(int(loans_json["data"]["loans"][i]["finishedRatio"])).split(".")[0] + "%"
                    loan_obj.cast = str(float(loans_json["data"]["loans"][i]["amount"]) - float(loans_json["data"]["loans"][i]["surplusAmount"]))
                    loan_obj.db_update(db)
                else:
                    pass
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = href
                    loan_obj.title = str(loans_json["data"]["loans"][i]["title"].encode("utf-8"))
                    loan_obj.borrow_amount = str(loans_json["data"]["loans"][i]["amount"])
                    loan_obj.loan_period = str(int(loans_json["data"]["loans"][i]["months"])) + "个月"
                    loan_obj.rate = str(loans_json["data"]["loans"][i]["interest"])
                    loan_obj.cast = str(float(loans_json["data"]["loans"][i]["amount"]) - float(loans_json["data"]["loans"][i]["surplusAmount"]))
                    loan_obj.schedule = str(int(loans_json["data"]["loans"][i]["finishedRatio"])).split(".")[0] + "%"
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
