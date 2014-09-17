#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback

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
    company_id = 4
    url = "https://www.yinhu.com/loan/loan_list.bl"
    request_headers = {'Referee': "https://www.yinhu.com", 'User-Agent': DEFAULT_UA}

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
    # offline
    off_ids_set = set()


    for id in db_ids:
        db_ids_set.add(id[0].encode("utf-8"))

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    try:
        loan_htm = download_page(url, request_headers)
        loan_htm_parse = parse_html(loan_htm, encoding="UTF-8")
        loans = loan_htm_parse.xpath("//div[@id='loan_list']/table/tbody/tr")
        if len(loans) > 0:
            for loan in loans:
                href = str(loan.xpath("td[1]/p/a/@href")[0])
                original_id = href.split("=")[1].encode("utf-8")
                try:
                    loan_status = str(loan.xpath("td[last()]/em/span/text()")[0].encode("utf-8")).strip()
                except:
                    loan_status = str(loan.xpath("td[last()]/a/span/text()")[0].encode("utf-8")).strip()

                if original_id and loan_status != "还款中":
                    online_ids_set.add(original_id)

                if loan_status == "还款中":
                    if original_id in db_ids_set:
                        off_ids_set.add(original_id)
                    continue

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = str(loan.xpath("td[6]/div[@class='bar_bg']/div/span/span/text()")[0].encode("utf-8"))\
                        .strip()
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id)
                    loan_obj.original_id = original_id
                    loan_obj.href = "https://www.yinhu.com" + href
                    loan_obj.title = str(loan.xpath("td[1]/p/a/text()")[0].encode("utf-8")).strip()
                    loan_obj.borrow_amount = str(loan.xpath("td[4]/text()")[0].encode("utf-8")).strip().replace(",", "")\
                        .replace("元", "")

                    loan_obj.rate = str(loan.xpath("td[3]/text()")[0].encode("utf-8")).strip()
                    loan_obj.loan_period = str(loan.xpath("td[5]/text()")[0].encode("utf-8")).strip()

                    loan_obj.schedule = str(loan.xpath("td[6]/div[@class='bar_bg']/div/span/span/text()")[0].encode("utf-8"))\
                        .strip()

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
