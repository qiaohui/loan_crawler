#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import lxml.html

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
    company_id = 26
    url = "http://www.longlongweb.com/invests"
    request_headers = {'Referee': "http://www.longlongweb.com", 'User-Agent': DEFAULT_UA}

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
        loan_htm_parse = parse_html(loan_htm, encoding="utf-8")
        loans = loan_htm_parse.xpath("//div[@class='main01']/span/dl")
        if len(loans) > 0:
            for loan in loans:
                if not lxml.html.tostring(loan.xpath("dd/p[1]")[0]).find("href") > 0:
                    continue

                href = str(loan.xpath("dd/table/tr[1]/td[1]/a/@href")[0])
                original_id = href.split("/")[2]
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.borrow_amount = str(loan.xpath("dd/table/tr[2]/td[1]/span/text()")[0].encode("utf-8"))\
                        .replace(",", "").replace("￥", "").strip()

                    loan_obj.href = "http://www.longlongweb.com" + href
                    loan_info_htm = download_page(loan_obj.href, request_headers)
                    loan_info_htm_parse = parse_html(loan_info_htm, encoding="utf-8")
                    loan_obj.cast = str(loan_info_htm_parse.xpath("//div[@class='zrll']/span[1]/text()")[0]
                                        .encode("utf-8")).replace(",", "").replace("￥", "").strip()
                    loan_obj.schedule = str(float(loan_obj.cast) / float(loan_obj.borrow_amount) * 100)
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = "http://www.longlongweb.com" + href
                    loan_obj.title = str(loan.xpath("dd/table/tr[1]/td[1]/a/@title")[0].encode("utf-8"))
                    loan_obj.rate = str(loan.xpath("dd/table/tr[2]/td[2]/span/text()")[0].encode("utf-8"))\
                        .replace("%", "")
                    loan_obj.borrow_amount = str(loan.xpath("dd/table/tr[2]/td[1]/span/text()")[0].encode("utf-8"))\
                        .replace(",", "").replace("￥", "").strip()
                    loan_obj.period = str(loan.xpath("dd/table/tr[2]/td[3]/span/text()")[0])
                    loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH

                    loan_info_htm = download_page(loan_obj.href, request_headers)
                    loan_info_htm_parse = parse_html(loan_info_htm, encoding="utf-8")
                    loan_obj.cast = str(loan_info_htm_parse.xpath("//div[@class='zrll']/span[1]/text()")[0]
                                        .encode("utf-8")).replace(",", "").replace("￥", "").strip()
                    loan_obj.schedule = str(float(loan_obj.cast) / float(loan_obj.borrow_amount) * 100)
                    loan_obj.repayment = str(loan_info_htm_parse.xpath("//div[@class='enterprise-botton']/span[2]/text()")[0]
                                             .encode("utf-8")).strip().replace("还款方式：", "")

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
