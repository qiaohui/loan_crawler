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
    company_id = 18
    url = "https://www.my089.com/Loan/default.aspx?pid=1"
    request_headers = {'Referee': "http://www.ppdai.com", 'User-Agent': DEFAULT_UA}

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
        page = str(htm_obj.xpath("//div[@class='yema rt']/span[@class='z_page']/text()")[0].encode("UTF-8"))\
            .replace("共", "").replace("页", "")
        for p in range(1, int(page) + 1):
            url = "https://www.my089.com/Loan/default.aspx?pid=" + str(p)
            logger.info("page url: %s", url)

            loan_htm = download_page(url, request_headers)
            loan_obj = parse_html(loan_htm)
            loans = loan_obj.xpath("//div[@class='Loan_box']/dl[@class='LoanList']")
            if len(loans) > 0:
                for loan in loans:
                    if str(loan.xpath("dd[last()]/p/span/text()")[0]) == "100%":
                        continue
                    href = str(loan.xpath("dd[2]/div[@class='txt_tou']/a/@href")[0])
                    original_id = href.split("=")[1].encode("utf-8")
                    if original_id:
                        online_ids_set.add(original_id)

                    if original_id in db_ids_set:
                        update_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.schedule = str(loan.xpath("dd[last()]/p/span/text()")[0].encode("UTF-8")).strip().replace("%", "")
                        loan_obj.db_update(db)
                    else:
                        new_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.href = "https://www.my089.com/Loan/" + href
                        loan_obj.title = str(loan.xpath("dd[2]/div[@class='txt_tou']/a/@title")[0].encode("UTF-8"))
                        loan_obj.borrow_amount = str(loan.xpath("dd[4]/span/text()")[0].encode("UTF-8")).strip().replace("￥", "")\
                            .replace(",", "")
                        loan_obj.rate = str(loan.xpath("dd[3]/span/text()")[0].encode("UTF-8")).strip().replace("%/年", "")
                        loan_obj.period = str(loan.xpath("dd[5]/span/text()")[0].encode("UTF-8")).strip().replace(" ", "")
                        s = str(loan.xpath("dd[5]/text()")[0].encode("UTF-8")).strip().replace(" ", "").replace("个", "")
                        loan_obj.period_unit = s.split("/")[0].strip()
                        loan_obj.repayment = s.split("/")[1].strip()
                        loan_obj.schedule = str(loan.xpath("dd[last()]/p/span/text()")[0].encode("UTF-8")).strip().replace("%", "")
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
