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
    company_id = 16
    #url = "http://www.itouzi.com/dinvest/invest/index"
    url = "http://www.itouzi.com/dinvest/debt/index"
    request_headers = {'Referee': "http://www.itouzi.com", 'User-Agent': DEFAULT_UA}

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
        # 注意ul的class后面有个空格
        loans = loan_htm_parse.xpath("//ul[@class='invest-product-case-list mtn btn clearfix ']/li")
        if len(loans) > 0:
            for loan in loans:
                if not loan.xpath("div[@class='i-p-c-subscription']/ul[@class='i-p-c-s-detail']"):
                    continue
                href = str(loan.xpath("h2/a[@class='fl']/@href")[0])
                original_id = href.split("id=")[1]
                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    #loan_obj = Loan(company_id, original_id)
                    #loan_obj.schedule = str(loan.xpath("td[6]/text()")[0].encode("utf-8")).strip().replace("%", "")
                    #loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = "http://www.itouzi.com" + href
                    loan_obj.title = str(loan.xpath("h2/a[@class='fl']/text()")[0].encode("utf-8")).strip()
                    loan_obj.repayment_mothod = str(loan.xpath("p/span[2]/text()")[0].encode("utf-8"))\
                        .strip().replace("还款方式：", "")
                    loan_obj.borrow_amount = int(loan.xpath("p/span[3]/strong/text()")[0]) * 10000

                    loan_obj.rate = str(loan.xpath("p/span[5]/em[1]/text()")[0].encode("utf-8")).strip().replace("%", "")
                    period = str(loan.xpath("p/span[4]/strong/text()")[0].encode("utf-8")).strip()
                    if period.find(loan_obj.PERIOD_UNIT_DAY) > 0:
                        loan_obj.period = period.replace(loan_obj.PERIOD_UNIT_DAY, "")
                        loan_obj.period_unit = loan_obj.PERIOD_UNIT_DAY
                    else:
                        loan_obj.period = period.replace("个", "").replace(loan_obj.PERIOD_UNIT_MONTH, "")
                        loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH

                    # 这个进度这块还不确定，需等有标时检查一遍
                    if loan.xpath("div[@class='i-p-c-subscription']/div[@class='i-p-c-s-detail']"):
                        loan_obj.schedule = str(loan.xpath("div[@class='i-p-c-subscription']/div[@class='i-p-c-s-detail']/span[1]/span[last()]/text()")[0].encode("utf-8")).strip().replace("%", "")
                        print loan_obj.schedule
                    #loan_obj.db_create(db)
        #
        #    logger.info("company %s crawler loan: new size %s, update size %s", company_id, len(new_ids_set), len(update_ids_set))
        #
        ## db - 新抓取的 = 就是要下线的
        #off_ids_set = db_ids_set - online_ids_set
        #if off_ids_set:
        #    loan_obj = Loan(company_id)
        #    loan_obj.db_offline(db, off_ids_set)
        #    logger.info("company %s crawler loan: offline %s", company_id, len(off_ids_set))

    except:
        logger.error("url: %s xpath failed:%s", url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    crawl()
