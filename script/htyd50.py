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
REFEREE = "http://www.htyd50.com/trade/hall.htm"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 26
    url = "http://www.htyd50.com/trade/borrow/bidding.htm"
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
        loans = htm_obj.xpath("//div[@class='page_block']/div[@class='page_block_content']/div[@class='min_height_300 mb_30']/div[@class='w980 clearfix']")
        print len(loans)
        if len(loans) > 0:
            for loan in loans:
                href = str(loan.xpath("div[2]/div[1]/div[1]/a/@href")[0].encode("utf-8"))
                original_id = href.replace(".html", "").split("/")[5].strip()
                print href, original_id
        #        if original_id:
        #            online_ids_set.add(original_id)
        #
        #        if original_id in db_ids_set:
        #            update_ids_set.add(original_id)
        #
        #            loan_obj = Loan(company_id, original_id)
        #            if loan.xpath("td[7]/div/a"):
        #                loan_obj.schedule = str(loan.xpath("td[7]/div/a/text()")[0].encode("UTF-8")).strip().replace("%", "")
        #            else:
        #                loan_obj.schedule = "0"
        #            loan_obj.db_update(db)
        #        else:
        #            new_ids_set.add(original_id)
        #
        #            loan_obj = Loan(company_id, original_id)
        #            loan_obj.href = "https://www.xinhehui.com" + href
        #            title_1 = str(loan.xpath("td[1]/p[1]/a/text()")[0].encode("utf-8")).strip()
        #            if loan.xpath("td[1]/p[1]/a/em"):
        #                title_2 = str(loan.xpath("td[1]/p[1]/a/em/text()")[0].encode("utf-8")).strip()
        #            else:
        #                title_2 = str(loan.xpath("td[1]/p[1]/a/span/text()")[0].encode("utf-8")).strip()
        #            loan_obj.title = title_1 + title_2
        #            borrow_amount = str(loan.xpath("td[2]/span/text()")[0].encode("utf-8")).strip().replace(" ", "")
        #            if borrow_amount.find("万") > 0:
        #                loan_obj.borrow_amount = float(borrow_amount.replace("万", "")) * 10000
        #            else:
        #                loan_obj.borrow_amount = float(borrow_amount.replace("元", "").replace(",", ""))
        #
        #            if loan.xpath("td[4]/span"):
        #                period = str(loan.xpath("td[4]/span/@title")[0].encode("UTF-8")).strip()
        #            else:
        #                period = str(loan.xpath("td[4]/text()")[0].encode("UTF-8")).strip()
        #            if period.find(loan_obj.PERIOD_UNIT_DAY) > 0:
        #                loan_obj.period = period.replace(loan_obj.PERIOD_UNIT_DAY, "")
        #                loan_obj.period_unit = loan_obj.PERIOD_UNIT_DAY
        #            else:
        #                loan_obj.period = period.replace("个", "").replace(loan_obj.PERIOD_UNIT_MONTH, "")
        #                loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH
        #
        #            loan_obj.rate = str(loan.xpath("td[3]/p/text()")[0]).strip().replace("%", "")
        #            loan_obj.repayment = str(loan.xpath("td[5]/text()")[0].encode("UTF-8")).strip()
        #            if loan.xpath("td[7]/div/a"):
        #                loan_obj.schedule = str(loan.xpath("td[7]/div/a/text()")[0].encode("UTF-8")).strip().replace("%", "")
        #            else:
        #                loan_obj.schedule = "0"
        #
        #            loan_obj.db_create(db)
        #
        #logger.info("company %s crawler loan: new size %s, update size %s", company_id, len(new_ids_set), len(update_ids_set))
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
