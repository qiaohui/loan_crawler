#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import lxml

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
REFEREE = "https://member.niwodai.com"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 23
    url = "https://member.niwodai.com/xiangmu/"
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
        htm_obj = parse_html(htm, encoding="utf-8")
        loan_size = int(str(htm_obj.xpath("//div[@class='biaoList']/table/tbody/tr[1]/th[last()]/text()")[0].encode("utf-8"))\
            .replace("共", "").replace("个标", "").strip())
        if loan_size > 0:
            page = loan_size / 10
            if loan_size % 10 > 0:
                page += 1
            for p in range(1, page+1):
                page_url = "https://member.niwodai.com/loan/loan.do?pageNo=%d&totalCount=%d" % (p, loan_size)
                page_html = download_page(page_url, request_headers)
                page_obj = parse_html(page_html, encoding="utf-8")
                loans = page_obj.xpath("//div[@class='biaoList']/table/tbody/tr")
                for loan in loans:
                    if lxml.html.tostring(loan).find("<th>") > 0:
                        continue
                    href = str(loan.xpath("td[1]/a/@href")[0])
                    original_id = href.replace(".html", "").split("/")[2]
                    if original_id:
                        online_ids_set.add(original_id)

                    if original_id in db_ids_set:
                        update_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.schedule = str(loan.xpath("td[5]/text()")[0].encode("utf-8")).strip().replace("%", "")
                        loan_obj.db_update(db)
                    else:
                        new_ids_set.add(original_id)

                        loan_obj = Loan(company_id, original_id)
                        loan_obj.href = REFEREE + href
                        loan_obj.title = str(loan.xpath("td[1]/a/text()")[0].encode("utf-8")).strip()
                        loan_obj.borrow_amount = str(loan.xpath("td[4]/em/text()")[0].encode("utf-8")).strip().replace(",", "")
                        loan_obj.rate = str(loan.xpath("td[2]/em/text()")[0].encode("utf-8")).strip().replace("%", "")
                        loan_obj.period = str(loan.xpath("td[3]/em/text()")[0].encode("utf-8")).strip()
                        loan_obj.period_unit = loan_obj.PERIOD_UNIT_DAY
                        loan_obj.schedule = str(loan.xpath("td[5]/text()")[0].encode("utf-8")).strip().replace("%", "")

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
