#! /usr/bin/env python
# coding: utf-8
import urllib2

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html
import zlib
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 20
    url = "http://www.xiaomabank.com/finance.do"
    request_headers = {'Referee': "http://www.xiaomabank.com", 'User-Agent': DEFAULT_UA}

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
        loans = loan_htm_parse.xpath("//div[@class='pil_main']/table[@class='pil_table']/tbody/tr")
        if len(loans) > 0:
            for loan in loans:
                href = str(loan.xpath("td[1]/a/@href")[0])
                original_id = href.split("=")[1].encode("utf-8")

                if original_id:
                    online_ids_set.add(original_id)

                if original_id in db_ids_set:
                    update_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.schedule = str(loan.xpath("td[6]/div[1]/div/@style")[0].encode("utf-8")).replace("width:", "").strip().replace("%;", "")
                    loan_obj.db_update(db)
                else:
                    new_ids_set.add(original_id)

                    loan_obj = Loan(company_id, original_id)
                    loan_obj.href = "http://www.xiaomabank.com/" + href
                    loan_obj.title = str(loan.xpath("td[1]/a/text()")[0].encode("utf-8")).strip()
                    loan_obj.borrow_amount = str(loan.xpath("td[5]/strong/text()")[0].encode("utf-8")).strip().replace(",", "")
                    loan_obj.rate = str(loan.xpath("td[3]/text()")[0].encode("utf-8")).strip().replace("%", "")
                    loan_obj.period = str(loan.xpath("td[4]/text()")[0].encode("utf-8")).replace("个月", "").strip()
                    loan_obj.period_unit = loan_obj.PERIOD_UNIT_MONTH
                    loan_obj.schedule = str(loan.xpath("td[6]/div[1]/div/@style")[0].encode("utf-8")).replace("width:", "").strip().replace("%;", "")

                    # 注意这里页面返回的gzip压缩后的，需要解压
                    resp = urllib2.urlopen(loan_obj.href)
                    respInfo = resp.info()
                    if(("Content-Encoding" in respInfo) and (respInfo['Content-Encoding'] == "gzip")):
                        respHtml = zlib.decompress(resp.read(), 16+zlib.MAX_WBITS)
                        info_htm_parse = parse_html(respHtml, encoding="utf-8")
                        loan_obj.repayment = str(info_htm_parse.xpath("//div[@id='pi_lt_bottom']/div[1]/div[1]/a/text()")[0].encode("utf-8"))

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
