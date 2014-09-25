#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback
import time

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
    company_id = 8
    url = "http://www.eloancn.com/new/loadAllTender.action?page=3&sidx=progress&sord=desc"
    request_headers = {'Referee': "http://www.eloancn.com", 'User-Agent': DEFAULT_UA}

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
        for p in range(1, 4):
            url = "http://www.eloancn.com/new/loadAllTender.action?page=%s" % p
            print url
            # 这个页面比较恶心，一个标的的属性不在一个div内
            loan_htm = download_page(url, request_headers)
            loan_htm_parse = parse_html(loan_htm, encoding="UTF-8")
            htm_1 = loan_htm_parse.xpath("//div[@class='lendtable']/dl/dd[@class='wd300 pdl10 fl']")
            htm_2 = loan_htm_parse.xpath("//div[@class='lendtable']/dl/dd[@class='wd140 fl']")
            htm_3 = loan_htm_parse.xpath("//div[@class='lendtable']/dl/dd[@class='wd130 fl pdl10']")
            htm_4 = loan_htm_parse.xpath("//div[@class='lendtable']/dl/dd[@class='wd130 fl']")

            loan_list = []
            for h1 in htm_1:
                loan_obj = Loan(company_id)
                loan_obj.title = str(h1.xpath("h3/a[@class='fl']/text()")[0].encode("utf-8"))
                loan_obj.href = str(h1.xpath("h3/a[@class='fl']/@href")[0]).replace(":80", "")
                loan_obj.original_id = loan_obj.href.split("=")[1]
                loan_list.append(loan_obj)
            for index, h2 in enumerate(htm_2):
                loan_list[index].borrow_amount = str(h2.xpath("p[@class='colorCb mt10']/text()")[0].encode("utf-8")).replace("￥","").replace(",","")
                loan_list[index].rate = str(h2.xpath("p[@class='colorE6']/span/text()")[0]).replace("%", "")
            for index, h3 in enumerate(htm_3):
                loan_list[index].period = str(h3.xpath("p/span/text()")[0].encode("utf-8")) + "个月"
                loan_list[index].repayment_mothod = str(h3.xpath("p[@class='']/text()")[0].encode("utf-8"))
            for index, h4 in enumerate(htm_4):
                loan_list[index].schedule = str(h4.xpath("p/span/em/text()")[0]).strip().replace("%", "")

            # 去掉已经满标的
            new_list = [i for i in loan_list if i.schedule != "100"]

            for loan in new_list:
                online_ids_set.add(loan.original_id)
                if loan.original_id in db_ids_set:
                    update_ids_set.add(loan.original_id)

                    loan.db_update(db)
                    print loan.original_id, loan.schedule
                else:
                    new_ids_set.add(loan.original_id)

                    loan.db_create(db)

            logger.info("company %s crawler loan: new size %s, update size %s", company_id, len(new_ids_set), len(update_ids_set))

            time.sleep(5)

        # db - 新抓取的 = 就是要下线的
        off_ids_set = db_ids_set - online_ids_set
        if off_ids_set:
            loan_obj_off = Loan(company_id)
            loan_obj_off.db_offline(db, off_ids_set)
            logger.info("company %s crawler loan: offline %s", company_id, len(off_ids_set))


    except:
        logger.error("url: %s xpath failed:%s", url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    crawl()
