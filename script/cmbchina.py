#! /usr/bin/env python
# coding: utf-8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html, post
from pygaga.simplejson import loads
from loan import Loan

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"


def download_page(url, headers, max_retry_count=5):
    return download(url, headers, max_retry=max_retry_count, throw_on_banned=True)


def crawl():
    company_id = 19
    url = "https://efinance.cmbchinaucs.com/Handler/ActionPage.aspx?targetAction=GetProjectList_Index"
    headers = {'Host': "efinance.cmbchinaucs.com",
                        'Connection': "keep-alive",
                        'Content-Length': "33",
                        'Cache-Control': "max-age=0",
                        'Accept': "text/plain, */*",
                        'Origin': "https://efinance.cmbchinaucs.com",
                        'X-Requested-With': "XMLHttpRequest",
                        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36",
                        'Content-Type': "application/x-www-form-urlencoded",
                        'Referer': "https://efinance.cmbchinaucs.com/",
                        'Accept-Encoding': "gzip,deflate",
                        'Accept-Language': "zh-CN,zh;q=0.8,en;q=0.6",
                        'Cookie': "ASP.NET_SessionId=woqbxpemqp3kk4syvfbkxtzw"}

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
        loan_htm = post(url, data={"targetAction": "GetProjectList_Index"}, headers=headers)
        loans_json = loads(loan_htm, encoding="UTF-8")
        print loans_json

    except:
        logger.error("url: %s xpath failed:%s", url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    crawl()
