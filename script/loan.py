#! /usr/bin/env python
# coding: utf-8


class Loan():

    PERIOD_UNIT_DAY = "天"
    PERIOD_UNIT_MONTH = "月"

    def __init__(self, company_id, original_id=""):
        self.company_id = company_id
        self.original_id = original_id
        self.href = ""
        self.title = ""
        self.description = ""
        self.borrow_amount = ""
        self.rate = ""
        self.period = ""
        self.period_unit = ""
        self.repayment = ""
        self.cast = ""
        self.schedule = ""

    def db_create(self, db):
        db.execute("insert into loan (company_id,original_id,url,title,description,borrow_amount,rate,period,period_unit,"
                  "repayment,cast,schedule,crawl_status,status,create_time,update_time) "
                   "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,now(),now())", self.company_id, self.original_id,
                   self.href, self.title, self.description, self.borrow_amount, self.rate, self.period, self.period_unit,
                   self.repayment, self.cast, self.schedule)

    def db_update(self, db):
        set_list = []
        if self.schedule:
            set_list.append("schedule='%s'" % self.schedule.replace("%", "%%"))
        if self.cast:
            set_list.append("cast='%s'" % self.cast)

        if len(set_list) > 0:
            set_list.append(" update_time=now()")
            sql = "update loan set %s where company_id='%s' and original_id='%s'" % (",".join(set_list),
                                                                                   self.company_id, self.original_id)
            db.execute(sql)

    def db_offline(self, db, off_ids_set):
        self.schedule = "100%%"
        sql = "update loan set status=1,schedule='%s',update_time=now() where company_id='%s' and original_id in (%s)" % \
              (self.schedule, self.company_id, ", ".join("'" + s + "'" for s in off_ids_set))
        db.execute(sql)