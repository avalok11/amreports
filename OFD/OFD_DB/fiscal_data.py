#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import RU_T_FISCAL_RECEIPT as r
import datetime
import sys


def main():
    old_stdout = sys.stdout
    today1 = datetime.datetime.today()
    date1 = today1.isoformat()
    day = datetime.datetime.today().date().isoformat()
    hour = datetime.datetime.today().hour
    minute = datetime.datetime.today().minute
    filename = "message_"+day+"_"+str(hour)+"_"+str(minute)+".log"
    log_file = open(filename, "w")
    #sys.stdout = log_file
    print ("Start logs.")
    #r.main(test=False, reg_id='0000083853048447', storage_id='8710000100099930', date_from='2017-06-11T00:00:00', hour_frame=12,
    #       date_to='2017-06-19T00:00:00') 0000734403026836 8710000100978624 date_from='2017-07-18T00:31:00', date_to='2017-07-18T16:39:00',
    r.main(test=True, reg_id='0001104870020004', storage_id='8710000100840306', date_from='2017-07-31T00:00:00',
           date_to='2017-08-03T23:59:59',
           send_to_sql=True, check_exist=True)
    #, reg_id='00106903709927', storage_id='0000582049063075', date_from='2017-06-27T10:00:00')

    #r.main(test=False, date_from='2017-06-14T00:00:00', date_to='2017-06-15T00:00:00')

    print ("Finished logs.")
    today2 = datetime.datetime.today()
    date2 = today2.isoformat()
    print "downloading finished: ", date1, " .. ", date2
    sys.stdout = old_stdout
    log_file.close()


if __name__ == "__main__":
    main()

