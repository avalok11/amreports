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
    #r.main(test=False, reg_id='0000083853048447', storage_id='8710000100099930', date_from='2017-06-11T00:00:00',
    #       date_to='2017-06-19T00:00:00')
    r.main(test=False, storage_id='8710000100845840', reg_id='0000773723059118',
           date_from='2017-07-03T12:00:00', date_to='2017-07-03T13:00:00', send_to_sql=True, check_exist=True)
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

