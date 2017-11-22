#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import RU_T_FISCAL_BULK_YUG as b
import datetime
import sys


def main():
    old_stdout = sys.stdout
    today1 = datetime.datetime.today()
    date1 = today1.isoformat()
    day = datetime.datetime.today().date().isoformat()
    hour = datetime.datetime.today().hour
    minute = datetime.datetime.today().minute
    filename = "logs\\message_"+day+"_"+str(hour)+"_"+str(minute)+".log"
    log_file = open(filename, "w")
    #sys.stdout = log_file
    print ("Start logs.")

    b.main(db_read_write=True, date_from='2017-10-01T00:00:00', date_to='2017-10-02T00:00:00')
    #, reg_id='0000747535020924', storage_id='8710000100763651') #, reg_id='0000735637027672', storage_id='8710000100977468')  #, reg_id='0000996815032543', storage_id='8710000100977628')0000996815032543 8710000100977628


    print ("Finished logs.")
    today2 = datetime.datetime.today()
    date2 = today2.isoformat()
    print ("downloading finished: ", date1, " .. ", date2)
    sys.stdout = old_stdout
    log_file.close()


if __name__ == "__main__":
    main()

