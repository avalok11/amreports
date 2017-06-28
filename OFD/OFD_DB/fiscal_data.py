#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import RU_T_FISCAL_RECEIPT as r
import datetime
import sys


def main():
    old_stdout = sys.stdout
    today = datetime.datetime.today().date().isoformat()
    filename = "message_"+today+".log"
    log_file = open(filename, "w")
    sys.stdout = log_file
    print "this will be written to message.log1"

    #r.main(test=False, reg_id='0000083853048447', storage_id='8710000100099930', date_from='2017-06-11T00:00:00',
    #       date_to='2017-06-19T00:00:00')
    r.main(test=False, date_from='2017-06-24T00:00:00', date_to='2017-06-26T00:00:00', send_to_sql=False)
    #, reg_id='0000580632035623', storage_id='8710000100537947', date_from='2017-06-27T10:00:00')

    #r.main(test=False, date_from='2017-06-14T00:00:00', date_to='2017-06-15T00:00:00')

    print "this will be written to message.log2"
    sys.stdout = old_stdout
    log_file.close()


if __name__ == "__main__":
    main()

