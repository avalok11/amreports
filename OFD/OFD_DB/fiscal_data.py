#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import RU_T_FISCAL_RECEIPT as r
import datetime


def main():
    #r.main(test=False, reg_id='0000083853048447', storage_id='8710000100099930', date_from='2017-06-11T00:00:00',
    #       date_to='2017-06-19T00:00:00')
    r.main(test=False, date_from='2017-06-22T00:00:00',
           date_to='2017-06-25T00:00:00')

    #r.main(test=False, date_from='2017-06-14T00:00:00', date_to='2017-06-15T00:00:00')


if __name__ == "__main__":
    main()

