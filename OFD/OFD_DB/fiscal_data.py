#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import RU_T_FISCAL_RECEIPT as r
import datetime


def main():
    r.main(test=False, date_from='2017-06-19T00:00:00', date_to='2017-06-20T00:00:00')

    #r.main(test=False, date_from='2017-06-14T00:00:00', date_to='2017-06-15T00:00:00')


if __name__ == "__main__":
    main()

