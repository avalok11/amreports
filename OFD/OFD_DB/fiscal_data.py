#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import RU_T_FISCAL_RECEIPT as r
import datetime


def main():
    today = datetime.datetime.today()
    if today.time() < datetime.time(4, 0, 0, 0):
        r.main(data_q=True)
    elif today.time() > datetime.time(4, 0, 0, 0):
        r.main(data_q=False)


if __name__ == "__main__":
    main()

