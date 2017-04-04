#!/usr/local/bin/python2
# -*- coding: utf-8 -*-


import excels
import mail_send


def main():
    excels.promo_index_avg()
    mail_send.main("Promo Shefburger.xls", 'PROMO SHEF')

if __name__ == "__main__":
    main()