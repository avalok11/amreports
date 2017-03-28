#!/usr/local/bin/python2
# -*- coding: utf-8 -*-


def transaction(cursor_my, units, ds, df):
    cursor_my.execute(
        'SELECT unit, sum(count) FROM ru_aop.ru_tra WHERE unit in %s AND datas between %s and %s'
        'GROUP BY unit;', (units, ds, df))
    return list(cursor_my.fetchall())
