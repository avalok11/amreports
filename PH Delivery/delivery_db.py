#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import pymysql
import pymssql
import validation
import datetime


def get_data_sales(day, cursor_ms, order_type_delivery='Delivery by courier'):
    '''

    :type order_type_delivery: object
    :param order_type_delivery:
    :param day:
    :param cursor_ms:
    :return:
    '''
    # подсчт данных для текущей даты
    # mpk, # нулевые доставки, # меньше 30 мин, 30-40, 40>, всего доставки, всего самовывоз
    cursor_ms.execute(
        'SELECT id_sap, %s, s0.d0, s30.d30, s3040.d3040, s40.d40, stotal.dtotal, sctotal.dctotal '
        '   FROM [iBase].[dbo].[units] '
        '   FULL OUTER JOIN '
        '       (SELECT CostCenter, count(DISTINCT OrderNum) d0 '
        '       FROM[iBase].[dbo].[IKO_SALES_DATA] '
        '           WHERE deliveryWayDuration = 0 AND OPENDATE = %s AND OrderType = %s '
        '       GROUP BY CostCenter) s0 '
        '   ON id_sap = s0.CostCenter '
        '   FULL OUTER JOIN '
        '       (SELECT CostCenter, count(DISTINCT OrderNum) d30'
        '       FROM[iBase].[dbo].[IKO_SALES_DATA]'
        '           WHERE deliveryWayDuration > 0 AND deliveryWayDuration <= 30 AND OPENDATE = %s '
        '       GROUP BY CostCenter) s30 '
        '   ON id_sap = s30.CostCenter '
        '   FULL OUTER JOIN '
        '       (SELECT CostCenter, count(DISTINCT OrderNum) d3040'
        '       FROM[iBase].[dbo].[IKO_SALES_DATA]'
        '           WHERE deliveryWayDuration > 30 AND deliveryWayDuration <= 40 AND OPENDATE = %s '
        '       GROUP BY CostCenter) s3040 '
        '   ON id_sap = s3040.CostCenter '
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, count(DISTINCT OrderNum) d40 '
        '       FROM[iBase].[dbo].[IKO_SALES_DATA]'
        '           WHERE deliveryWayDuration > 40 AND OPENDATE = %s '
        '       GROUP BY CostCenter) s40 '
        '   ON id_sap = s40.CostCenter'
        '   FULL OUTER JOIN '
        '       (SELECT CostCenter, count(DISTINCT OrderNum) dtotal '
        '       FROM[iBase].[dbo].[IKO_SALES_DATA]'
        '           WHERE OrderType=%s AND OPENDATE = %s '
        '       GROUP BY CostCenter) stotal '
        '   ON id_sap = stotal.CostCenter '
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, count(DISTINCT OrderNum) dctotal'
        '       FROM[iBase].[dbo].[IKO_SALES_DATA]'
        '           WHERE OrderType like N\'Доставка самовывоз\' AND OPENDATE = %s'
        '       GROUP BY CostCenter) sctotal '
        '       ON id_sap = sctotal.CostCenter '
        'WHERE unt_active = 1 AND unt_brand = 14 '
        'ORDER BY id_sap;', (day, day, order_type_delivery, day, day, day, order_type_delivery, day, day))
    return cursor_ms.fetchall()


def get_range_data_sl(cursor_my, ds, df):
    cursor_my.execute('SELECT sum(d0), sum(d30), sum(d3040), sum(d40), sum(dtotal), sum(dctotal), mpk, %s'
                      ' FROM ru_aop.ph_delivery'
                      ' WHERE date BETWEEN %s AND %s '
                      ' GROUP BY mpk;', (df, ds, df))
    return cursor_my.fetchall()


def save_data_sales(cursor_my, day_data):
    # записывам дневные даные в таблицу MySQL ph_delivery
    cursor_my.executemany('REPLACE INTO ph_delivery SET '
                          'mpk=%s, date=%s, d0=%s, d30=%s, d3040=%s, d40=%s, dtotal=%s, dctotal=%s;', day_data)


def update_week(cursor_my, data):
    cursor_my.executemany('UPDATE ph_delivery SET '
                          '  w0=%s, w30=%s, w3040=%s, w40=%s, wtotal=%s, wctotal=%s'
                          '  WHERE mpk=%s AND date=%s;', data)


def update_month(cursor_my, data):
    cursor_my.executemany('UPDATE ph_delivery SET '
                          '  m0=%s, m30=%s, m3040=%s, m40=%s, mtotal=%s, mctotal=%s'
                          '  WHERE mpk=%s AND date=%s;', data)


def update_year(cursor_my, data):
    cursor_my.executemany('UPDATE ph_delivery SET '
                          '  y0=%s, y30=%s, y3040=%s, y40=%s, ytotal=%s, yctotal=%s'
                          '  WHERE mpk=%s AND date=%s;', data)


def main():
    # make a connection to MySQL ru_bi RU server
    conn_my = pymysql.connect(host=validation.ip_mysql, user=validation.usr_my, password=validation.pwd_my,
                              db=validation.db_my, charset='utf8')
    cursor_my = conn_my.cursor()

    # make a connection to MSSQL iBase RU server
    conn_ms = pymssql.connect(host=validation.ip_mssql, user=validation.usr_ms, password=validation.pwd_ms,
                              database=validation.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()

    # ИИЦИАЛИЗАЦИЯ ДАТ: день, последний вторник, првое число теущего месяца и првое число текущего года
    today = (datetime.datetime.now() - datetime.timedelta(days=1))
    #today = datetime.datetime(2017, 4, 17)
    # day = (today - datetime.timedelta(days=1)).date().strftime("%Y-%m-%d")
    # print "Day - ", day
    print "Day - ", today.date()
    w = (today - (datetime.timedelta(days=((today - datetime.timedelta(days=1)).weekday()))))\
        .date().strftime("%Y-%m-%d")
    print "Week - ", w
    m = (str(today.year) + "-" + str(today.month) + "-01")
    print "Month - ", m
    y = (str(today.year) + "-01-01")
    print "Year - ", y
    # range of dates:
    interval = 9  # интервал дат воторы еобходимо считать, по умолчанию =7
    range_dates = list()
    for d in range(interval):
        range_dates.append((today - datetime.timedelta(days=d)).date().strftime("%Y-%m-%d"))
    print "range of days - ", range_dates

    # считывам даные из iBase IKO_SALES_DATA и подсчитываем кол-во транзакций
    days_data = list()
    for day in range_dates:
        row = get_data_sales(day, cursor_ms)
        days_data.append(row)

    # записываем дневные данные в базу ru_aop ph_delivery
    for day_data in days_data:
        save_data_sales(cursor_my, day_data)

    # обовляем таблицу даными week to dat, month to day и ear to day
    for day in range_dates:
        dat = datetime.datetime.strptime(day, "%Y-%m-%d")
        w = (dat - (datetime.timedelta(days=((dat - datetime.timedelta(days=1)).weekday()))))\
            .date().strftime("%Y-%m-%d")
        m = (str(dat.year) + "-" + str(dat.month) + "-01")
        # print "Day", day, w, m, y
        y = (str(dat.year) + "-01-01")
        week_data = get_range_data_sl(cursor_my, w, day)
        update_week(cursor_my, week_data)
        month_data = get_range_data_sl(cursor_my, m, day)
        year_data = get_range_data_sl(cursor_my, y, day)
        update_week(cursor_my, week_data)
        update_month(cursor_my, month_data)
        update_year(cursor_my, year_data)

    conn_my.commit()
    conn_my.close()
    conn_ms.close()

if __name__ == "__main__":
    main()
