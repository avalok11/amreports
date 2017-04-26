#!/usr/local/bin/python2
# -*- coding: utf-8 -*-


import pymysql
import pymssql
import validation
import datetime


def get_data_trans(day, day_ly, cursor_ms):
    '''

    :param day_ly:
    :param order_type_delivery:
    :param day:
    :param cursor_ms:
    :return:
    '''
    # подсчет данных для текущей даты
    # mpk, всего продажи, дайн ин продажи, дайн ин продажи прошлый год, делко продажи, вынос продажи
    # транзакции всего, дайн ин транзакции, дайн ин прошлый год, делко транзакции, вынос транзакции
    cursor_ms.execute(
        'SELECT id_sap, %s, sales.sl, din.dinsl, din_ly.dinsl_ly, delco.ddelcsl, carry.dcarsl, trans.tr, '
        '       dintr.dintr, dinintr_ly.dintr_ly, delcotr.ddecotr, dcarrytr.dcartr'
        '   FROM[iBase].[dbo].[units]'
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, sum(DishDiscountSumIntwithoutVAT) sl'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s'
        '       GROUP BY CostCenter, OpenDate) sales'
        '   ON sales.CostCenter = id_sap'
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, sum(DishDiscountSumIntwithoutVAT) dinsl'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s AND (OrderType not like N\'Delivery by courier\''
        '                                   AND OrderType not like N\'Доставка самовывоз\')'
        '       GROUP BY CostCenter, OpenDate) din'
        '   ON din.CostCenter = id_sap'
        '   FULL OUTER JOIN'
        '       (SELECT unit, sum(net) dinsl_ly'
        '       FROM ru_det'
        '       WHERE datas = %s AND outdoor = 0 '
        '       GROUP BY unit) din_ly'
        '   ON din_ly.unit = id_sap '
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, sum(DishDiscountSumIntwithoutVAT) ddelcsl'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s AND OrderType like N\'Delivery by courier\' '
        '       GROUP BY CostCenter, OpenDate) delco'
        '   ON delco.CostCenter = id_sap'
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, sum(DishDiscountSumIntwithoutVAT) dcarsl'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s AND OrderType like N\'Доставка самовывоз\''
        '       GROUP BY CostCenter, OpenDate) carry'
        '   ON carry.CostCenter = id_sap '
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, count(DISTINCT OrderNum) tr'
        '       FROM IKO_SALES_DATA WHERE OpenDate = %s'
        '       GROUP BY CostCenter, OpenDate) trans'
        '   ON trans.CostCenter = id_sap'
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, count(DISTINCT OrderNum) dintr'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s AND (OrderType not like N\'Delivery by courier\' '
        '                                   AND OrderType not like N\'Доставка самовывоз\') '
        '       GROUP BY CostCenter, OpenDate) dintr'
        '   ON dintr.CostCenter = id_sap '
        '   FULL OUTER JOIN'
        '       (SELECT unit, sum(count) dintr_ly'
        '       FROM ru_tra'
        '       WHERE datas = %s AND outdoor = 0'
        '       GROUP BY unit) dinintr_ly'
        '   ON dinintr_ly.unit = id_sap'
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, count(DISTINCT OrderNum) ddecotr'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s AND OrderType like N\'Delivery by courier\' '
        '       GROUP BY CostCenter, OpenDate) delcotr'
        '   ON delcotr.CostCenter = id_sap '
        '   FULL OUTER JOIN'
        '       (SELECT CostCenter, OpenDate, count(DISTINCT OrderNum) dcartr'
        '       FROM IKO_SALES_DATA'
        '       WHERE OpenDate = %s AND OrderType like N\'Доставка самовывоз\' '
        '       GROUP BY CostCenter, OpenDate) dcarrytr'
        '   ON dcarrytr.CostCenter = id_sap '
        'WHERE unt_brand = 14 and unt_active = 1 '
        'ORDER BY id_sap',
        (day, day, day, day_ly, day, day, day, day, day_ly, day, day))
    return cursor_ms.fetchall()


def update_week_data(cursor_my, ds, df):
    cursor_my.execute('UPDATE ph_sl_tr as dest, (SELECT mpk, sum(sl) s_sl, sum(dinsl) sdinsl, sum(dinsl_ly) sdinsl_ly, '
                      '     sum(ddelcsl) sddelcsl, sum(dcarsl) sdcarsl, sum(tr) str, sum(dintr) sdintr, '
                      '     sum(dintr_ly) sdintr_ly, sum(ddelctr) sddelctr, sum(dcartr) sdcatr '
                      '         FROM ph_sl_tr '
                      '         WHERE day BETWEEN %s AND %s GROUP BY mpk) as src'
                      '     SET '
                      '         wsl =      src.s_sl, '
                      '         winsl =    src.sdinsl, '
                      '         winsl_ly = src.sdinsl_ly, '
                      '         wdelcsl =  src.sddelcsl, '
                      '         wcarsl =   src.sdcarsl,'
                      '         wtr =      src.str, '
                      '         wintr =    src.sdintr, '
                      '         wintr_ly = src.sdintr_ly,'
                      '         wdelctr =  src.sddelctr,'
                      '         wcartr =   src.sdcatr'
                      '     WHERE dest.mpk=src.mpk and dest.day=%s;', (ds, df, df))
    return cursor_my.fetchall()


def update_month_data(cursor_my, ds, df):
    cursor_my.execute('UPDATE ph_sl_tr as dest, (SELECT mpk, sum(sl) s_sl, sum(dinsl) sdinsl, sum(dinsl_ly) sdinsl_ly, '
                      '     sum(ddelcsl) sddelcsl, sum(dcarsl) sdcarsl, sum(tr) str, sum(dintr) sdintr, '
                      '     sum(dintr_ly) sdintr_ly, sum(ddelctr) sddelctr, sum(dcartr) sdcatr '
                      '         FROM ph_sl_tr '
                      '         WHERE day BETWEEN %s AND %s GROUP BY mpk) as src'
                      '     SET '
                      '         msl =      src.s_sl, '
                      '         minsl =    src.sdinsl, '
                      '         minsl_ly = src.sdinsl_ly, '
                      '         mdelcsl =  src.sddelcsl, '
                      '         mcarsl =   src.sdcarsl,'
                      '         mtr =      src.str, '
                      '         mintr =    src.sdintr, '
                      '         mintr_ly = src.sdintr_ly,'
                      '         mdelctr =  src.sddelctr,'
                      '         mcartr =   src.sdcatr'
                      '     WHERE dest.mpk=src.mpk and dest.day=%s;', (ds, df, df))
    return cursor_my.fetchall()


def update_year_data(cursor_my, ds, df):
    cursor_my.execute('UPDATE ph_sl_tr as dest, (SELECT mpk, sum(sl) s_sl, sum(dinsl) sdinsl, sum(dinsl_ly) sdinsl_ly, '
                      '     sum(ddelcsl) sddelcsl, sum(dcarsl) sdcarsl, sum(tr) str, sum(dintr) sdintr, '
                      '     sum(dintr_ly) sdintr_ly, sum(ddelctr) sddelctr, sum(dcartr) sdcatr '
                      '         FROM ph_sl_tr '
                      '         WHERE day BETWEEN %s AND %s GROUP BY mpk) as src'
                      '     SET '
                      '         ysl =      src.s_sl, '
                      '         yinsl =    src.sdinsl, '
                      '         yinsl_ly = src.sdinsl_ly, '
                      '         ydelcsl =  src.sddelcsl, '
                      '         ycarsl =   src.sdcarsl,'
                      '         ytr =      src.str, '
                      '         yintr =    src.sdintr, '
                      '         yintr_ly = src.sdintr_ly,'
                      '         ydelctr =  src.sddelctr,'
                      '         ycartr =   src.sdcatr'
                      '     WHERE dest.mpk=src.mpk and dest.day=%s;', (ds, df, df))
    return cursor_my.fetchall()


def save_data_tr(cursor_my, day_data):
    # записывам дневные даные в таблицу MySQL ph_delivery
    cursor_my.executemany('REPLACE INTO ph_sl_tr SET '
                          'mpk=%s, day=%s, sl=%s, dinsl=%s, dinsl_ly=%s, ddelcsl=%s, dcarsl=%s, '
                          'tr=%s, dintr=%s, dintr_ly=%s, ddelctr=%s, dcartr=%s;', day_data)


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
    print "Day - ", today.date()
    # определение даты дня дели прошлого года (проверка а высокосый год)
    if today.year%4 == 0 or (today.year-1) == 0:
        day_ly = (today - datetime.timedelta(days=365))
    else:
        day_ly = (today - datetime.timedelta(days=364))
    print "Previous year date - ", day_ly
    # определяем даты за которые мы будет вытаскивать данные из MSSQL базы (текущий и прошлый год)
    interval = 9  # интервал дат которые необходимо считать данные, по умолчанию =2, текущий и предыдущший год
    range_dates = list()
    for d in range(interval):
        range_dates.append(((today - datetime.timedelta(days=d)).date().strftime("%Y-%m-%d"),
                            (day_ly - datetime.timedelta(days=d)).date().strftime("%Y%m%d")))
    print "range of days - ", range_dates

    # считывам даные из iBase IKO_SALES_DATA + ru_det + ru_tra и подсчитываем кол-во транзакций
    # mpk, всего продажи, дайн ин продажи, дайн ин продажи прошлый год, делко продажи, вынос продажи
    # транзакции всего, дайн ин транзакции, дайн ин прошлый год, делко транзакции, вынос транзакции
    days_data = list()
    for day in range_dates:
        # print day[0], day[1]
        # print "Days", day
        row = get_data_trans(day[0], day[1], cursor_ms)
        days_data.append(row)

    # записываем подневные данные в базу ru_aop ph_sl_tr
    for day_data in days_data:
        # print day_data
        save_data_tr(cursor_my, day_data)
    conn_my.commit()

    # обовляем таблицу данные week to date для всех дней
    for day in range_dates:
        dat = datetime.datetime.strptime(day[0], "%Y-%m-%d")
        wstart = (dat - (datetime.timedelta(days=((dat - datetime.timedelta(days=1)).weekday()))))\
            .date().strftime("%Y-%m-%d")
        update_week_data(cursor_my, wstart, day[0])

    # обовляем таблицу данные month to date для всех дней из заданного диапазона
    for day in range_dates:
        dat = datetime.datetime.strptime(day[0], "%Y-%m-%d")
        mstart = (str(dat.year) + "-" + str(dat.month) + "-01")
        update_month_data(cursor_my, mstart, day[0])

    # обовляем таблицу данные year to date для всех дней из заданного диапазона
    for day in range_dates:
        dat = datetime.datetime.strptime(day[0], "%Y-%m-%d")
        ystart = (str(dat.year) + "-01-01")
        update_year_data(cursor_my, ystart, day[0])
    conn_my.commit()

    conn_my.close()
    conn_ms.close()


if __name__ == "__main__":
    main()
