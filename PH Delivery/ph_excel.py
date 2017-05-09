#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import xlwt
import datetime
import pymysql
import validation


def sql_sl_tr(cursor_my, yesterday):
    cursor_my.execute('SELECT b.brand_name, b.brand_ops_director, u.id_sap, u.unt_name, r.reg_name, r.reg_ac_name, '
                      '     p.dinsl, 100*p.dinsl/p.dinsl_ly, p.winsl, 100*p.winsl/p.winsl_ly, p.minsl, '
                      '                 100*p.minsl/p.minsl_ly, p.yinsl, 100*p.yinsl/p.yinsl_ly, '
                      '     p.ddelcsl, 100*p.ddelcsl/p.sl, p.wdelcsl, 100*p.wdelcsl/p.wsl, p.mdelcsl, '
                      '                 100*p.mdelcsl/p.msl, p.ydelcsl, 100*p.ydelcsl/p.ysl, '
                      '     p.dcarsl, 100*p.dcarsl/p.sl, p.wcarsl, 100*p.wcarsl/p.wsl, p.mcarsl, '
                      '                 100*p.mcarsl/p.msl, p.ycarsl, 100*p.ycarsl/p.ysl,   '
                      '     p.dintr, 100*p.dintr/p.dintr_ly, p.wintr, 100*p.wintr/p.wintr_ly, p.mintr, '
                      '                 100*p.mintr/p.mintr_ly, p.yintr, 100*p.yintr/p.yintr_ly, '
                      '     p.ddelctr, 100*p.ddelctr/p.tr, p.wdelctr, 100*p.wdelctr/p.wtr, p.mdelctr, '
                      '                 100*p.mdelctr/p.mtr, p.ydelctr, 100*p.ydelctr/p.ytr,'
                      '     p.dcartr, 100*p.dcartr/p.tr, p.wcartr, 100*p.wcartr/p.wtr, p.mcartr, '
                      '                 100*p.mcartr/p.mtr, p.ycartr, 100*p.ycartr/p.ytr,'
                      '     rdinsl, rdinslind, rwinsl, rwinslind, rminsl, rminslind, ryinsl, ryinslind,'
                      '     rddelcsl, rddelcslind, rwdelcsl, rwdelcslind, rmdelcsl, rmdelcslind, rydelcsl, rydelcslind,'
                      '     rdcarsl, rdcarslind, rwcarsl, rwcarslind, rmcarsl, rmcarslind, rycarsl, rycarslind,'
                      ' '
                      '     rdintr, rdintrind, rwintr, rwintrind, rmintr, rmintrind, ryintr, ryintrind,'
                      '     rddelctr, rddelctrind, rwdelctr, rwdelctrind, rmdelctr, rmdelctrind, rydelctr, rydelctrind,'
                      '     rdcartr, rdcartrind, rwcartr, rwcartrind, rmcartr, rmcartrind, rycartr, rycartrind, '
                      ' '
                      '     bdinsl, bdinslind, bwinsl, bwinslind, bminsl, bminslind, byinsl, byinslind,'
                      '     bddelcsl, bddelcslind, bwdelcsl, bwdelcslind, bmdelcsl, bmdelcslind, bydelcsl, bydelcslind,'
                      '     bdcarsl, bdcarslind, bwcarsl, bwcarslind, bmcarsl, bmcarslind, bycarsl, bycarslind,'
                      ' '
                      '     bdintr, bdintrind, bwintr, bwintrind, bmintr, bmintrind, byintr, byintrind,'
                      '     bddelctr, bddelctrind, bwdelctr, bwdelctrind, bmdelctr, bmdelctrind, bydelctr, bydelctrind,'
                      '     bdcartr, bdcartrind, bwcartr, bwcartrind, bmcartr, bmcartrind, bycartr, bycartrind'
                      ' '
                      ' FROM units u'
                      ' INNER JOIN regions r ON u.unt_id_reg=r.id_reg'
                      ' INNER JOIN brands b ON u.unt_brand=b.id_brand'
                      ' LEFT JOIN `ph_sl_tr` p ON u.id_sap=p.mpk'
                      ' LEFT JOIN (SELECT unt_id_reg,'
                      '     sum(dinsl) rdinsl, 100*sum(dinsl)/sum(dinsl_ly) rdinslind, sum(winsl) rwinsl, '
                      '         100*sum(winsl)/sum(winsl_ly) rwinslind, sum(minsl) rminsl, '
                      '         100*sum(minsl)/sum(minsl_ly) rminslind, sum(yinsl) ryinsl, '
                      '         100*sum(yinsl)/sum(yinsl_ly) ryinslind,'
                      '     sum(ddelcsl) rddelcsl, 100*sum(ddelcsl)/sum(sl) rddelcslind, sum(wdelcsl) '
                      '         rwdelcsl, 100*sum(wdelcsl)/sum(wsl) rwdelcslind, sum(mdelcsl) rmdelcsl, '
                      '         100*sum(mdelcsl)/sum(msl) rmdelcslind, sum(ydelcsl) rydelcsl, '
                      '         100*sum(ydelcsl)/sum(ysl) rydelcslind,'
                      '     sum(dcarsl) rdcarsl, 100*sum(dcarsl)/sum(sl) rdcarslind, sum(wcarsl) rwcarsl, '
                      '         100*sum(wcarsl)/sum(wsl) rwcarslind, sum(mcarsl) rmcarsl, '
                      '         100*sum(mcarsl)/sum(msl) rmcarslind, sum(ycarsl) rycarsl, '
                      '         100*sum(ycarsl)/sum(ysl) rycarslind,'
                      ' '
                      '     sum(dintr) rdintr, 100*sum(dintr)/sum(dintr_ly) rdintrind, sum(wintr) rwintr, '
                      '         100*sum(wintr)/sum(wintr_ly) rwintrind, sum(mintr) rmintr, '
                      '         100*sum(mintr)/sum(mintr_ly) rmintrind, sum(yintr) ryintr, '
                      '         100*sum(yintr)/sum(yintr_ly) ryintrind,'
                      '     sum(ddelctr) rddelctr, 100*sum(ddelctr)/sum(tr) rddelctrind, sum(wdelctr) rwdelctr, '
                      '         100*sum(wdelctr)/sum(wtr) rwdelctrind, sum(mdelctr) rmdelctr, '
                      '         100*sum(mdelctr)/sum(mtr) rmdelctrind, sum(ydelctr) rydelctr, '
                      '         100*sum(ydelctr)/sum(ytr) rydelctrind,'
                      '     sum(dcartr) rdcartr, 100*sum(dcartr)/sum(tr) rdcartrind, sum(wcartr) rwcartr, '
                      '         100*sum(wcartr)/sum(wtr) rwcartrind, sum(mcartr) rmcartr, '
                      '         100*sum(mcartr)/sum(mtr) rmcartrind, sum(ycartr) rycartr, '
                      '         100*sum(ycartr)/sum(ytr) rycartrind'
                      ' FROM `ph_sl_tr`'
                      ' INNER JOIN units ON ph_sl_tr.mpk=units.id_sap'
                      ' WHERE day=%s'
                      ' GROUP BY unt_id_reg) reg ON reg.unt_id_reg=u.unt_id_reg'
                      ' LEFT JOIN (SELECT unt_id_reg,'
                      '     sum(dinsl) bdinsl, 100*sum(dinsl)/sum(dinsl_ly) bdinslind, sum(winsl) bwinsl, '
                      '         100*sum(winsl)/sum(winsl_ly) bwinslind, sum(minsl) bminsl, '
                      '         100*sum(minsl)/sum(minsl_ly) bminslind, sum(yinsl) byinsl, '
                      '         100*sum(yinsl)/sum(yinsl_ly) byinslind,'
                      '     sum(ddelcsl) bddelcsl, 100*sum(ddelcsl)/sum(sl) bddelcslind, sum(wdelcsl) bwdelcsl, '
                      '         100*sum(wdelcsl)/sum(wsl) bwdelcslind, sum(mdelcsl) bmdelcsl, '
                      '         100*sum(mdelcsl)/sum(msl) bmdelcslind, sum(ydelcsl) bydelcsl, '
                      '         100*sum(ydelcsl)/sum(ysl) bydelcslind,'
                      '     sum(dcarsl) bdcarsl, 100*sum(dcarsl)/sum(sl) bdcarslind, sum(wcarsl) bwcarsl, '
                      '         100*sum(wcarsl)/sum(wsl) bwcarslind, sum(mcarsl) bmcarsl, '
                      '         100*sum(mcarsl)/sum(msl) bmcarslind, sum(ycarsl) bycarsl, '
                      '         100*sum(ycarsl)/sum(ysl) bycarslind,'
                      ' '
                      '     sum(dintr) bdintr, 100*sum(dintr)/sum(dintr_ly) bdintrind, sum(wintr) bwintr, '
                      '         100*sum(wintr)/sum(wintr_ly) bwintrind, sum(mintr) bmintr, '
                      '         100*sum(mintr)/sum(mintr_ly) bmintrind, sum(yintr) byintr, '
                      '         100*sum(yintr)/sum(yintr_ly) byintrind,'
                      '     sum(ddelctr) bddelctr, 100*sum(ddelctr)/sum(tr) bddelctrind, sum(wdelctr) bwdelctr, '
                      '         100*sum(wdelctr)/sum(wtr) bwdelctrind, sum(mdelctr) bmdelctr, '
                      '         100*sum(mdelctr)/sum(mtr) bmdelctrind, sum(ydelctr) bydelctr, '
                      '         100*sum(ydelctr)/sum(ytr) bydelctrind,'
                      '     sum(dcartr) bdcartr, 100*sum(dcartr)/sum(tr) bdcartrind, sum(wcartr) bwcartr, '
                      '         100*sum(wcartr)/sum(wtr) bwcartrind, sum(mcartr) bmcartr, '
                      '         100*sum(mcartr)/sum(mtr) bmcartrind, sum(ycartr) bycartr, '
                      '         100*sum(ycartr)/sum(ytr) bycartrind'
                      ' FROM `ph_sl_tr`'
                      ' INNER JOIN units ON ph_sl_tr.mpk=units.id_sap'
                      ' WHERE day=%s'
                      ' GROUP BY unt_brand) brand ON reg.unt_id_reg=u.unt_id_reg'
                      ' '
                      ' WHERE u.unt_brand=14 and unt_active=1 AND p.day=%s'
                      ' ORDER BY r.reg_name, u.unt_name ASC;', (yesterday, yesterday, yesterday))
    return list(cursor_my.fetchall())


def sql_delivery(cursor_my, yesterday):
    cursor_my.execute('SELECT b.brand_name, b.brand_ops_director, u.id_sap, u.unt_name, u.unt_id_reg, r.reg_name, '
                      'r.reg_ac_name,'
                      '     p.d0, p.d30, p.d3040, p.d40, p.dtotal, p.dctotal,'
                      '     p.w0, p.w30, p.w3040, p.w40, p.wtotal, p.wctotal,'
                      '     p.m0, p.m30, p.m3040, p.m40, p.mtotal, p.mctotal,'
                      '     p.y0, p.y30, p.y3040, p.y40, p.ytotal, p.yctotal,'
                      '     rd0, rd30, rd3040, rd40, rdtotal, rdctotal,'
                      '     rw0, rw30, rw3040, rw40, rwtotal, rwctotal,'
                      '     rm0, rm30, rm3040, rm40, rmtotal, rmctotal,'
                      '     ry0, ry30, ry3040, ry40, rytotal, ryctotal,'
                      '     bd0, bd30, bd3040, bd40, bdtotal, bdctotal,'
                      '     bw0, bw30, bw3040, bw40, bwtotal, bwctotal,'
                      '     bm0, bm30, bm3040, bm40, bmtotal, bmctotal,'
                      '     by0, by30, by3040, by40, bytotal, byctotal'
                      ' FROM units u'
                      ' INNER JOIN regions r ON u.unt_id_reg=r.id_reg'
                      ' INNER JOIN brands b ON u.unt_brand=b.id_brand'
                      ' LEFT JOIN `ph_delivery` p ON u.id_sap=p.mpk'
                      ' LEFT JOIN (SELECT unt_id_reg,'
                      '              sum(d0) as rd0, sum(d30) as rd30, sum(d3040) as rd3040, sum(d40) as rd40, '
                      '              sum(dtotal) as rdtotal, sum(dctotal) as rdctotal, sum(w0) as rw0, '
                      '              sum(w30) as rw30, sum(w3040) as rw3040, sum(w40) as rw40, sum(wtotal) as rwtotal, '
                      '              sum(wctotal) as rwctotal, sum(m0) as rm0, sum(m30) as rm30, sum(m3040) as rm3040, '
                      '              sum(m40) as rm40, sum(mtotal) as rmtotal, sum(mctotal) as rmctotal, '
                      '              sum(y0) as ry0, sum(y30) as ry30, sum(y3040) as ry3040, sum(y40) as ry40, '
                      '              sum(ytotal) as rytotal, sum(yctotal) as ryctotal'
                      '            FROM `ph_delivery`'
                      '            INNER JOIN units ON ph_delivery.mpk=units.id_sap'
                      '            WHERE date=%s'
                      '            GROUP BY unt_id_reg) reg '
                      ' ON reg.unt_id_reg=u.unt_id_reg'
                      ' LEFT JOIN (SELECT unt_brand,'
                      '               sum(d0) as bd0, sum(d30) as bd30, sum(d3040) as bd3040, '
                      '               sum(d40) as bd40, sum(dtotal) as bdtotal, sum(dctotal) as bdctotal,'
                      '               sum(w0) as bw0, sum(w30) as bw30, sum(w3040) as bw3040, '
                      '               sum(w40) as bw40, sum(wtotal) as bwtotal, sum(wctotal) as bwctotal,'
                      '               sum(m0) as bm0, sum(m30) as bm30, sum(m3040) as bm3040, '
                      '               sum(m40) as bm40, sum(mtotal) as bmtotal, sum(mctotal) as bmctotal,'
                      '               sum(y0) as by0, sum(y30) as by30, sum(y3040) as by3040, '
                      '               sum(y40) as by40, sum(ytotal) as bytotal, sum(yctotal) as byctotal'
                      '             FROM `ph_delivery`'
                      '             INNER JOIN units ON ph_delivery.mpk=units.id_sap'
                      '             WHERE date=%s'
                      '             GROUP BY unt_brand) brand ON brand.unt_brand=u.unt_brand'
                      '             WHERE u.unt_brand=14 and unt_active=1 AND p.date=%s'
                      ' ORDER BY r.reg_name, u.unt_name ASC;', (yesterday, yesterday, yesterday))
    return list(cursor_my.fetchall())


def main():
    """

    :return:
    """
    # ====================
    # подключение к базе
    # ====================

    conn_my = pymysql.connect(host=validation.ip_mysql, user=validation.usr_my, password=validation.pwd_my,
                              db=validation.db_my, charset='utf8')
    cursor_my = conn_my.cursor()

    style_head = xlwt.easyxf('font: name Times New Roman, color-index red, bold on, height 200')
    style1_1 = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                           'align: wrap on, vert centre, horiz center; borders: top thin, right thin, '
                           'left thin, bottom thin;')
    style_left = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                             'align: wrap on, vert centre, horiz center; borders: left thin;')
    style_right = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                              'align: wrap on, vert centre, horiz center; borders: right thin;')
    style_bottom = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                               'align: wrap on, vert centre, horiz center; borders: bottom thin;')
    style_bottom_ind = xlwt.easyxf('font: name Times New Roman, color-index red, bold off, height 200; '
                               'align: wrap on, vert centre, horiz center; borders: bottom thin;')
    style_bottom_right = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                                     'align: wrap on, vert centre, horiz center; borders: bottom thin, right thin;')
    style_bottom_right_ind = xlwt.easyxf('font: name Times New Roman, color-index red, bold off, height 200; '
                                     'align: wrap on, vert centre, horiz center; borders: bottom thin, right thin;')
    style_data = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                             'align: wrap on, vert centre, horiz center;')
    style_data_ind = xlwt.easyxf('font: name Times New Roman, color-index red, bold off, height 160; '
                             'align: wrap on, vert centre, horiz center;')
    style_data_ind_right = xlwt.easyxf('font: name Times New Roman, color-index red, bold off, height 160; '
                                 'align: wrap on, vert centre, horiz center; borders: right thin;')
    style1_1_bold = xlwt.easyxf('font: name Times New Roman, bold on, height 200; '
                                'align: wrap on, vert centre, horiz center; borders: top thin, right thin, '
                                'left thin, bottom thin;')
    style_mpk = xlwt.easyxf("font: height 140, name Calibri, color 4; align: horiz right, vertical center;"
                            "borders: right thin, bottom thin, left thin;")

    # ====================
    # настройки для DELIVERY
    # ====================
    row_wday = 2
    row_wtd = 8
    row_mtd = 14
    row_ytd = 20

    topic = ['= 0 min #', '< 30 min %', '30<40 min %', '> 40 min %', '# total delivery', '# carry out самовывоз']
    brand = 0
    brand_n = 1
    mpk = 2
    unit = 3
    area = 5
    area_n = 6
    total_row = 24
    region_start = 24
    brand_start = 48
    m0 = 7
    m30 = 8
    m3040 = 9
    m40 = 10
    totaldelivery = 11
    totalcaryout = 12

    # ====================
    # настройки для SALES TRANSACTION
    # ====================
    row_sldinein = 2
    row_sldelco = 10
    row_slcar = 18
    row_trdinein = 26
    row_trdelco = 32
    row_trcar = 38

    head1 = ['SALES DineIn', 'SALES Delco', 'SALES Cary Out', 'TRANS DineIn', 'TRANS Delco', 'TRANS Cary Out']
    head2 = ['Day', 'WTD', 'MTD', 'YTD']
    head3 = ['Value', 'Ind.']

    sl_brand = 0
    sl_brand_n = 1
    sl_mpk = 2
    sl_unit = 3
    sl_area = 4
    sl_area_n = 5
    sl_total_row = 48
    sl_rest_start = 6
    sl_region_start = 54
    sl_brand_start = 102

    # вчерашнее число
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date().strftime("%Y-%m-%d")

    # ====================
    # получение данных DELIVERY
    # ====================
    delivery_data = sql_delivery(cursor_my, yesterday)
    print delivery_data
    sales_data = sql_sl_tr(cursor_my, yesterday)
    print sales_data

    # ====================
    # создаем эксель
    # ====================
    book = xlwt.Workbook(encoding='utf-8')
    sheet1 = book.add_sheet('Delivery')
    sheet2 = book.add_sheet('Sales Transactions')

    col_width = 256 * 25
    sheet1.col(1).width = col_width
    sheet2.col(1).width = col_width
    col_mpk_width = 256 * 8
    sheet1.col(0).width = col_mpk_width
    sheet2.col(0).width = col_mpk_width
    # ширина столбцов
    col_width = 256 * 12
    for i in range(25):
        sheet1.col(i + 2).width = col_width
        sheet2.col(i + 2).width = col_width

    # =======================
    # =======================
    # =======================
    # =======================
    # вкладка Delivery
    # печатает заголовок файла
    string = 0
    col = 2
    sheet1.write(string, 0, yesterday, style_head)
    sheet2.write(string, 0, yesterday, style_head)
    string += 1
    sheet1.write_merge(string, string, row_wday, (row_wday + 23), "Speed of Delivery", style1_1)
    string += 1
    sheet1.write_merge(string, string, row_wday, (row_wday + 5), "WDAY", style1_1)
    sheet1.write_merge(string, string, row_wtd, (row_wtd + 5), "WTD", style1_1)
    sheet1.write_merge(string, string, row_mtd, (row_mtd + 5), "MTD", style1_1)
    sheet1.write_merge(string, string, row_ytd, (row_ytd + 5), "YTD", style1_1)

    string += 1
    sheet1.write(string, col - 2, 'MPK', style1_1_bold)
    sheet1.write(string, col - 1, 'UNIT', style1_1_bold)
    sheet2.write(string, col - 2, 'MPK', style1_1_bold)
    sheet2.write(string, col - 1, 'UNIT', style1_1_bold)
    for i in range(4):
        for t in topic:
            sheet1.write(string, col, t, style1_1)
            col += 1

    # =======================
    # вкладка Delivery
    # печатаем тело отчета
    string += 1
    region_previous = None
    for rest in delivery_data:
        # ==================================
        #  печатаем регион если он изменился
        # ==================================
        region_current = rest[area]
        if None is region_previous:
            region_previous = region_current
        if region_previous != region_current:
            row = 1
            print rest_previous[area]
            sheet1.write(string, row, rest_previous[area] + " " + rest_previous[area_n], style1_1)
            row += 1
            for i in range(region_start, region_start + total_row, 6):
                try:
                    k30 = str(round((100 * rest_previous[i + m30] / rest_previous[i + totaldelivery]), 2)) + "%"
                except TypeError:
                    k30 = '0%'
                try:
                    k3040 = str(round(100 * rest_previous[i + m3040] / rest_previous[i + totaldelivery], 2)) + "%"
                except TypeError:
                    k3040 = '0%'
                try:
                    k40 = str(round(100 * rest_previous[i + m40] / rest_previous[i + totaldelivery], 2)) + "%"
                except TypeError:
                    k40 = '0%'
                sheet1.write(string, row + i + 0 - region_start, rest_previous[i + m0], style_bottom)
                sheet1.write(string, row + i + 1 - region_start, k30, style_bottom)
                sheet1.write(string, row + i + 2 - region_start, k3040, style_bottom)
                sheet1.write(string, row + i + 3 - region_start, k40, style_bottom)
                sheet1.write(string, row + i + 4 - region_start, rest_previous[i + totaldelivery], style_bottom)
                sheet1.write(string, row + i + 5 - region_start, rest_previous[i + totalcaryout], style_bottom_right)
            string += 2

        # ==================================
        #  печатаем рестораны
        # ==================================
        row = 0
        sheet1.write(string, row, rest[mpk], style_mpk)
        row += 1
        sheet1.write(string, row, rest[unit], style1_1)
        row += 1
        for i in range(0, total_row, 6):
            try:
                k30 = str(100 * rest[i + m30] / rest[i + totaldelivery]) + "%"
            except TypeError:
                k30 = '0%'
            try:
                k3040 = str(100 * rest[i + m3040] / rest[i + totaldelivery]) + "%"
            except TypeError:
                k3040 = '0%'
            try:
                k40 = str(100 * rest[i + m40] / rest[i + totaldelivery]) + "%"
            except TypeError:
                k40 = '0%'
            sheet1.write(string, row + i + 0, rest[i + m0], style_left)
            sheet1.write(string, row + i + 1, k30, style_data)
            sheet1.write(string, row + i + 2, k3040, style_data)
            sheet1.write(string, row + i + 3, k40, style_data)
            sheet1.write(string, row + i + 4, rest[i + totaldelivery], style_data)
            sheet1.write(string, row + i + 5, rest[i + totalcaryout], style_right)
        string += 1

        region_previous = rest[area]
        rest_previous = rest

    # =======================
    # печатаем последний регион и бранд
    # =======================
    row = 1
    sheet1.write(string, row, rest_previous[area] + " " + rest_previous[area_n], style1_1)
    row += 1
    for i in range(region_start, region_start + total_row, 6):
        try:
            k30 = str(round((100 * rest_previous[i + m30] / rest_previous[i + totaldelivery]), 2)) + "%"
        except TypeError:
            k30 = '0%'
        try:
            k3040 = str(round(100 * rest_previous[i + m3040] / rest_previous[i + totaldelivery], 2)) + "%"
        except TypeError:
            k3040 = '0%'
        try:
            k40 = str(round(100 * rest_previous[i + m40] / rest_previous[i + totaldelivery], 2)) + "%"
        except TypeError:
            k40 = '0%'
        sheet1.write(string, row + i + 0 - region_start, rest_previous[i + m0], style_bottom)
        sheet1.write(string, row + i + 1 - region_start, k30, style_bottom)
        sheet1.write(string, row + i + 2 - region_start, k3040, style_bottom)
        sheet1.write(string, row + i + 3 - region_start, k40, style_bottom)
        sheet1.write(string, row + i + 4 - region_start, rest_previous[i + totaldelivery], style_bottom)
        sheet1.write(string, row + i + 5 - region_start, rest_previous[i + totalcaryout], style_bottom_right)
    string += 2

    row = 1
    sheet1.write(string, row, rest_previous[brand] + " " + rest_previous[brand_n], style1_1)
    row += 1
    for i in range(brand_start, brand_start + total_row, 6):
        try:
            k30 = str(round((100 * rest_previous[i + m30] / rest_previous[i + totaldelivery]), 2)) + "%"
        except TypeError:
            k30 = '0%'
        try:
            k3040 = str(round(100 * rest_previous[i + m3040] / rest_previous[i + totaldelivery], 2)) + "%"
        except TypeError:
            k3040 = '0%'
        try:
            k40 = str(round(100 * rest_previous[i + m40] / rest_previous[i + totaldelivery], 2)) + "%"
        except TypeError:
            k40 = '0%'
        sheet1.write(string, row + i + 0 - brand_start, rest_previous[i + m0], style_bottom)
        sheet1.write(string, row + i + 1 - brand_start, k30, style_bottom)
        sheet1.write(string, row + i + 2 - brand_start, k3040, style_bottom)
        sheet1.write(string, row + i + 3 - brand_start, k40, style_bottom)
        sheet1.write(string, row + i + 4 - brand_start, rest_previous[i + totaldelivery], style_bottom)
        sheet1.write(string, row + i + 5 - brand_start, rest_previous[i + totalcaryout], style_bottom_right)

    # =======================
    # =======================
    # =======================
    # =======================
    # вкладка Sales Transaction
    # печатает заголовок файла
    row_h1 = row_sldinein
    stringh1 = 1
    stringh2 = 2
    stringh3 = 3
    for h1 in head1:
        # print "H1"
        # print stringh1,  row_h1, (row_h1 + len(head2)*len(head3) - 1)
        sheet2.write_merge(stringh1, stringh1, row_h1, (row_h1 + len(head2)*len(head3) - 1), h1, style1_1_bold)
        row_h2 = row_h1
        for h2 in head2:
            sheet2.write_merge(stringh2, stringh2, row_h2, (row_h2 + len(head3) - 1), h2, style1_1_bold)
            row_h3 = row_h2
            for h3 in head3:
                sheet2.write(stringh3, row_h3, h3, style1_1_bold)
                row_h3 += len(head3) - 1
            row_h2 += len(head3)
        row_h1 += len(head2)*len(head3)

    # =======================
    # вкладка Sales
    # печатаем тело отчета
    string = 4
    region_previous = None
    for rest in sales_data:
        # ==================================
        #  печатаем регион если он изменился
        # ==================================
        region_current = rest[sl_area]
        if None is region_previous:
            region_previous = region_current
        if region_previous != region_current:
            row = 1
            sheet2.write(string, row, rest_previous[sl_area] + " " + rest_previous[sl_area_n], style1_1)
            row += 1
            for i in range(0, sl_total_row, 8):
                print i + sl_region_start + 0
                sheet2.write(string, row + i + 0, rest_previous[i + sl_region_start + 0], style_data)
                sheet2.write(string, row + i + 1, rest_previous[i + sl_region_start + 1], style_data_ind)
                sheet2.write(string, row + i + 2, rest_previous[i + sl_region_start + 2], style_data)
                sheet2.write(string, row + i + 3, rest_previous[i + sl_region_start + 3], style_data_ind)
                sheet2.write(string, row + i + 4, rest_previous[i + sl_region_start + 4], style_data)
                sheet2.write(string, row + i + 5, rest_previous[i + sl_region_start + 5], style_data_ind)
                sheet2.write(string, row + i + 6, rest_previous[i + sl_region_start + 6], style_data)
                sheet2.write(string, row + i + 7, rest_previous[i + sl_region_start + 7], style_data_ind_right)
            string += 2

        # ==================================
        #  печатаем рестораны
        # ==================================
        row = 0
        sheet2.write(string, row, rest[sl_mpk], style_mpk)
        row += 1
        sheet2.write(string, row, rest[sl_unit], style1_1)
        row += 1
        for i in range(0, sl_total_row, 8):
            sheet2.write(string, row + i + 0, rest[i + sl_rest_start], style_data)
            sheet2.write(string, row + i + 1, rest[i + sl_rest_start + 1], style_data_ind)
            sheet2.write(string, row + i + 2, rest[i + sl_rest_start + 2], style_data)
            sheet2.write(string, row + i + 3, rest[i + sl_rest_start + 3], style_data_ind)
            sheet2.write(string, row + i + 4, rest[i + sl_rest_start + 4], style_data)
            sheet2.write(string, row + i + 5, rest[i + sl_rest_start + 5], style_data_ind)
            sheet2.write(string, row + i + 6, rest[i + sl_rest_start + 6], style_data)
            sheet2.write(string, row + i + 7, rest[i + sl_rest_start + 7], style_data_ind_right)

        string += 1
        region_previous = rest[sl_area]
        rest_previous = rest

    # ==========
    # печатаем последний регион и бренд
    # ==========
    row = 1
    sheet2.write(string, row, rest_previous[sl_area] + " " + rest_previous[sl_area_n], style1_1)
    row += 1
    for i in range(0, sl_total_row, 8):
        sheet2.write(string, row + i + 0, rest_previous[i + sl_region_start + 0], style_bottom)
        sheet2.write(string, row + i + 1, rest_previous[i + sl_region_start + 1], style_bottom_ind)
        sheet2.write(string, row + i + 2, rest_previous[i + sl_region_start + 2], style_bottom)
        sheet2.write(string, row + i + 3, rest_previous[i + sl_region_start + 3], style_bottom_ind)
        sheet2.write(string, row + i + 4, rest_previous[i + sl_region_start + 4], style_bottom)
        sheet2.write(string, row + i + 5, rest_previous[i + sl_region_start + 5], style_bottom_ind)
        sheet2.write(string, row + i + 6, rest_previous[i + sl_region_start + 6], style_bottom)
        sheet2.write(string, row + i + 7, rest_previous[i + sl_region_start + 7], style_bottom_right_ind)
    string += 2

    row = 1
    sheet2.write(string, row, rest_previous[sl_brand] + " " + rest_previous[sl_brand_n], style1_1)
    row += 1
    for i in range(0, sl_total_row, 8):
        sheet2.write(string, row + i + 0, rest_previous[i + sl_brand_start + 0], style_bottom)
        sheet2.write(string, row + i + 1, rest_previous[i + sl_brand_start + 1], style_bottom_ind)
        sheet2.write(string, row + i + 2, rest_previous[i + sl_brand_start + 2], style_bottom)
        sheet2.write(string, row + i + 3, rest_previous[i + sl_brand_start + 3], style_bottom_ind)
        sheet2.write(string, row + i + 4, rest_previous[i + sl_brand_start + 4], style_bottom)
        sheet2.write(string, row + i + 5, rest_previous[i + sl_brand_start + 5], style_bottom_ind)
        sheet2.write(string, row + i + 6, rest_previous[i + sl_brand_start + 6], style_bottom)
        sheet2.write(string, row + i + 7, rest_previous[i + sl_brand_start + 7], style_bottom_right_ind)

    book.save("PH Delivery.xls")


if __name__ == "__main__":
    main()
