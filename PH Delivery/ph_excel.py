#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import xlwt
import datetime
import pymysql
import validation


def sql(cursor_my, yesterday):
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
    style_region = xlwt.easyxf('font: name Times New Roman, bold on, height 280')
    style1_1 = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                           'align: wrap on, vert centre, horiz center; borders: top thin, right thin, '
                           'left thin, bottom thin;')
    style_left = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                             'align: wrap on, vert centre, horiz center; borders: left thin;')
    style_right = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                              'align: wrap on, vert centre, horiz center; borders: right thin;')
    style_bottom = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                               'align: wrap on, vert centre, horiz center; borders: bottom thin;')
    style_bottom_right = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                                     'align: wrap on, vert centre, horiz center; borders: bottom thin, right thin;')
    style_data = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                             'align: wrap on, vert centre, horiz center;')
    style1_1_bold = xlwt.easyxf('font: name Times New Roman, bold on, height 200; '
                                'align: wrap on, vert centre, horiz center; borders: top thin, right thin, '
                                'left thin, bottom thin;')
    style_mpk = xlwt.easyxf("font: height 140, name Calibri, color 4; align: horiz right, vertical center;"
                            "borders: right thin, bottom thin, left thin;")
    # style_emo = xlwt.easyxf('font: name Wingdings, color-index blue, bold off, height 240')
    # style_emo_bad = xlwt.easyxf('font: name Wingdings, color-index green, bold off, height 240')
    # style_emo_good = xlwt.easyxf('font: name Wingdings, color-index red, bold off, height 240')

    # первоначальные настройки
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

    # вчерашнее число
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date().strftime("%Y-%m-%d")

    # ====================
    # получени еданных иза базы данных
    # ====================
    data = sql(cursor_my, yesterday)
    print data

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
    for i in range(4):
        for t in topic:
            sheet1.write(string, col, t, style1_1)
            col += 1

    # =======================
    # вкладка Delivery
    # печатаем тело отчета
    string += 1
    region_previous = None
    for rest in data:
        # ==================================
        #  печатаем регион если он изменился
        # ==================================
        region_current = rest[area]
        if None is region_previous:
            region_previous = region_current
        if region_previous != region_current:
            row = 1
            sheet1.write(string, row, rest_previous[area] + " " + rest_previous[area_n], style1_1)
            row += 1
            for i in range(region_start, region_start+total_row, 6):
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


    book.save("PH Delivery.xls")


if __name__ == "__main__":
    main()
