#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import pymysql
import validation
import amsql
import xlwt
import datetime


def main():
    """

    :return:
    """

    # make a connection to MySQL ru_bi RU server
    conn_my = pymysql.connect(host=validation.ip_mysql, user=validation.usr_my, password=validation.pwd_my,
                              db=validation.db_my, charset='utf8')
    cursor_my = conn_my.cursor()

    units = 403001
    ds = 20170327
    df = (datetime.datetime.now() - datetime.timedelta(days=1)).date().strftime("%Y%m%d")
    df = 20170327
    
    ds_march = 20170301
    df_march = 20170331
    products = ("1021385", "1021397")
    # 1023097 1023108

    # подготовка справочников
    units = amsql.units(cursor_my)
    regions = set()
    for r in units:
        regions.add((r[4], r[5]))
    districts = set()
    for d in units:
        districts.add((d[2], d[3]))
    brands = set()
    for b in units:
        brands.add((b[0], b[1]))

    # СЧИТАЕМ DAY
    # TRANSACTION расчет транзакций - Ресторан, Регион, Дистрикт, Бренд
    units_tran_day = dict(amsql.transaction(cursor_my, df, df))
    regions_tran_day = dict(amsql.transaction(cursor_my, df, df, region='All'))
    districts_tran_day = dict(amsql.transaction(cursor_my, df, df, district='All'))
    brand_tran_day = dict(amsql.transaction(cursor_my, df, df, brand='KFC RU'))
    # предварительный расчет для среднего чека за март
    units_tran_day_m = dict(amsql.transaction(cursor_my, ds_march, df_march))
    regions_tran_day_m = dict(amsql.transaction(cursor_my, ds_march, df_march, region='All'))
    districts_tran_day_m = dict(amsql.transaction(cursor_my, ds_march, df_march, district='All'))
    brand_tran_day_m = dict(amsql.transaction(cursor_my, ds_march, df_march, brand='KFC RU'))

    # SALES расчет продаж - Ресторан, Регион, Дистрикт, Бренд
    units_sales_day = dict(amsql.sales(cursor_my, df, df))
    regions_sales_day = dict(amsql.sales(cursor_my, df, df, region='All'))
    districts_sales_day = dict(amsql.sales(cursor_my, df, df, district='All'))
    brand_sales_day = dict(amsql.sales(cursor_my, df, df, brand='KFC RU'))
    # предварительный расчет для среднего чека за март
    units_sales_day_m = dict(amsql.sales(cursor_my, ds_march, df_march))
    regions_sales_day_m = dict(amsql.sales(cursor_my, ds_march, df_march, region='All'))
    districts_sales_day_m = dict(amsql.sales(cursor_my, ds_march, df_march, district='All'))
    brand_sales_day_m = dict(amsql.sales(cursor_my, ds_march, df_march, brand='KFC RU'))

    # QTY расчет количества продуктов - Ресторан, Регион, Дистрикт, Бренд
    units_qty_day = dict(amsql.qty(cursor_my, df, df, products))
    regions_qty_day = dict(amsql.qty(cursor_my, df, df, products, region='All'))
    districts_qty_day = dict(amsql.qty(cursor_my, df, df, products, district='All'))
    brand_qty_day = dict(amsql.qty(cursor_my, df, df, products, brand='KFC RU'))
    
    # СЧИТАЕМ PTD
    # TRANSACTION расчет транзакций - Ресторан, Регион, Дистрикт, Бренд
    units_tran_ptd = dict(amsql.transaction(cursor_my, ds ,df))
    regions_tran_ptd = dict(amsql.transaction(cursor_my, ds ,df, region='All'))
    districts_tran_ptd = dict(amsql.transaction(cursor_my, ds, df, district='All'))
    brand_tran_ptd = dict(amsql.transaction(cursor_my, ds, df, brand='KFC RU'))

    # SALES расчет продаж - Ресторан, Регион, Дистрикт, Бренд
    units_sales_ptd = dict(amsql.sales(cursor_my, ds, df))
    regions_sales_ptd = dict(amsql.sales(cursor_my, ds, df, region='All'))
    districts_sales_ptd = dict(amsql.sales(cursor_my, ds, df, district='All'))
    brand_sales_ptd = dict(amsql.sales(cursor_my, ds, df, brand='KFC RU'))

    # QTY расчет количества продуктов - Ресторан, Регион, Дистрикт, Бренд
    units_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products))
    regions_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products, region='All'))
    districts_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products, district='All'))
    brand_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products, brand='KFC RU'))
    
    # считаем AVG за МАРТ
    avg_units_march = dict()
    for u in units_tran_day_m:
        avg_units_march[u] = units_sales_day_m[u]/units_tran_day_m[u]
    avg_regions_march = dict()
    for r in regions_tran_day_m:
        avg_regions_march[r] = regions_sales_day_m[r]/regions_tran_day_m[r]
    avg_districts_march = dict()
    for d in districts_tran_day_m:
        avg_districts_march[d] = districts_sales_day_m[d]/districts_tran_day_m[d]
    avg_brand_march = dict()
    for b in brand_tran_day_m:
        avg_brand_march[b] = brand_sales_day_m[b]/brand_tran_day_m[b]
        print avg_brand_march
        print brand_sales_day_m[b], "SALES"
        print brand_tran_day_m[b], "TRANSACTION"

    # ====================
    # создаем эксель. форматирование, заголовки  и прочее
    # ====================
    style_region = xlwt.easyxf('font: name Times New Roman, bold on, height 280')
    style1_1 = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                           'align: wrap on, vert centre, horiz center')
    style3 = xlwt.easyxf('font: name Times New Roman, bold off, height 240')
    style_dates = xlwt.easyxf('font: name Times New Roman, bold off, height 160')
    style_emo = xlwt.easyxf('font: name Wingdings, color-index blue, bold off, height 240')
    style_emo_bad = xlwt.easyxf('font: name Wingdings, color-index green, bold off, height 240')
    style_emo_good = xlwt.easyxf('font: name Wingdings, color-index red, bold off, height 240')
    book = xlwt.Workbook(encoding="utf-8")
    sheet1 = book.add_sheet("All")
    sheet2 = book.add_sheet("Small")
    sheet3 = book.add_sheet("Medium")
    sheet4 = book.add_sheet("Large")

    row_day_1 = 8
    row_ptd_1 = 10
    row_day_2 = 14
    row_ptd_2 = 16
    indent = 5

    string = 0
    sheet1.write(string, 0, "Промо ШЕФБУРГЕР", style_region)
    string += 2
    sheet1.write_merge(2, 6, 8, 11, "Суммарный рейтинг двух позиций: Шефбургер и Шефбургер Острый на 100 транзакций."
                                    " (Цель – 15 штук суммарно на 100 транзакций)", style1_1)
    sheet1.write_merge(2, 6, 14, 16, "Рост среднего чека – Апрель к Марту +2%.", style1_1)

    string += 6
    sheet1.write(string, row_ptd_1, "PTD", style_region)
    sheet1.write(string, row_day_1, "DAY (yest.)", style_region)
    sheet1.write(string, row_ptd_2, "PTD", style_region)
    sheet1.write(string, row_day_2, "DAY (yest.)", style_region)
    string += 1
    sheet1.write(string, row_ptd_1, "(" + str(ds) + "-" + str(df) + ")", style_dates)
    sheet1.write(string, row_day_1, "(" + str(df) + ")", style_dates)
    sheet1.write(string, row_ptd_2, "(" + str(ds) + "-" + str(df) + ")", style_dates)
    sheet1.write(string, row_day_2, "(" + str(df) + ")", style_dates)
    string += 2

    # ====================
    # Печать Бренда
    # ====================
    for i in brands:
        sheet1.write_merge(string, string, 0, indent, i[0]+" ,"+i[1], style_region)
        sheet1.write(string, row_day_1, str(round(brand_tran_day[i[0]] / brand_sales_day[i[0]], 2))
                     + " %", style_region)
        sheet1.write(string, row_ptd_1, str(round(brand_tran_ptd[i[0]] / brand_sales_ptd[i[0]], 2))
                     + " %", style_region)
        sheet1.write(string, row_day_2, str(round((brand_sales_day[i[0]] / brand_tran_day[i[0]])
                                                  / avg_brand_march[i[0]], 2)) + " %", style_region)
        print (brand_sales_day[i[0]] / brand_tran_day[i[0]])
        print avg_brand_march[i[0]]
        sheet1.write(string, row_ptd_2, str(round((brand_sales_ptd[i[0]] / brand_tran_ptd[i[0]])
                                                  / avg_brand_march[i[0]], 2)) + " %", style_region)
        print (brand_sales_ptd[i[0]] / brand_tran_ptd[i[0]])
        print avg_brand_march[i[0]]

    #print amsql.transaction(cursor_my, ds, df, region='All')

    # ====================
    # Печать Дистрикт
    # ====================
    #print amsql.sales(cursor_my, ds, df, district='KFC RU 2')

    # ====================
    # Печать Регион
    # ====================
    #print amsql.qty(cursor_my, ds, df, products, region='KFC RU MOSCOW 2')

    # ====================
    # Печать Ресторан
    # ====================
    #print amsql.units(cursor_my)

    book.save("Promo Shefburger.xls")


if __name__ == "__main__":
    main()
