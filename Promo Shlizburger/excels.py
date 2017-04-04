#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import calculation as cl
import xlwt


def print_index(brands, sheet, string, indent, style_region, first_day, first_ptd, second_day, second_ptd, row_day_1,
                row_ptd_1, row_day_2, row_ptd_2):
    # печатаем заголовок строки
    sheet.write_merge(string, string, 0, indent, str(brands[0]) + "  " + brands[1], style_region)
    # печатаем первые показатели (индекс)
    sheet.write(string, row_day_1, first_day + " %", style_region)
    sheet.write(string, row_ptd_1, first_ptd + " %", style_region)
    # печатаем вторые показатели (прирост среднего чека)
    sheet.write(string, row_day_2, second_day + " %", style_region)
    sheet.write(string, row_ptd_2, second_ptd + " %", style_region)


def promo_index_avg():
    """

    :return:
    """
    style_region = xlwt.easyxf('font: name Times New Roman, bold on, height 280')
    style1_1 = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                           'align: wrap on, vert centre, horiz center')
    style_rest = xlwt.easyxf('font: name Times New Roman, bold off, height 240')
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
    indent = 6

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
    sheet1.write(string, row_ptd_1, "(" + str(cl.ds) + "-" + str(cl.df) + ")", style_dates)
    sheet1.write(string, row_day_1, "(" + str(cl.df) + ")", style_dates)
    sheet1.write(string, row_ptd_2, "(" + str(cl.ds) + "-" + str(cl.df) + ")", style_dates)
    sheet1.write(string, row_day_2, "(" + str(cl.df) + ")", style_dates)
    string += 2

    # ====================
    # Печать Бренда
    # для всех возможных брендов расчитаем показатели
    # ====================
    for r in cl.brands:
        print_index(r, sheet1, string, indent, style_region,
                    str(round(cl.brand_qty_day[r[0]] / cl.brand_tran_day[r[0]], 2) * 100),
                    str(round(cl.brand_qty_ptd[r[0]] / cl.brand_tran_ptd[r[0]], 2) * 100),
                    str(round((cl.brand_sales_day[r[0]] / cl.brand_tran_day[r[0]])
                              / cl.avg_brand_march[r[0]], 2) * 100),
                    str(round((cl.brand_sales_ptd[r[0]] / cl.brand_tran_ptd[r[0]])
                              / cl.avg_brand_march[r[0]], 2) * 100),
                    row_day_1, row_ptd_1, row_day_2, row_ptd_2)
        string += 1

    string += 1  # дополнительный отступ

    # ====================
    # Печать Дистрикт
    # ====================
    # print amsql.sales(cursor_my, ds, df, district='KFC RU 2')
    for r in cl.districts:
        print_index(r, sheet1, string, indent, style_region,
                    str(round(cl.districts_qty_day[r[0]] / cl.districts_tran_day[r[0]], 2) * 100),
                    str(round(cl.districts_qty_ptd[r[0]] / cl.districts_tran_ptd[r[0]], 2) * 100),
                    str(round((cl.districts_sales_day[r[0]] / cl.districts_tran_day[r[0]])
                              / cl.avg_districts_march[r[0]], 2) * 100),
                    str(round((cl.districts_sales_ptd[r[0]] / cl.districts_tran_ptd[r[0]])
                              / cl.avg_districts_march[r[0]], 2) * 100),
                    row_day_1, row_ptd_1, row_day_2, row_ptd_2)
        string += 1
    string += 1

    # ====================
    # Печать Регион
    # ====================
    # print amsql.qty(cursor_my, ds, df, products, region='KFC RU MOSCOW 2')
    for r in cl.regions:
        print r
        print_index(r, sheet1, string, indent, style_region,
                    str(round(cl.regions_qty_day[r[0]] / cl.regions_tran_day[r[0]], 2)*100),
                    str(round(cl.regions_qty_ptd[r[0]] / cl.regions_tran_ptd[r[0]], 2) * 100),
                    str(round((cl.regions_sales_day[r[0]] / cl.regions_tran_day[r[0]])
                              / cl.avg_regions_march[r[0]], 2) * 100),
                    str(round((cl.regions_sales_ptd[r[0]] / cl.regions_tran_ptd[r[0]])
                              / cl.avg_regions_march[r[0]], 2) * 100),
                    row_day_1, row_ptd_1, row_day_2, row_ptd_2)
        string += 1
    string += 1

    # ====================
    # Печать Ресторан
    # ====================
    # print amsql.units(cursor_my)
    group_previous = None
    for r in cl.sorted_units:
        print r
        group = r[2]
        if group_previous != group:
            string += 1
            sheet1.write_merge(string, string, 0, indent, r[2], style_region)
            string += 1
        print_index(r, sheet1, string, indent, style_rest,
                    str(r[3]), str(r[4]), str(r[5]), str(r[6]),
                    row_day_1, row_ptd_1, row_day_2, row_ptd_2)
        group_previous = r[2]
        string += 1

    # ====================
    # Печать Ресторан
    #  влкадка LARGE, MEDIUM, SMALL
    # ====================
    string_l = 0
    string_m = 0
    string_s = 0
    for r in cl.sorted_units:
        print r[2]
        if r[2] == 'LARGE':
            sheet_l = sheet4
            print_index(r, sheet_l, string_l, indent, style_rest,
                        str(r[3]), str(r[4]), str(r[5]), str(r[6]),
                        row_day_1, row_ptd_1, row_day_2, row_ptd_2)
            string_l += 1
        elif r[2] == 'MEDIUM':
            sheet_m = sheet3
            print_index(r, sheet_m, string_m, indent, style_rest,
                        str(r[3]), str(r[4]), str(r[5]), str(r[6]),
                        row_day_1, row_ptd_1, row_day_2, row_ptd_2)
            string_m += 1
        elif r[2] == 'SMALL':
            sheet_s = sheet2
            print_index(r, sheet_s, string_s, indent, style_rest,
                        str(r[3]), str(r[4]), str(r[5]), str(r[6]),
                        row_day_1, row_ptd_1, row_day_2, row_ptd_2)
            string_s += 1

    book.save("Promo Shefburger.xls")


if __name__ == "__main__":
    promo_index_avg()
