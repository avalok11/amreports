#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import pymysql
import validation
import amsql
import datetime


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
units = sorted(units, key=lambda x: x[7])
regions = set()
for r in units:
    regions.add((r[4], r[5]))
regions = list(sorted(regions))
districts = set()
for d in units:
    districts.add((d[2], d[3]))
districts = list(sorted(districts))
brands = set()
for b in units:
    brands.add((b[8], b[9]))
brands = list(sorted(brands))

# СЧИТАЕМ DAY
# TRANSACTION расчет транзакций - Ресторан, Регион, Дистрикт, Бренд
print ("1. TRANSACTION, day: " + str(datetime.datetime.now()))
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
print ("2. Sales, day: " + str(datetime.datetime.now()))
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
print ("3. QTY, day: " + str(datetime.datetime.now()))
units_qty_day = dict(amsql.qty(cursor_my, df, df, products))
regions_qty_day = dict(amsql.qty(cursor_my, df, df, products, region='All'))
districts_qty_day = dict(amsql.qty(cursor_my, df, df, products, district='All'))
brand_qty_day = dict(amsql.qty(cursor_my, df, df, products, brand='KFC RU'))

# СЧИТАЕМ PTD
# TRANSACTION расчет транзакций - Ресторан, Регион, Дистрикт, Бренд
print ("4. TRANSACTION, ptd: " + str(datetime.datetime.now()))
units_tran_ptd = dict(amsql.transaction(cursor_my, ds, df))
regions_tran_ptd = dict(amsql.transaction(cursor_my, ds, df, region='All'))
districts_tran_ptd = dict(amsql.transaction(cursor_my, ds, df, district='All'))
brand_tran_ptd = dict(amsql.transaction(cursor_my, ds, df, brand='KFC RU'))

# SALES расчет продаж - Ресторан, Регион, Дистрикт, Бренд
print ("5. SALES, ptd: " + str(datetime.datetime.now()))
units_sales_ptd = dict(amsql.sales(cursor_my, ds, df))
regions_sales_ptd = dict(amsql.sales(cursor_my, ds, df, region='All'))
districts_sales_ptd = dict(amsql.sales(cursor_my, ds, df, district='All'))
brand_sales_ptd = dict(amsql.sales(cursor_my, ds, df, brand='KFC RU'))

# QTY расчет количества продуктов - Ресторан, Регион, Дистрикт, Бренд
print ("6. QTY, ptd: " + str(datetime.datetime.now()))
units_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products))
regions_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products, region='All'))
districts_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products, district='All'))
brand_qty_ptd = dict(amsql.qty(cursor_my, ds, df, products, brand='KFC RU'))

# считаем AVG за МАРТ
print ("7. считаем AVG за МАРТ: " + str(datetime.datetime.now()))
avg_units_march = dict()
for u in units_tran_day_m:
    avg_units_march[u] = units_sales_day_m[u] / units_tran_day_m[u]
avg_regions_march = dict()
for r in regions_tran_day_m:
    avg_regions_march[r] = regions_sales_day_m[r] / regions_tran_day_m[r]
avg_districts_march = dict()
for d in districts_tran_day_m:
    avg_districts_march[d] = districts_sales_day_m[d] / districts_tran_day_m[d]
avg_brand_march = dict()
for b in brand_tran_day_m:
    avg_brand_march[b] = brand_sales_day_m[b] / brand_tran_day_m[b]
    print avg_brand_march
    print brand_sales_day_m[b], "SALES"
    print brand_tran_day_m[b], "TRANSACTION"

# готовим массив из показателей ресторана mpk, rest_name, group, index_day, index_ptd, avg_d, avg_ptd
# отсортированый по группам и по показателям
sorted_units = list()
for i in units:
    try:
        sorted_units.append((i[0], i[1], i[7],
                             round(units_qty_day[i[0]] / units_tran_day[i[0]], 2)*100,
                             round(units_qty_ptd[i[0]] / units_tran_ptd[i[0]], 2) * 100,
                             round((units_sales_day[i[0]] / units_tran_day[i[0]])
                                       / avg_units_march[i[0]], 2) * 100,
                             round((units_sales_ptd[i[0]] / units_tran_ptd[i[0]])
                                       / avg_units_march[i[0]], 2) * 100))
    except KeyError:
        sorted_units.append((i[0], i[1], i[7],
                             0, 0, 0, 0))
sorted_units = sorted(sorted(sorted_units, key=lambda x: x[4], reverse=True), key=lambda y: y[2])