#!/usr/local/bin/python2
# -*- coding: utf-8 -*-


def transaction(cursor_my, ds, df, unit=None, region=None, district=None, brand=None):
    """
    Собираем количество тразакций по ресторанам и за выбранный период времени

    :param brand: BRAND
    :param district: указатель рсчета для Дистрикта
    :param region:  указатель расчета для Региона
    :param cursor_my: ссылка на подключение к базе данных MYSQL
    :param unit: массив номеров ресторанов
    :param ds: дата начала
    :param df: дата окончания
    :return: возвращает сумму транзакци по всем ресторанам РУ или по выбранному одному ресторану
    """
    if unit is not None:
        cursor_my.execute(
            'SELECT unit, sum(count) '
            '   FROM ru_aop.ru_tra '
            'WHERE unit = %s AND datas between %s and %s '
            'GROUP BY unit;', (unit, ds, df))
        return list(cursor_my.fetchall())
    elif region is 'All':
        cursor_my.execute(
            'SELECT r.reg_name, sum(tr.count) '
            '   FROM ru_aop.ru_tra tr'
            '   INNER JOIN ru_aop.units un'
            '     ON tr.unit=un.id_sap'
            '   INNER JOIN ru_aop.regions r'
            '     ON r.id_reg=un.unt_id_reg '
            'WHERE datas between %s and %s '
            'GROUP BY r.reg_name;', (ds, df))
        return list(cursor_my.fetchall())
    elif region is not None:
        cursor_my.execute(
            'SELECT r.reg_name, sum(tr.count) '
            '   FROM ru_aop.ru_tra tr'
            '   INNER JOIN ru_aop.units un'
            '     ON tr.unit=un.id_sap'
            '   INNER JOIN ru_aop.regions r'
            '     ON r.id_reg=un.unt_id_reg '
            'WHERE r.reg_name = %s AND datas between %s and %s '
            'GROUP BY r.reg_name;', (region, ds, df))
        return list(cursor_my.fetchall())
    elif district is 'All':
        cursor_my.execute(
            'SELECT d.dis_name, sum(tr.count) '
            '   FROM ru_aop.ru_tra tr'
            '   INNER JOIN ru_aop.units un'
            '     ON tr.unit=un.id_sap'
            '   INNER JOIN ru_aop.districts d'
            '     ON d.dis_id=un.id_d '
            'WHERE datas between %s and %s '
            'GROUP BY d.dis_name;', (ds, df))
        return list(cursor_my.fetchall())
    elif district is not None:
        cursor_my.execute(
            'SELECT d.dis_name, sum(tr.count) '
            '   FROM ru_aop.ru_tra tr'
            '   INNER JOIN ru_aop.units un'
            '     ON tr.unit=un.id_sap'
            '   INNER JOIN ru_aop.districts d'
            '     ON d.dis_id=un.id_d '
            'WHERE d.dis_name = %s AND datas between %s and %s '
            'GROUP BY d.dis_name;', (district, ds, df))
        return list(cursor_my.fetchall())
    elif brand is not None:
        cursor_my.execute(
            'SELECT b.brand_name, sum(tr.count) '
            '   FROM ru_aop.ru_tra tr'
            '   INNER JOIN ru_aop.units un'
            '     ON tr.unit=un.id_sap'
            '   INNER JOIN ru_aop.brands b '
            '     ON b.id_brand=un.unt_brand '
            'WHERE b.brand_name = %s AND datas between %s and %s '
            'GROUP BY b.brand_name;', (brand, ds, df))
        return list(cursor_my.fetchall())
    else:
        cursor_my.execute(
            'SELECT unit, sum(count) FROM ru_aop.ru_tra '
            '   WHERE unit BETWEEN 403001 AND 403999 AND datas between %s and %s '
            'GROUP BY unit;', (ds, df))
        return list(cursor_my.fetchall())


def units(cursor_my, unit=None):
    """
    Данные о ресторане, регионе, дистректе, типе ресторана и размере
    :param cursor_my: указатель на базу данных
    :param unit: номер ресторана
    :return: возвращает детали по всем ресторанам РУ или по конкретному ресторану
    """
    if unit is None:
        cursor_my.execute(
            'SELECT u.id_sap, u.unt_name, d.dis_name, d.dis_dc_name, r.reg_name, r.reg_ac_name, '
            '       u.unt_intranet_path, g.grp, b.brand_name, b.brand_ops_director'
            '  FROM units u '
            '  INNER JOIN regions r ON u.unt_id_reg = r.id_reg '
            '  INNER JOIN unt_group_tmp g ON u.id_sap=g.mpk '
            '  INNER JOIN districts d ON  d.dis_id=u.id_d '
            '  INNER JOIN brands b ON b.id_brand=u.unt_brand '
            'WHERE u.id_sap BETWEEN 403001 AND 403999'
            '      AND u.unt_active = 1')
        return list(cursor_my.fetchall())
    else:
        cursor_my.execute(
            'SELECT u.id_sap, u.unt_nam, d.dis_name, d.dis_dc_name, r.reg_name, r.reg_ac_name, '
            '       u.unt_intranet_path, g.grp, e b.brand_name, b.brand_ops_director'
            '  FROM units u '
            '  INNER JOIN regions r ON u.unt_id_reg = r.id_reg '
            '  INNER JOIN unt_group_tmp g ON u.id_sap=g.mpk '
            '  INNER JOIN districts d ON  d.dis_id=u.id_d '
            '  INNER JOIN brands b ON b.id_brand=u.unt_brand '
            'WHERE u.id_sap = %s'
            '      AND u.unt_active = 1', unit)
        return list(cursor_my.fetchall())


def sales(cursor_my, ds, df, unit=None, region=None, district=None, brand=None):
    """
    Сумма продаж по ресторанам в выбранный период
    :param brand:
    :param district: указатель рсчета для Дистрикта
    :param region:  указатель расчета для Региона
    :param cursor_my: указатель к базе данных
    :param ds: дата старт
    :param df: дата финишь
    :param unit: номер ресторана
    :return: возвращает сумму продаж или по всем ресторанам РУ или по конкретному ресторану
    """
    if unit is not None:
        cursor_my.execute(
            'SELECT unit, sum(net) '
            '   FROM ru_aop.ru_det '
            '   WHERE unit = %s AND datas between %s and %s '
            'GROUP BY unit;', (unit, ds, df))
        return list(cursor_my.fetchall())
    elif region is 'All':
        cursor_my.execute(
            'SELECT r.reg_name, sum(det.net) '
            '   FROM ru_aop.ru_det det'
            '   INNER JOIN ru_aop.units un'
            '     ON det.unit=un.id_sap'
            '   INNER JOIN ru_aop.regions r'
            '     ON r.id_reg=un.unt_id_reg '
            'WHERE datas between %s and %s '
            'GROUP BY r.reg_name;', (ds, df))
        return list(cursor_my.fetchall())
    elif region is not None:
        cursor_my.execute(
            'SELECT r.reg_name, sum(det.net) '
            '   FROM ru_aop.ru_det det'
            '   INNER JOIN ru_aop.units un'
            '     ON det.unit=un.id_sap'
            '   INNER JOIN ru_aop.regions r '
            '     ON r.id_reg=un.unt_id_reg '
            'WHERE r.reg_name = %s AND datas between %s and %s '
            'GROUP BY r.reg_name;', (region, ds, df))
        return list(cursor_my.fetchall())
    elif district is 'All':
        cursor_my.execute(
            'SELECT d.dis_name, sum(det.net) '
            '   FROM ru_aop.ru_det det'
            '   INNER JOIN ru_aop.units un '
            '     ON det.unit=un.id_sap '
            '   INNER JOIN ru_aop.districts d'
            '     ON d.dis_id=un.id_d '
            'WHERE datas between %s and %s '
            'GROUP BY d.dis_name;', (ds, df))
        return list(cursor_my.fetchall())
    elif district is not None:
        cursor_my.execute(
            'SELECT d.dis_name, sum(det.net) '
            '   FROM ru_aop.ru_det det'
            '   INNER JOIN ru_aop.units un '
            '     ON det.unit=un.id_sap'
            '   INNER JOIN ru_aop.districts d'
            '     ON d.dis_id=un.id_d '
            'WHERE d.dis_name = %s AND datas between %s and %s '
            'GROUP BY d.dis_name;', (district, ds, df))
        return list(cursor_my.fetchall())
    elif brand is not None:
        cursor_my.execute(
            'SELECT b.brand_name, sum(det.net) '
            '   FROM ru_aop.ru_det det'
            '   INNER JOIN ru_aop.units un'
            '     ON det.unit=un.id_sap'
            '   INNER JOIN ru_aop.brands b'
            '     ON b.id_brand=un.unt_brand '
            'WHERE b.brand_name = %s AND datas between %s and %s '
            'GROUP BY b.brand_name;', (brand, ds, df))
        return list(cursor_my.fetchall())
    else:
        cursor_my.execute(
            'SELECT unit, sum(net) '
            '   FROM ru_aop.ru_det '
            '   WHERE unit BETWEEN 403001 AND 403999 AND datas between %s and %s '
            'GROUP BY unit;', (ds, df))
        return list(cursor_my.fetchall())


def qty(cursor_my, ds, df, prod, unit=None, region=None, district=None, brand=None):
    """

    :param district: указатель рсчета для Дистрикта
    :param region:  указатель расчета для Региона
    :param cursor_my: указатель на базу данных
    :param ds:  дата старт
    :param df:  дата финишь
    :param prod:  шифр продукта из РКипера
    :param unit: номер ресторана
    :return: вовзращает список проданных указанных продуктов 
    """
    if unit is not None:
        cursor_my.execute(
            'SELECT sm.unit, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m'
            '     ON m.id=sm.id'
            'WHERE datas between %s and %s AND ptype!=9 AND sm.unit=%s AND m.stv_id in %s '
            'GROUP BY unit, datas;', (ds, df, unit, prod))
        return cursor_my.fetchall()
    elif region is 'All':
        cursor_my.execute(
            'SELECT r.reg_name, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m'
            '     ON m.id=sm.id'
            '   INNER JOIN ru_aop.units un'
            '     ON sm.unit=un.id_sap'
            '   INNER JOIN ru_aop.regions r '
            '     ON r.id_reg=un.unt_id_reg '
            'WHERE datas between %s and %s AND ptype!=9 AND m.stv_id in %s '
            'GROUP BY r.reg_name, datas;', (ds, df, prod))
        return cursor_my.fetchall()
    elif region is not None:
        cursor_my.execute(
            'SELECT r.reg_name, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m'
            '     ON m.id=sm.id'
            '   INNER JOIN ru_aop.units un'
            '     ON sm.unit=un.id_sap'
            '   INNER JOIN ru_aop.regions r '
            '     ON r.id_reg=un.unt_id_reg '
            'WHERE datas between %s and %s AND ptype!=9 AND r.reg_name=%s AND m.stv_id in %s '
            'GROUP BY r.reg_name, datas;', (ds, df, region, prod))
        return cursor_my.fetchall()
    elif district is 'All':
        cursor_my.execute(
            'SELECT d.dis_name, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m'
            '     ON m.id=sm.id'
            '   INNER JOIN ru_aop.units un'
            '     ON sm.unit=un.id_sap'
            '   INNER JOIN ru_aop.districts d'
            '     ON d.dis_id=un.id_d '
            'WHERE datas between %s and %s AND ptype!=9 AND m.stv_id in %s '
            'GROUP BY d.dis_name, datas;', (ds, df, prod))
        return cursor_my.fetchall()
    elif district is not None:
        cursor_my.execute(
            'SELECT d.dis_name, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m'
            '     ON m.id=sm.id'
            '   INNER JOIN ru_aop.units un'
            '     ON sm.unit=un.id_sap'
            '   INNER JOIN ru_aop.districts d'
            '     ON d.dis_id=un.id_d '
            'WHERE datas between %s and %s AND ptype!=9 AND d.dis_name=%s AND m.stv_id in %s '
            'GROUP BY d.dis_name, datas;', (ds, df, district, prod))
        return cursor_my.fetchall()
    elif brand is not None:
        cursor_my.execute(
            'SELECT b.brand_name, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m'
            '     ON m.id=sm.id'
            '   INNER JOIN ru_aop.units un'
            '     ON sm.unit=un.id_sap'
            '   INNER JOIN ru_aop.brands b '
            '     ON b.id_brand=un.unt_brand '
            'WHERE datas between %s and %s AND ptype!=9 AND b.brand_name = %s AND m.stv_id in %s '
            'GROUP BY b.brand_name;', (ds, df, brand, prod))
        return list(cursor_my.fetchall())
    else:
        cursor_my.execute(
            'SELECT sm.unit, sum(qty) '
            '   FROM ru_aop.ru_mix_4 sm'
            '   INNER JOIN ru_aop.ru_menu m '
            '     ON m.id=sm.id '
            'WHERE datas between %s and %s AND ptype!=9 AND m.stv_id in %s '
            'GROUP BY unit, datas;', (ds, df, prod))
        return cursor_my.fetchall()

