#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl


def connect(idd=vl.ofd_idd, login=vl.ofd_name, pwd=vl.ofd_pwd):
    """
    Аутентификация
    :param idd: application organisation id - from SBIS
    :param login: login name
    :param pwd: password
    :return: cookies which is used in next GET requests
            sid	    String, обязательный	Идентификатор сессии
            token	String, необязательный	Не используется
    """
    response = requests.post('https://api.sbis.ru/oauth/service/',
                             data=json.dumps({'app_client_id': idd,
                                              'login': login,
                                              'password': pwd}),
                             headers={'content-type': 'application/json; charset=utf-8'})
    return response, response.cookies


def list_fn(cooks, regid, inn='7825335145', status=2):
    """
    Список фискальных накопителей по ККТ
    :param cooks: получаем данные после авторизации на сайте connect()
    :param inn: ИНН организации
    :param status: Статус регистрации ККТ в ОФД:
                    0 – не зарегистрирована,
                    1 – идёт регистрация,
                    2 – активирована,
                    3 – снята с регистрации,
                    4 – ожидание активации.
    :param regid: Регистрационный номер ККТ, выданный ФНС, получаем из базы данных RU_T_FISCAL_KKT
    :return:
    storageId	String, обязательный	Номер фискального накопителя	«9999999»
    model	String, обязательный	Название модели ККТ	«ФН-Модель»
    status	Number, обязательный	Статус регистрации ФН в ОФД.	«2»
    effectiveFrom	String, обязательный	Время начала работы накопителя	«2016-10-19T12:20:45»
    effectiveTo	String	Время окончания работы накопителя, отсутствует для действующего накопителя	«2016-11-19T23:20:45»
    """
    #response = requests.get('https://api.sbis.ru/ofd/v1/orgs/'+str(inn)+'/kkts/'+str(regid)+'/storages?status='
    #                        + str(status), cookies=cooks)
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(regid) + '/storages',
                            cookies=cooks)
    return response.json()


def reg_kkt(cursor_ms):
    cursor_ms.execute("SELECT regId FROM RU_T_FISCAL_KKT;")
    return cursor_ms.fetchall()


def main():
    # ====================
    # АТОРИЗАЦИЯ
    # ====================
    connection, cooks = connect()
    print (connection.status_code)
    print (connection.url)
    print (cooks)

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL
    # ===========================
    # make a connection to MSSQL iBase RU server
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФИСКАЛЬНЫЙ НАКОПИТЕЛЕЙ
    # ====================
    fn_list = pd.DataFrame()
    regid = reg_kkt(cursor_ms)
    #regid= (('0000612404012879',), ('0000612219058428',), ('0000612124004976',))
    for k in regid:
        dat = pd.DataFrame(list_fn(cooks, k[0], inn='7825335145', status=2))
        if 'effectiveTo' not in dat.columns.values:
            dat['effectiveTo'] = None
        if 'effectiveFrom' not in dat.columns.values:
            dat['effectiveFrom'] = None
        dat['regId'] = k[0]
        fn_list = pd.concat([fn_list, dat])
        print dat
    fn_list.to_csv('FN.csv', sep=';', encoding='utf-8')
    connection.close()
    #print 'effectiveFrom'
    #print fn_list['effectiveFrom']
    #fn_list['effectiveFrom'] = pd.to_datetime(fn_list['effectiveFrom'])
    #print 'effectiveTo'
    #print fn_list['effectiveTo']
    #fn_list['effectiveTo'] = pd.to_datetime(fn_list['effectiveTo'])
    #print '123123123132'
    drive_list = [((x[3], x[5],) + tuple(x)) for x in fn_list.values.tolist()]
    print "\nDRIVE LIST"
    print drive_list

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_DRIVE;')

    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_DRIVE WHERE regId=%s and storageId=%s)"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_DRIVE "
                          "     (effectiveFrom, effectiveTo, model, regId, status, storageId) "
                          "    VALUES (%s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", drive_list)

    conn_ms.commit()
    conn_ms.close()

if __name__ == "__main__":
    main()




        #requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000182040024937/storages/8710000100086130/'
                     #'docs?dateFrom=2017-05-01T00:00:00&limit=5', cookies=cooks)