#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl
import ast


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

    cooks = dict(sid=ast.literal_eval(response.content)['sid'])
    return response, cooks


def list_kkt(cooks, inn='7825335145', status=2):
    """
    Список ККТ по организации
    :param cooks: получаем данные после авторизации на сайте connect()
    :param inn: ИНН организации
    :param status: Статус регистрации ККТ в ОФД:
                    0 – не зарегистрирована,
                    1 – идёт регистрация,
                    2 – активирована,
                    3 – снята с регистрации,
                    4 – ожидание активации.
    :return:
                    regId	    String, обязательный	Регистрационный номер ККТ, выданный ФНС
                    model	    String, обязательный	Название модели ККТ
                    factoryId	String, обязательный	Заводской номер ККТ
                    address	    String, обязательный	Адрес установки ККТ
                    status	    Number, обязательный	Статус регистрации ККТ в ОФД.
    """
    #response = requests.get('https://api.sbis.ru/ofd/v1/orgs/'+str(inn)+'/kkts?status='+str(status), cookies=cooks)
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts',
                            cookies=cooks)
    print response
    return response.json()


def main():
    # ====================
    # АТОРИЗАЦИЯ
    # ====================
    connection, cooks = connect()
    print (connection.status_code)
    print (connection.url)
    print (cooks)
    test = False

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ПРИНТЕРОВ
    # ====================
    kkts = list_kkt(cooks)
    print "Список ККТ по организации"
    print kkts

    kkt_list = pd.DataFrame(kkts)
    kkt_list = kkt_list[['address', 'factoryId', 'model', 'regId', 'status']]
    kkt_list.to_csv("C:\Users\\aleksey.yarkov\PycharmProjects\\amreports\OFD\OFD_DB\\FN.csv", sep=';', encoding='utf-8')
    kkt_list = [((x[1],) + tuple(x)) for x in kkt_list.values.tolist()]
    print "Всего обнаружено принтеров:", len(kkt_list)
    connection.close()

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL И ЗАГРУЗКА ПОЛУЧЕННОЙ ИНФОРМАЦИИ ИЗ ОФД В ДБ
    # ===========================
    # make a connection to MSSQL iBase RU server
    if test is False:
        conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                                  database=vl.db_ms, charset='utf8')
        cursor_ms = conn_ms.cursor()
    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
        cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_KKT;')
        cursor_ms.executemany("BEGIN "
                              "  IF NOT EXISTS "
                              "    (SELECT 1 FROM RU_T_FISCAL_KKT WHERE factoryId=%s)"
                              "  BEGIN "
                              "    INSERT INTO RU_T_FISCAL_KKT (address,factoryId,model,regId,status) "
                              "    VALUES (%s, %s, %s, %s, %s)"
                              "  END "
                              "END", kkt_list)

        conn_ms.commit()
        conn_ms.close()

if __name__ == "__main__":
    main()




        #requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000182040024937/storages/8710000100086130/'
                     #'docs?dateFrom=2017-05-01T00:00:00&limit=5', cookies=cooks)