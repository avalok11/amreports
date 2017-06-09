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
    return response.json()


def main():
    # ====================
    # АТОРИЗАЦИЯ
    # ====================
    connection, cooks = connect()
    print (connection.status_code)
    print (connection.url)
    print (cooks)

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ПРИНТЕРОВ
    # ====================
    kkts = list_kkt(cooks)
    print "Список ККТ по организации"
    print kkts

    kkt_list = pd.DataFrame(kkts)
    kkt_list = kkt_list[['address','factoryId','model','regId','status']]
    kkt_list.to_csv('KKT.csv', sep=';', encoding='utf-8')
    #kkt_list = kkt_list.values.tolist()
    kkt_list = [((x[1],) + tuple(x)) for x in kkt_list.values.tolist()]
    print kkt_list
    a = [(u'00106904517510', u'127253, \u041c\u043e\u0441\u043a\u0432\u0430 \u0433, \u043a\u043c.\u041c\u041a\u0410\u0414 82-\u0439, \u0432\u043b\u0434. 18', u'00106904517510', u'\u0410\u0422\u041e\u041b 77\u0424', u'0000594589009678', u'2'),
         (u'00106903712317', u'162626, \u0412\u043e\u043b\u043e\u0433\u043e\u0434\u0441\u043a\u0430\u044f \u043e\u0431\u043b, \u0433.\u0427\u0435\u0440\u0435\u043f\u043e\u0432\u0435\u0446, \u0443\u043b.\u0413\u043e\u0434\u043e\u0432\u0438\u043a\u043e\u0432\u0430, \u0434.37', u'00106903712317', u'\u0410\u0422\u041e\u041b 77\u0424', u'0000520642017035', u'2')]

    connection.close()

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL И ЗАГРУЗКА ПОЛУЧЕННОЙ ИНФОРМАЦИИ ИЗ ОФД В ДБ
    # ===========================
    # make a connection to MSSQL iBase RU server
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