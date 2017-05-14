#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import csv


def connect(idd='1025293145607151', login='yarkov.aleksei', pwd='331812qweASD'):
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
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/'+str(inn)+'/kkts?status='+str(status), cookies=cooks)
    return response.json()


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
    :param regid: Регистрационный номер ККТ, выданный ФНС, получаем из процедуры list_kkt()
    :return:
    """
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/'+str(inn)+'/kkts/'+str(regid)+'/storages?status='
                            +str(status),cookies=cooks)
    return response.json()


def main():
    connection, cooks = connect()
    print (connection.status_code)
    print (connection.url)
    print (cooks)

    kkts = list_kkt(cooks)
    print "Список ККТ по организации"
    # print kkts

    kkt_list = pd.DataFrame(kkts)
    kkt_list.set_index('regId', inplace=True)
    kkt_list.drop('status', axis=1, inplace=True)
    kkt_list.to_csv('df.csv', sep=';', encoding='utf-8')

    fn_list = pd.DataFrame()
    for regId in kkt_list.index.values:
        #print pd.DataFrame(list_fn(cooks, regId, inn='7825335145', status=2))
        dat = pd.DataFrame(list_fn(cooks, regId, inn='7825335145', status=2))
        dat['regId'] = regId
        fn_list = pd.concat([fn_list, dat])
        print fn_list
    fn_list.to_csv('fn_list.csv', sep=';', encoding='utf-8')

    kkt_list.reset_index(inplace=True)
    full_data = pd.merge(kkt_list, fn_list, how='inner', on='regId')
    full_data.to_csv('full_list.csv', sep=';', encoding='utf-8')

    connection.close()



    # print kkt_list[['regId', 'factoryId', 'address']]





    #i = 0
    #for a in kkts:
    #print "ID:", kkts['regId'], "Factor", kkts['factoryId'], "Model: ", kkts['model'], "Адрес: ", kkts['address']
    #    i += 1
    #print i

    #print "\nFN=++++++++++++"
    #df_kkts = pd.DataFrame(kkts)
    #df_kkts.set_index('address', inplace=True)
    #df_kkts.sort_index(inplace=True)
    #print df_kkts





    #print "Список фискальных документов по фискальному накопителю"
    #r = requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000292532034264/storages/8710000100082845/'
    #                 'docs?dateFrom=2017-05-01T12:12:12&limit=5', cookies=cooks)
    #print "Получение фискального документа по идентификатору"
    #r = requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000292532034264/storages/8710000100082845/'
    #                 'docs/77?format=json', cookies=cooks)


    # print('JSON')
    # r = requests.get('https://api.sbis.ru/v1/orgs/7825335145/kkts?status=2', params=js)
    # print (r.status_code)
    # print (r.url)


if __name__ == "__main__":
    main()
