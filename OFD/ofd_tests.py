#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime


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
                            + str(status), cookies=cooks)
    return response.json()


def list_checks(cooks, regid, storageid, datefrom, inn='7825335145'):
    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/'+str(inn)+'/kkts/'+str(regid)+'/storages/'
                            + str(storageid)+'/docs?dateFrom='+str(datefrom)+'&limit=1000', cookies=cooks)
    return response


def search_nds0(doc):
    check = pd.DataFrame()
    j = 0
    for d in doc:
        nds0 = None
        ndsCalculated10 = None
        ndsCalculated18 = None
        fiscalDriveNumber = None
        fiscalDocumentNumber = None
        shiftNumber = None
        dateTime = None
        print '\nНайденный чек:'
        if 'receipt' in d:
            print j
            j += 1
            print d
            if 'nds0' in d['receipt']:
                nds0 = d['receipt']['nds0']
            if 'ndsCalculated10' in d['receipt']:
                ndsCalculated10 = d['receipt']['ndsCalculated10']
            if 'ndsCalculated18' in d['receipt']:
                ndsCalculated18 = d['receipt']['ndsCalculated18']
            if 'fiscalDocumentNumber' in d['receipt']:
                fiscalDocumentNumber = d['receipt']['fiscalDocumentNumber']
            if 'dateTime' in d['receipt']:
                dateTime = datetime.datetime.fromtimestamp(int(d['receipt']['dateTime'])).strftime('%Y-%m-%d %H:%M:%S')
            if 'shiftNumber' in d['receipt']:
                shiftNumber = d['receipt']['shiftNumber']
            if 'fiscalDriveNumber' in d['receipt']:
                fiscalDriveNumber = d['receipt']['fiscalDriveNumber']
        if nds0 is not None:
            detail = pd.Series([fiscalDriveNumber, fiscalDocumentNumber, shiftNumber, dateTime, nds0,
                                ndsCalculated10, ndsCalculated18], index=['fiscalDriveNumber', 'fiscalDocumentNumber',
                                                                            'shiftNumber', 'dateTime', 'ndsNo',
                                                                            'ndsCalculated10', 'ndsCalculated18'])
            check = check.append(detail, ignore_index=True)
    return check


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
    # print kkts

    kkt_list = pd.DataFrame(kkts)
    kkt_list.set_index('regId', inplace=True)
    kkt_list.drop('status', axis=1, inplace=True)
    kkt_list.to_csv('df.csv', sep=';', encoding='utf-8')

    # ====================
    # ДЛЯ ТЕСТА ОГРАНИЧИВАЕМ КОЛ-ВО ПРИНТЕРОВ
    # ====================
    #kkt_list = kkt_list.iloc[0:10]
    #print kkt_list


    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФН ПО КАЖДОМУ ПРИНТЕРУ
    # ====================
    fn_list = pd.DataFrame()
    for regId in kkt_list.index.values:
        print "Фискальный накопитель принтера номер: ", regId, ". По адресу: ", kkt_list.loc[regId]['address']
        dat = pd.DataFrame(list_fn(cooks, regId, inn='7825335145', status=2))
        dat['regId'] = regId
        fn_list = pd.concat([fn_list, dat])
        print dat
    fn_list.to_csv('fn_list.csv', sep=';', encoding='utf-8')

    # ====================
    # ОБЬЕДИНЕНИЕ ФН И ПРИНТЕРОВ В ОДИН СПИСОК
    # ====================
    kkt_list.reset_index(inplace=True)
    full_data = pd.merge(kkt_list, fn_list, how='inner', on='regId')
    full_data.to_csv('full_list.csv', sep=';', encoding='utf-8')

    # ====================
    # Список всех фискальных документов по всем фискальном накопителям в организации
    # ====================
    all_docs = pd.DataFrame()
    print "список фискальных документов"

    for i in range(len(full_data)):
        print "Порядковый номер принтера: ", i
        regid = full_data.iloc[i]['regId']
        storageid = full_data.iloc[i]['storageId']
        print regid, storageid
        #regid = '0000182040024937'
        #storageid = '8710000100086130'
        r = list_checks(cooks, regid, storageid, '2017-05-13T00:00:00')
        r = r.json()
        doc = search_nds0(r)
        all_docs = pd.concat([all_docs, doc])

#    print all_docs
    all_docs.to_csv('list_ndsNo.csv', sep=';', encoding='utf-8')

    full_data_with_checks = pd.merge(full_data, all_docs, how='inner', left_on='storageId', right_on='fiscalDriveNumber')
    full_data_with_checks.to_csv('list_ndsNo_addr.csv', sep=';', encoding='utf-8')

    connection.close()


if __name__ == "__main__":
    main()




        #requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000182040024937/storages/8710000100086130/'
                     #'docs?dateFrom=2017-05-01T00:00:00&limit=5', cookies=cooks)