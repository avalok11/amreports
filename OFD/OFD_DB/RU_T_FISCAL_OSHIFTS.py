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


def list_checks(cooks, regid, storageid, datefrom, inn='7825335145'):
    """

    :param cooks: получаем после успешного подклчения к ОФД
    :param regid:
        regId	String, обязательный	Регистрационный номер ККТ, выданный ФНС	«123»
    :param storageid:
        storageId	String, обязательный	Номер фискального накопителя	«9999999»
    :param datefrom:
        dateFrom	String, обязательный	Время начала периода запрашиваемых документов	«2016-10-19T12:20:45»
        dateTo	String	Время окончания периода запрашиваемых документов. Если не указано, берётся текущее время	«2016-11-19T23:20:45»
    :param inn:
        inn	String, обязательный	ИНН организации-владельца ККТ	«1234567890»
    :return:
        список всех фискальных документов
        нас интересуют
        openShift	Отчет об открытии смены	Object	0...1

            Фискальный документ «Отчет об открытии смены»
            Данные возвращаются в объекте с именем openShift в формате, описанном ниже:
            Тэг	Имя реквизита в формате JSON	Описание реквизита	Тип данных JSON	Кардинальность
            2	code	код документа (всегда должен быть равен 2)	Number	1
            1048	user	наименование пользователя	String (Unicode)	1
            1018	userInn	ИНН пользователя	String (Unicode)	1
            1021	operator	кассир	String (Unicode)	1
            1009	retailPlaceAddress 	адрес (место) расчетов	String (Unicode)	1
            1012	dateTime	дата, время	Number	1
            1038	shiftNumber	номер смены	Number	1
            1037	kktRegId 	регистрационный номер ККТ	String (Unicode)	1
            1041	fiscalDriveNumber	заводской номер фискального накопителя	String (Unicode)	1
            1040	fiscalDocumentNumber	порядковый номер фискального документа	Number	1
            1077	fiscalSign	фискальный признак документа	Number	1
            1069	message	сообщение оператору	Array[объект]	0..n
            1084	properties	дополнительный реквизит	Array[объект]	0..n
    """

    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/'+str(inn)+'/kkts/'+str(regid)+'/storages/'
                            + str(storageid)+'/docs?dateFrom='+str(datefrom)+'&limit=10', cookies=cooks)
    return response.json()


#def reg_kkt(cursor_ms):
#    cursor_ms.execute("SELECT regId FROM RU_T_FISCAL_KKT;")
#    return cursor_ms.fetchall()


def reg_drive(cursor_ms):
    cursor_ms.execute("SELECT regId, storageId FROM RU_T_FISCAL_DRIVE;")
    return cursor_ms.fetchall()


def main():
    # ====================
    # АТОРИЗАЦИЯ
    # ====================
    connection, cooks = connect()
    print "Connection is established"
    print " _________________________"
    print (connection.status_code)
    print (connection.url)
    print (cooks)
    print " _________________________"

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL
    # ===========================
    # make a connection to MSSQL iBase RU server
    #conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
    #                          database=vl.db_ms, charset='utf8')
    #cursor_ms = conn_ms.cursor()

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФИСКАЛЬНЫЙ НАКОПИТЕЛЕЙ
    # ====================
    fiscal_documents = pd.DataFrame()
    open_shift = pd.DataFrame()
    close_shift = pd.DataFrame()
    receipt = pd.DataFrame()
    # kktid = reg_kkt(cursor_ms)  # registration id of FP
    # driveid = reg_drive(cursor_ms)

    id = (('0000612404012879', '8710000100612901'), ('0000612219058428', '8710000100609863'),
          ('0000612124004976', '8710000100604073'))

    for k in id:
        data = pd.DataFrame((list_checks(cooks, k[0], k[1], '2017-06-06T00:00:00')))
        open_shift_tmp = data['openShift'].dropna()
        close_shift_tmp = data['closeShift'].dropna()
        receipt_tmp = data['receipt'].dropna()
        for i in open_shift_tmp:
            open_shift = pd.concat([open_shift, pd.DataFrame(i, index=[0])])
        for i in close_shift_tmp:
            close_shift = pd.concat([close_shift, pd.DataFrame(i, index=[0])])
        for i in receipt_tmp:
            print "\n\n\n", i
            print pd.DataFrame(i)
            receipt = pd.concat([receipt, pd.DataFrame(i)])





    #fiscal_documents = fiscal_documents[['effectiveFrom', 'effectiveTo', 'model', 'regId', 'status', 'storageId']]
    open_shift.to_csv('openshift.csv', sep=';', encoding='utf-8')
    close_shift.to_csv('closeshift.csv', sep=';', encoding='utf-8')
    receipt.to_csv('receipt.csv', sep=';', encoding='utf-8')
    connection.close()
    #print 'effectiveFrom'
    #print fn_list['effectiveFrom']
    #fn_list['effectiveFrom'] = pd.to_datetime(fn_list['effectiveFrom'])
    #print 'effectiveTo'
    #print fn_list['effectiveTo']
    #fn_list['effectiveTo'] = pd.to_datetime(fn_list['effectiveTo'])
    #print '123123123132'
    #drive_list = [((x[3], x[5],) + tuple(x)) for x in fn_list.values.tolist()]
    #print "\nDRIVE LIST"
    #print drive_list

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================
    # УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
    #cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_DRIVE;')

    #cursor_ms.executemany("BEGIN "
    #                      "  IF NOT EXISTS "
    #                      "    (SELECT 1 FROM RU_T_FISCAL_DRIVE WHERE regId=%s and storageId=%s)"
    #                      "  BEGIN "
    #                      "    INSERT INTO RU_T_FISCAL_DRIVE "
    #                      "     (effectiveFrom, effectiveTo, model, regId, status, storageId) "
    #                      "    VALUES (%s, %s, %s, %s, %s, %s)"
    #                      "  END "
    #                      "END", drive_list)

    #conn_ms.commit()
    #conn_ms.close()

if __name__ == "__main__":
    main()




        #requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000182040024937/storages/8710000100086130/'
                     #'docs?dateFrom=2017-05-01T00:00:00&limit=5', cookies=cooks)