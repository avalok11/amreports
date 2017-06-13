#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl
import numpy


def read_name():
    receipt['items'] = receipt['items'].apply(lambda x: str(x))


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
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФИСКАЛЬНЫЙ НАКОПИТЕЛЕЙ
    # ====================
    # kktid = reg_kkt(cursor_ms)  # registration id of FP
    # driveid = reg_drive(cursor_ms)

    id = (('0000612404012879', '8710000100612901'), ('0000612219058428', '8710000100609863'),
          ('0000612124004976', '8710000100604073'))
    #id = (('0000574146027868', '8710000100779501'),
    #      ('0000574048062921', '8710000100779443'),
    #      ('0000573980017345', '8710000100779164'))
    #id = (('0000509048051340', '8710000100373867'), )

    # ======================
    # предварительное определение полей значений
    # ======================
    open_shift = pd.DataFrame(columns=['code', 'dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'fiscalSign',
                                        'kktRegId', 'operator', 'rawData', 'shiftNumber', 'userInn'])
    close_shift = pd.DataFrame(columns=['code', 'dateTime', 'documentsQuantity', 'fiscalDocumentNumber',
                                        'fiscalDriveExhaustionSign', 'fiscalDriveMemoryExceededSign',
                                        'fiscalDriveNumber', 'fiscalDriveReplaceRequiredSign', 'fiscalSign', 'kktRegId',
                                        'notTransmittedDocumentsDateTime', 'notTransmittedDocumentsQuantity',
                                        'ofdResponseTimeoutSign', 'operator', 'rawData', 'receiptsQuantity',
                                        'shiftNumber', 'userInn'])
    receipt = pd.DataFrame(columns=['cashTotalSum', 'dateTime', 'ecashTotalSum', 'fiscalDocumentNumber',
                                    'fiscalDriveNumber', 'fiscalSign', 'items', 'kktRegId', 'nds0', 'nds10', 'nds18',
                                    'operationType', 'operator', 'rawData', 'receiptCode', 'requestNumber',
                                    'shiftNumber', 'totalSum', 'user', 'userInn'])
    open_shift = pd.DataFrame()
    close_shift = pd.DataFrame()
    receipt = pd.DataFrame()

    today = datetime.datetime.today()
    day_check = today - datetime.timedelta(days=3)
    datas = day_check.date().isoformat()+"T00:00:00"
    day = day_check.date().isoformat()

    #datas = '2017-06-07T00:00:00'

    for k in id:
        data = pd.DataFrame((list_checks(cooks, k[0], k[1], datas)))
        try:
            open_shift_tmp = data['openShift'].dropna()
            for i in open_shift_tmp:
                df = pd.DataFrame(i, index=[0])
                df['dateTime'] = datetime.datetime.fromtimestamp(df['dateTime'])
                open_shift = pd.concat([open_shift, df])
        except KeyError:
            None
        try:
            close_shift_tmp = data['closeShift'].dropna()
            for i in close_shift_tmp:
                df = pd.DataFrame(i, index=[0])
                df['dateTime'] = df['dateTime'].apply(lambda x: datetime.datetime.fromtimestamp(x))
                close_shift = pd.concat([close_shift, df])
        except KeyError:
            None
        try:
            receipt_tmp = data['receipt'].dropna()
            for i in receipt_tmp:
                df = pd.DataFrame(i)
                df['dateTime'] = df['dateTime'].apply(lambda x: datetime.datetime.fromtimestamp(x))
                if 'ndsNo' in df.columns.values:
                    df['nds0'] = df['ndsNo']
                    df.drop('ndsNo', axis=1, inplace=True)
                if 'nds0' not in df.columns.values:
                    df['nds0'] = 0
                if 'nds10' not in df.columns.values:
                    df['nds10'] = 0
                if 'nds18' not in df.columns.values:
                    df['nds18'] = 0
                receipt = pd.concat([receipt, df])
        except KeyError:
            None
    connection.close()

    # ===========================
    # ПОДГОТОВКА ДАННЫХ ОБ ОТКРЫТИИ СМЕН
    # ===========================
    #for i in range(len(open_shift['dateTime'])):
    #    open_shift['dateTime'].iloc[i] = datetime.datetime.fromtimestamp(open_shift['dateTime'].iloc[i])
        #open_shift['fiscalSign'].iloc[i] = int(open_shift['fiscalSign'].iloc[i])
    open_shift = open_shift[['code', 'dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'fiscalSign',
                                       'kktRegId', 'operator', 'rawData', 'shiftNumber', 'userInn']]
    open_shift.drop('rawData', axis=1, inplace=True)
    open_shift.to_csv('openshift_'+str(day)+'.csv', sep=';', encoding='utf-8')
    open_shift = [((x[1], x[3], x[5], x[7]) + tuple(x)) for x in open_shift.values.tolist()]
    print "OPEN SHIFTS"
    print open_shift

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ЗАКРЫТИИ СМЕН
    # ===========================
    #for i in range(len(close_shift['dateTime'])):
    #    close_shift['dateTime'].iloc[i] = datetime.datetime.fromtimestamp(close_shift['dateTime'].iloc[i])
    #    close_shift['notTransmittedDocumentsDateTime'].iloc[i] = \
    #        datetime.datetime.fromtimestamp(close_shift['notTransmittedDocumentsDateTime'].iloc[i])
        #close_shift['code'].iloc[i] = int(close_shift['code'].iloc[i])
        #close_shift['documentsQuantity'].iloc[i] = int(close_shift['documentsQuantity'].iloc[i])
        #close_shift['fiscalDocumentNumber'].iloc[i] = int(close_shift['fiscalDocumentNumber'].iloc[i])
        #close_shift['fiscalDriveExhaustionSign'].iloc[i] = int(close_shift['fiscalDriveExhaustionSign'].iloc[i])
        #close_shift['fiscalDriveMemoryExceededSign'].iloc[i] = int(close_shift['fiscalDriveMemoryExceededSign'].iloc[i])
        #close_shift['fiscalDriveReplaceRequiredSign'].iloc[i] = \
        #    int(close_shift['fiscalDriveReplaceRequiredSign'].iloc[i])
        #close_shift['fiscalSign'].iloc[i] = int(close_shift['fiscalSign'].iloc[i])
        #close_shift['notTransmittedDocumentsQuantity'].iloc[i] = \
        #    int(close_shift['notTransmittedDocumentsQuantity'].iloc[i])
        #close_shift['ofdResponseTimeoutSign'].iloc[i] = int(close_shift['ofdResponseTimeoutSign'].iloc[i])
        #close_shift['receiptsQuantity'].iloc[i] = int(close_shift['receiptsQuantity'].iloc[i])
        #close_shift['shiftNumber'].iloc[i] = int(close_shift['shiftNumber'].iloc[i])
    close_shift = close_shift[['code', 'dateTime', 'documentsQuantity', 'fiscalDocumentNumber',
                                        'fiscalDriveExhaustionSign', 'fiscalDriveMemoryExceededSign',
                                        'fiscalDriveNumber', 'fiscalDriveReplaceRequiredSign', 'fiscalSign', 'kktRegId',
                                        'notTransmittedDocumentsDateTime', 'notTransmittedDocumentsQuantity',
                                        'ofdResponseTimeoutSign', 'operator', 'rawData', 'receiptsQuantity',
                                        'shiftNumber', 'userInn']]
    close_shift.drop('rawData', axis=1, inplace=True)
    close_shift.to_csv('closeshift_'+str(day)+'.csv', sep=';', encoding='utf-8')
    close_shift = [((x[1], x[6], x[9], x[15]) + tuple(x)) for x in close_shift.values.tolist()]
    print "CLOSED SHIFTS"
    print close_shift

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ЧЕКАХ
    # ===========================
    #for i in range(len(receipt['dateTime'])):
    #    receipt['dateTime'].iloc[i] = datetime.datetime.fromtimestamp(receipt['dateTime'].iloc[i])
        #receipt['fiscalDocumentNumber'].iloc[i] = int(receipt['fiscalDocumentNumber'].iloc[i])
        #receipt['operationType'].iloc[i] = int(receipt['operationType'].iloc[i])
        #receipt['receiptCode'].iloc[i] = int(receipt['receiptCode'].iloc[i])
        #receipt['requestNumber'].iloc[i] = int(receipt['requestNumber'].iloc[i])
        #receipt['shiftNumber'].iloc[i] = int(receipt['shiftNumber'].iloc[i])
        #receipt['userInn'].iloc[i] = int(receipt['userInn'].iloc[i])
        #receipt['fiscalSign'].iloc[i] = int(receipt['fiscalSign'].iloc[i])
    receipt['numid'] = receipt.index
    #try:
    #    receipt.drop('rawData', axis=1, inplace=True)
    #    receipt.drop('items', axis=1, inplace=True)
    #except ValueError:
    #    None
    try:
        receipt = receipt[['cashTotalSum', 'dateTime', 'ecashTotalSum', 'fiscalDocumentNumber',
                           'fiscalDriveNumber', 'fiscalSign', 'items', 'kktRegId', 'nds0', 'nds10', 'nds18',  #'fiscalDriveNumber', 'fiscalSign', 'items', 'kktRegId', 'nds0', 'nds10', 'nds18',
                           'operationType', 'operator', 'receiptCode', 'requestNumber',
                           'shiftNumber', 'totalSum', 'user', 'userInn', 'numid']]
        receipt['nds0'] /= 100
        receipt['nds10'] /= 100
        receipt['nds18'] /= 100
        receipt['cashTotalSum'] /= 100
        receipt['ecashTotalSum'] /= 100
        print receipt['items']
        #print (receipt['items'].apply(lambda x: x['name']))
        receipt['items'] = receipt['items'].apply(read_name(x))
        print receipt['items']
    except KeyError:
        None
    receipt.to_csv('receipt_' + str(day) + '.csv', sep=';', encoding='utf-8')
    receipt = [((x[1], x[4], x[7], x[15], x[19]) + tuple(x)) for x in receipt.values.tolist()]  #receipt = [((x[1], x[4], x[7], x[15]) + tuple(x)) for x in receipt.values.tolist()]
    print "RECEIPT"
    print receipt

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================

    # ДОБАВЛЯЕМ ОТКРЫТЫЕ СМЕНЫ

    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_OSHIFT WHERE dateTime=%s and fiscalDriveNumber=%s "
                          "                                            and kktRegId=%s and shiftNumber=%s)"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_OSHIFT "
                          "         (code, dateTime, fiscalDocumentNumber, fiscalDriveNumber, fiscalSign, kktRegId, "
                          "          operator, shiftNumber, usrInn) "
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", open_shift)

    # ЗАКРЫТЫЕ СМЕНЫ
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_CSHIFT WHERE dateTime=%s and fiscalDriveNumber=%s"
                          "                                            and kktRegId=%s and shiftNumber=%s)"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_CSHIFT "
                          "            (code, dateTime, documentsQuantity, fiscalDocumentNumber,"
                          "             fiscalDriveExhaustionSign, fiscalDriveMemoryExceededSign,"
                          "             fiscalDriveNumber, fiscalDriveReplaceRequiredSign, fiscalSign, kktRegId,"
                          "             notTransmittedDocumentsDateTime, notTransmittedDocumentsQuantity,"
                          "             ofdResponseTimeoutSign, operator, receiptsQuantity,"
                          "             shiftNumber, userInn)"
                          "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                          "            %s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", close_shift)

    # ЧЕКИ
    cursor_ms.executemany("BEGIN "
                          "  IF NOT EXISTS "
                          "    (SELECT 1 FROM RU_T_FISCAL_RECEIPT WHERE dateTime=%s and fiscalDriveNumber=%s"
                          "                                            and kktRegId=%s and shiftNumber=%s and numid=%s)"
                          "  BEGIN "
                          "    INSERT INTO RU_T_FISCAL_RECEIPT "
                          "            (cashTotalSum, dateTime, ecashTotalSum, fiscalDocumentNumber,"
                          "             fiscalDriveNumber, fiscalSign, items, kktRegId, nds0, nds10, nds18,"  #fiscalDriveNumber, fiscalSign, items, kktRegId, nds0, nds10, nds18,"
                          "             operationType, operator, receiptCode, requestNumber,"
                          "             shiftNumber, totalSum, usr, userInn, numid)  "
                          "    VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                          "             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                          "  END "
                          "END", receipt)

    conn_ms.commit()
    conn_ms.close()

if __name__ == "__main__":
    main()




        #requests.get('https://api.sbis.ru/ofd/v1/orgs/7825335145/kkts/0000182040024937/storages/8710000100086130/'
                     #'docs?dateFrom=2017-05-01T00:00:00&limit=5', cookies=cooks)