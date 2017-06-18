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

    response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(regid) + '/storages/'
                            + str(storageid) + '/docs?dateFrom=' + str(datefrom) + '&limit=1000', cookies=cooks)
    return response.json()


# def reg_kkt(cursor_ms):
#    cursor_ms.execute("SELECT regId FROM RU_T_FISCAL_KKT;")
#    return cursor_ms.fetchall()

def nds_check(df):
    if 'ndsNo' in df.columns.values:
        df['nds0'] = df['ndsNo']
        df.drop('ndsNo', axis=1, inplace=True)
    if 'ndsCalculated10' in df.columns.values:
        df['nds10'] = df['ndsCalculated10']
        df.drop('ndsCalculated10', axis=1, inplace=True)
    if 'ndsCalculated18' in df.columns.values:
        df['nds18'] = df['ndsCalculated18']
        df.drop('ndsCalculated18', axis=1, inplace=True)
    if 'ndsCalculated0' in df.columns.values:
        df['nds0'] = df['ndsCalculated0']
        df.drop('ndsCalculated0', axis=1, inplace=True)
    if 'nds0' not in df.columns.values:
        df['nds0'] = 0
    if 'nds10' not in df.columns.values:
        df['nds10'] = 0
    if 'nds18' not in df.columns.values:
        df['nds18'] = 0
    return df


def reg_drive(cursor_ms):
    cursor_ms.execute("SELECT regId, storageId FROM RU_T_FISCAL_DRIVE WHERE status=2;")
    return cursor_ms.fetchall()


def main(hour_frame=38):
    test = False
    # ====================
    # АТОРИЗАЦИЯ
    # ====================
    connection, cooks = connect()
    print "\nConnection is established"
    print "_________________________"
    print (connection.status_code)
    print (connection.url)
    print (cooks)
    print "_________________________"
    print "=========================\n"

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL
    # ===========================
    # make a connection to MSSQL iBase RU server
    if test is False:
        conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                                  database=vl.db_ms, charset='utf8')
        cursor_ms = conn_ms.cursor()

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФИСКАЛЬНЫЙ НАКОПИТЕЛЕЙ
    # ====================
    id = (('0000612404012879', '8710000100612901'), ('0000612219058428', '8710000100609863'),
          ('0000612124004976', '8710000100604073'))
    # id = (('0000574146027868', '8710000100779501'),
    #      ('0000574048062921', '8710000100779443'),
    #      ('0000573980017345', '8710000100779164'))

    if test is False:
        id = reg_drive(cursor_ms)
    # id = (('0000544620064870', '8710000100512111'),)
    #id = (('0000509048051340', '8710000100373867'),)
    print id

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
    items = pd.DataFrame()
    properties = pd.DataFrame()
    modifiers = pd.DataFrame()

    today = datetime.datetime.today()
    day_check = today - datetime.timedelta(hours=hour_frame)
    datas = day_check.isoformat()

    print "Data to check: ", datas
    day = day_check.date().isoformat()


    for k in id:
        print k[0], k[1]
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
                df['dateTime'] = datetime.datetime.fromtimestamp(df['dateTime'])
                df['notTransmittedDocumentsDateTime'] = datetime.datetime.fromtimestamp(
                    df['notTransmittedDocumentsDateTime'])
                # df['dateTime'] = df['dateTime'].apply(lambda x: datetime.datetime.fromtimestamp(x))
                close_shift = pd.concat([close_shift, df])
        except KeyError:
            None
        try:
            receipt_tmp = data['receipt'].dropna()
            for i in receipt_tmp:
                print "Data from OFD: ", i
                try:
                    items_tmp = i['items']
                    indeks = 0
                    for y in items_tmp:
                        df = pd.DataFrame(y, index=[indeks])
                        indeks += 1
                        df['fiscalDriveNumber'] = i['fiscalDriveNumber']
                        df['kktRegId'] = i['kktRegId']
                        df['shiftNumber'] = i['shiftNumber']
                        df['fiscalDocumentNumber'] = i['fiscalDocumentNumber']
                        df = nds_check(df)
                        items = pd.concat([items, df])
                        None
                    del (i['items'])
                except KeyError:
                    None
                try:
                    properties_tmp = i['properties']
                    indeks = 0
                    for y in properties_tmp:
                        df = pd.DataFrame(y, index=[indeks])
                        indeks += 1
                        df['fiscalDriveNumber'] = i['fiscalDriveNumber']
                        df['kktRegId'] = i['kktRegId']
                        df['shiftNumber'] = i['shiftNumber']
                        df['fiscalDocumentNumber'] = i['fiscalDocumentNumber']
                        properties = pd.concat([properties, df])
                    del (i['properties'])
                except KeyError:
                    None
                try:
                    modifiers_tmp = i['modifiers']
                    indeks = 0
                    for y in modifiers_tmp:
                        df = pd.DataFrame(y, index=[indeks])
                        indeks += 1
                        df['fiscalDriveNumber'] = i['fiscalDriveNumber']
                        df['kktRegId'] = i['kktRegId']
                        df['shiftNumber'] = i['shiftNumber']
                        df['fiscalDocumentNumber'] = i['fiscalDocumentNumber']
                        modifiers = pd.concat([modifiers, df])
                    del (i['modifiers'])
                except KeyError:
                    None
                df = pd.DataFrame(i, index=[0])
                df['dateTime'] = df['dateTime'].apply(lambda x: datetime.datetime.fromtimestamp(x))
                # items_tmp = df['items']
                # df.drop('items', axis=1, inplace=True)
                # ind = 0
                # dy = pd.DataFrame()
                # for y in items_tmp:
                #    dy = pd.concat([dy, pd.DataFrame(y, index=[ind])])
                #    ind += 1
                # df = df.merge(dy, left_index=True, right_index=True, how='inner')
                # df.fillna(value=0, inplace=True)
                df = nds_check(df)
                df.drop('rawData', axis=1, inplace=True)
                receipt = pd.concat([receipt, df])
        except KeyError:
            None
    connection.close()

    # ===========================
    # ПОДГОТОВКА ДАННЫХ ОБ ОТКРЫТИИ СМЕН
    # ===========================
    ind_oshift = False
    try:
        open_shift = open_shift[['code', 'dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'fiscalSign',
                                 'kktRegId', 'operator', 'rawData', 'shiftNumber', 'userInn']]
        open_shift.drop('rawData', axis=1, inplace=True)
        open_shift.to_csv('openshift_' + str(day) + '.csv', sep=';', encoding='utf-8')
        open_shift = [((x[1], x[3], x[5], x[7]) + tuple(x)) for x in open_shift.values.tolist()]
        print "OPEN SHIFTS"
        print open_shift
        ind_oshift = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ЗАКРЫТИИ СМЕН
    # ===========================
    ind_cshift = False
    try:
        close_shift = close_shift[['code', 'dateTime', 'documentsQuantity', 'fiscalDocumentNumber',
                                   'fiscalDriveExhaustionSign', 'fiscalDriveMemoryExceededSign',
                                   'fiscalDriveNumber', 'fiscalDriveReplaceRequiredSign', 'fiscalSign', 'kktRegId',
                                   'notTransmittedDocumentsDateTime', 'notTransmittedDocumentsQuantity',
                                   'ofdResponseTimeoutSign', 'operator', 'rawData', 'receiptsQuantity',
                                   'shiftNumber', 'userInn']]
        close_shift.drop('rawData', axis=1, inplace=True)
        close_shift.to_csv('closeshift_' + str(day) + '.csv', sep=';', encoding='utf-8')
        close_shift = [((x[1], x[6], x[9], x[15]) + tuple(x)) for x in close_shift.values.tolist()]
        print "CLOSED SHIFTS"
        print close_shift
        ind_cshift = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ЧЕКАХ
    # ===========================
    ind_receipt = False
    try:
        receipt = receipt[['cashTotalSum', 'dateTime', 'ecashTotalSum', 'fiscalDocumentNumber',
                           'fiscalDriveNumber', 'fiscalSign', 'kktRegId', 'nds0', 'nds10', 'nds18',
                           'operationType', 'operator', 'receiptCode', 'requestNumber',
                           'shiftNumber', 'user', 'userInn']]
        receipt['nds0'] /= 100
        receipt['nds10'] /= 100
        receipt['nds18'] /= 100
        receipt['cashTotalSum'] /= 100
        receipt['ecashTotalSum'] /= 100
        receipt.to_csv('receipt_' + str(day) + '.csv', sep=';', encoding='utf-8')
        receipt = [((x[1], x[4], x[6], x[14], x[3]) + tuple(x)) for x in receipt.values.tolist()]
        print "RECEIPT"
        print receipt
        ind_receipt = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ПРОДУКТАХ В ЧЕКАХ
    # ===========================
    ind_item = False
    try:
        items['numid'] = items.index.values
        items = items[['fiscalDocumentNumber', 'fiscalDriveNumber', 'kktRegId', 'shiftNumber', 'numid',
                       'nds0', 'nds10', 'nds18',
                       'name', 'price', 'quantity', 'sum']]
        items['nds0'] /= 100
        items['nds10'] /= 100
        items['nds18'] /= 100
        items['sum'] /= 100
        items['price'] /= 100
        items.to_csv('items_' + str(day) + '.csv', sep=';', encoding='utf-8')
        items = [((x[0], x[1], x[2], x[3], x[4]) + tuple(x)) for x in items.values.tolist()]
        print "RECEIPT"
        print items
        ind_item = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О СВОЙСТВАХ ЧЕКА
    # ===========================
    ind_property = False
    try:
        properties['numid'] = properties.index.values
        properties = properties[['fiscalDocumentNumber', 'fiscalDriveNumber', 'kktRegId', 'shiftNumber', 'numid',
                                 'key', 'value']]
        properties.to_csv('properties_' + str(day) + '.csv', sep=';', encoding='utf-8')
        properties = [((x[0], x[1], x[2], x[3], x[4]) + tuple(x)) for x in properties.values.tolist()]
        print "PROPERIES"
        print properties
        ind_property = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О МОДИФИКАТОРАХ ЧЕКА
    # ===========================
    ind_modifier = False
    try:
        modifiers['numid'] = modifiers.index.values
        modifiers = modifiers[['fiscalDocumentNumber', 'fiscalDriveNumber', 'kktRegId', 'shiftNumber', 'numid',
                               'discountSum']]
        modifiers.to_csv('modifiers_' + str(day) + '.csv', sep=';', encoding='utf-8')
        modifiers = [((x[0], x[1], x[2], x[3], x[4]) + tuple(x)) for x in modifiers.values.tolist()]
        print "MODIFIERS"
        print modifiers
        ind_modifier = True
    except KeyError:
        None

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================

    # ДОБАВЛЯЕМ ОТКРЫТЫЕ СМЕНЫ
    if test is False:
        if ind_oshift:
            cursor_ms.executemany("BEGIN "
                                  "  IF NOT EXISTS "
                                  "    (SELECT 1 FROM RU_T_FISCAL_OSHIFT WHERE dateTime=%s and fiscalDriveNumber=%s "
                                  "                                            and kktRegId=%s and shiftNumber=%s)"
                                  "  BEGIN "
                                  "    INSERT INTO RU_T_FISCAL_OSHIFT "
                                  "         (code, dateTime, fiscalDocumentNumber, fiscalDriveNumber, fiscalSign, "
                                  "          kktRegId, operator, shiftNumber, usrInn) "
                                  "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                  "  END "
                                  "END", open_shift)

        # ЗАКРЫТЫЕ СМЕНЫ
        if ind_cshift:
            cursor_ms.executemany("BEGIN "
                                  "  IF NOT EXISTS "
                                  "    (SELECT 1 FROM RU_T_FISCAL_CSHIFT WHERE dateTime=%s and fiscalDriveNumber=%s"
                                  "                                            and kktRegId=%s and shiftNumber=%s)"
                                  "  BEGIN "
                                  "    INSERT INTO RU_T_FISCAL_CSHIFT "
                                  "           (code, dateTime, documentsQuantity, fiscalDocumentNumber,"
                                  "            fiscalDriveExhaustionSign, fiscalDriveMemoryExceededSign,"
                                  "            fiscalDriveNumber, fiscalDriveReplaceRequiredSign, fiscalSign, kktRegId,"
                                  "            notTransmittedDocumentsDateTime, notTransmittedDocumentsQuantity,"
                                  "            ofdResponseTimeoutSign, operator, receiptsQuantity,"
                                  "            shiftNumber, userInn)"
                                  "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                  "            %s, %s, %s, %s, %s, %s, %s)"
                                  "  END "
                                  "END", close_shift)

        # ЧЕКИ
        if ind_receipt:
            cursor_ms.executemany("BEGIN "
                                  "  IF NOT EXISTS "
                                  "    (SELECT 1 FROM RU_T_FISCAL_RECEIPT WHERE dateTime=%s and fiscalDriveNumber=%s"
                                  "                                            and kktRegId=%s and shiftNumber=%s and "
                                  "                                            fiscalDocumentNumber=%s)"
                                  "  BEGIN "
                                  "    INSERT INTO RU_T_FISCAL_RECEIPT "
                                  "            (cashTotalSum, dateTime, ecashTotalSum, fiscalDocumentNumber,"
                                  "             fiscalDriveNumber, fiscalSign, kktRegId, nds0, nds10, nds18,"
                                  "             operationType, operator, receiptCode, requestNumber,"
                                  "             shiftNumber, usr, userInn)  "
                                  "    VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                  "             %s, %s, %s, %s, %s, %s, %s)"
                                  "  END "
                                  "END", receipt)
        # СОСТАВ ЧЕКА
        if ind_item:
            cursor_ms.executemany("BEGIN "
                                  "  IF NOT EXISTS "
                                  "    (SELECT 1 FROM RU_T_FISCAL_ITEMS WHERE fiscalDocumentNumber=%s and "
                                  "                                           fiscalDriveNumber=%s"
                                  "                                           and kktRegId=%s and shiftNumber=%s and "
                                  "                                           numid=%s)"
                                  "  BEGIN "
                                  "    INSERT INTO RU_T_FISCAL_ITEMS "
                                  "            (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                  "             nds0, nds10, nds18, name, price, quantity, sum)  "
                                  "    VALUES  (%s, %s, %s, %s, %s, "
                                  "             %s, %s, %s, %s, %s, %s, %s)"
                                  "  END "
                                  "END", items)
        if ind_property:
            cursor_ms.executemany("BEGIN "
                                  "  IF NOT EXISTS "
                                  "    (SELECT 1 FROM RU_T_FISCAL_PROPERTIES WHERE fiscalDocumentNumber=%s and "
                                  "                                           fiscalDriveNumber=%s"
                                  "                                           and kktRegId=%s and shiftNumber=%s and "
                                  "                                           numid=%s)"
                                  "  BEGIN "
                                  "    INSERT INTO RU_T_FISCAL_PROPERTIES "
                                  "            (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                  "             keys, value)  "
                                  "    VALUES  (%s, %s, %s, %s, %s, "
                                  "             %s, %s)"
                                  "  END "
                                  "END", properties)
        if ind_modifier:
            cursor_ms.executemany("BEGIN "
                                  "  IF NOT EXISTS "
                                  "    (SELECT 1 FROM RU_T_FISCAL_MODIFIERS WHERE fiscalDocumentNumber=%s and "
                                  "                                           fiscalDriveNumber=%s"
                                  "                                           and kktRegId=%s and shiftNumber=%s and "
                                  "                                           numid=%s)"
                                  "  BEGIN "
                                  "    INSERT INTO RU_T_FISCAL_MODIFIERS "
                                  "            (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                  "             discountsum)  "
                                  "    VALUES  (%s, %s, %s, %s, %s, "
                                  "             %s)"
                                  "  END "
                                  "END", modifiers)

        conn_ms.commit()
        conn_ms.close()
        print "Program is Finished."


if __name__ == "__main__":
    main()

