#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl
import sys


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


def list_checks(cooks, regid, storageid, datefrom, dateto=None, inn='7825335145'):
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
    if dateto is None:
        response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(regid) + '/storages/'
                                + str(storageid) + '/docs?dateFrom=' + str(datefrom) + '&limit=350', cookies=cooks)  # + '&limit=1000'
    else:
        response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(regid) + '/storages/'
                                + str(storageid) + '/docs?dateFrom=' + str(datefrom) + '&dateTo=' + str(dateto) +
                                '&limit=100', cookies=cooks)
    return response.json()


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


def main(test=True, reg_id=None, storage_id=None, date_from=None, date_to=None, hour_frame=2):
    """
    :param test:
    :param reg_id: регистрационный номер принтера
    :param storage_id: регистрационный номер ФН
    :param date_from: с какой даты мы хотим начать забирать данные формат пример - 2017-06-18T00:00:00
    :param date_to: по какую дату хоти забирать данные
    :param hour_frame: какой сдвиг назад часов, используется по умолчанию
    :return:
    """

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

    if reg_id is None or storage_id is None:
        if test is False:
            id = reg_drive(cursor_ms)
        else:
            reg_id = '0000083853048447'
            storage_id = '8710000100099930'
            id = ((reg_id, storage_id),)
    else:
        id = ((reg_id, storage_id),)

    # id = (('0000544620064870', '8710000100512111'),)
    # id = (('0000509048051340', '8710000100373867'),)
    print "Всего принтеров и фискальных накопителей", len(id)

    # date_to = '2017-06-11T20:00:00'
    # date_from = '2017-06-09T20:00:00'
    # ====================
    # определение даты начала сбора информации
    # ====================
    if date_from is None:
        todayx = datetime.datetime.today()
        day_check = todayx - datetime.timedelta(hours=hour_frame)
        date_from = day_check.isoformat()
        #date_to = today.isoformat()
        #date_to = '2017-06-19T20:00:00'
        #date_from = '2017-06-18T00:00:00'
        print "Data from to check: ", date_from
        print "Data to check: ", date_to
        day = day_check.date().isoformat()
    elif date_from is not None:
        todayx = datetime.datetime.today()
        day_check = datetime.datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
        date_from = day_check.isoformat()
        print "Data from is set to: ", date_from
        print "Data to check: ", date_to
        day = day_check.date().isoformat()
    if date_to is not None:
        todayx = datetime.datetime.strptime(date_to, '%Y-%m-%dT%H:%M:%S')
    if (date_to is not None) and (date_to < date_from):
        print "Error datetime, date From: ", date_from, ", date_to: ", date_to
        sys.exit()

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
    data = pd.DataFrame()

    from_d = date_from
    amount_of_kkt = len(id)
    for k in id:
        print "Осталось проверить кол-во принтеров: ", amount_of_kkt
        amount_of_kkt -= 1
        print "Регистрационный номер принтера и ФН ", k[0], k[1]
        data_check = 0
        date_from = from_d
        length = 0
        while data_check == 0:
            if date_to is None:
                try:
                    datat = pd.DataFrame((list_checks(cooks, k[0], k[1], date_from)))
                except ValueError:
                    datat = pd.DataFrame(list_checks(cooks, k[0], k[1], date_from), index=[0])
            else:
                try:
                    datat = pd.DataFrame(list_checks(cooks, k[0], k[1], date_from, date_to))
                except ValueError:
                    datat = pd.DataFrame(list_checks(cooks, k[0], k[1], date_from, date_to), index=[0])
            try:
                rec = datat['receipt'].dropna()
                rec = rec.iloc[-1]
                del(rec['items'])
                df = pd.DataFrame(rec, index=[0])
                data_check = 1
                date = datetime.datetime.fromtimestamp(df['dateTime'])
                if date < todayx:
                    data_check = 0
                    print "  собраны данные с ", date_from, " по ", date
                    print "    еще"
                    date_from = date.isoformat()
                else:
                    print "  достигли финальной даты"
            except KeyError:
                data_check = 1
                print "  Больше нет данных."
            length += len(datat)
            data = pd.concat([data, datat])
        print from_d, "..", todayx, "всего документов", length
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
            counts = 0
            for i in receipt_tmp:
                counts += 1
                #print "Data from OFD: ", i
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
                df = nds_check(df)
                df.drop('rawData', axis=1, inplace=True)
                receipt = pd.concat([receipt, df])
        except KeyError:
            None
    print "\nGetting data from OFD is finished. Total amount of documents: ", len(data)
    # print "=======================================\n"
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
        print "OPEN SHIFTS", len(open_shift)
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
        print "CLOSED SHIFTS", len(close_shift)
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
        print "RECEIPT", len(receipt)
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
        print "ITEMS", len(items)
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
        print "PROPERTIES", len(properties)
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
        print "MODIFIERS", len(modifiers)
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
