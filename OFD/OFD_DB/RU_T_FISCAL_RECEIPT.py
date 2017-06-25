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
    print "\nNew connection is established."
    print (response.status_code)
    print (response.url)
    print (response.cookies)
    print "--------------------------------"
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
                                + str(storageid) + '/docs?dateFrom=' + str(datefrom) + '&limit=1000', cookies=cooks)  # + '&limit=1000'
    else:
        response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(regid) + '/storages/'
                                + str(storageid) + '/docs?dateFrom=' + str(datefrom) + '&dateTo=' + str(dateto) +
                                '&limit=1000', cookies=cooks)
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
    if reg_id is None or storage_id is None:
        if test is False:
            id = reg_drive(cursor_ms)
        else:
            reg_id = '0000083853048447'  # для тестирования
            storage_id = '8710000100099930'  # для тестирования
            id = ((reg_id, storage_id),)
    else:
        id = ((reg_id, storage_id),)

    print "Всего принтеров и фискальных накопителей", len(id)

    # ====================
    # определение даты начала сбора информации
    # ====================
    if date_from is None:
        todayx = datetime.datetime.today()
        day_check = todayx - datetime.timedelta(hours=hour_frame)
        date_from = day_check.isoformat()
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
        connection, cooks = connect()
        print "Осталось проверить кол-во принтеров: ", amount_of_kkt
        amount_of_kkt -= 1
        print "Регистрационный номер принтера и ФН ", k[0], k[1]
        data_check = 0
        date_from = from_d
        length = 0
        while data_check == 0:  # проверяем весь ли диапазон дат проверен
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
                #rec = rec.iloc[-1]
                #del(rec['items'])
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

        if length != 0:  # проверка получили ли данные (оптимизация скорости)
            # ищем открытые смены
            try:
                t_o = datetime.datetime.today()
                open_shift_tmp = pd.DataFrame(data['openShift'].dropna().tolist())
                open_shift_tmp['dateTime'] = open_shift_tmp['dateTime'].apply(lambda x: datetime.datetime.fromtimestamp(x))
                open_shift = pd.concat([open_shift, open_shift_tmp])
                t_of = datetime.datetime.today()
                print " 1. len of open_shift:", len(open_shift_tmp), "time: ", t_of-t_o
            except KeyError:
                None
            # ищем закрытые смены
            try:
                t_c = datetime.datetime.today()
                close_shift_tmp = pd.DataFrame(data['closeShift'].dropna().tolist())
                close_shift_tmp['dateTime'] = close_shift_tmp['dateTime']\
                    .apply(lambda x: datetime.datetime.fromtimestamp(x))
                close_shift_tmp['notTransmittedDocumentsDateTime'] = close_shift_tmp['notTransmittedDocumentsDateTime']\
                    .apply(lambda x: datetime.datetime.fromtimestamp(x))
                close_shift = pd.concat([close_shift, close_shift_tmp])
                t_cf = datetime.datetime.today()
                print " 2. len of close_shift: ", len(close_shift_tmp), "time: ", t_cf-t_c
            except KeyError:
                None
            # ищем чеки и их содержимое
            try:
                t_r = datetime.datetime.today()
                receipt_tmp = pd.DataFrame(data['receipt'].dropna().tolist())
                receipt_tmp['dateTime'] = receipt_tmp['dateTime'].apply(lambda x: datetime.datetime.fromtimestamp(x))
                receipt_tmp = nds_check(receipt_tmp)
                receipt_tmp.drop('rawData', axis=1, inplace=True)
                # содержимое чека - items
                try:
                    items_tmp = pd.DataFrame(receipt_tmp['items'].dropna().tolist())
                    for c in items_tmp.columns.values:
                        dataf = pd.DataFrame(items_tmp[c].dropna().tolist())
                        dataf['fiscalDriveNumber'] = receipt_tmp['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp['fiscalDocumentNumber']
                        dataf['numid'] = c
                        items = pd.concat([items, dataf])
                    items = nds_check(items)
                except KeyError:
                    None
                # properties
                try:
                    properties_tmp = pd.DataFrame(receipt_tmp['properties'].dropna().tolist())
                    for c in properties_tmp.columns.values:
                        dataf = pd.DataFrame(properties_tmp[c].dropna().tolist())
                        dataf['fiscalDriveNumber'] = receipt_tmp['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp['fiscalDocumentNumber']
                        dataf['numid'] = c
                        properties = pd.concat([properties, dataf])
                except KeyError:
                    None
                # modifiers
                try:
                    modifiers_tmp = pd.DataFrame(receipt_tmp['modifiers'].dropna().tolist())
                    for c in modifiers_tmp.columns.values:
                        dataf = pd.DataFrame(modifiers_tmp[c].dropna().tolist())
                        dataf['fiscalDriveNumber'] = receipt_tmp['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp['fiscalDocumentNumber']
                        dataf['numid'] = c
                        modifiers = pd.concat([modifiers, dataf])
                except KeyError:
                    None
                receipt = pd.concat([receipt, receipt_tmp])
                t_rf = datetime.datetime.today()
                print " 3. work on receipts, total: ", len(receipt_tmp), "time: ", t_rf - t_r
            except KeyError:
                None
            try:
                receipt.drop('items', axis=1, inplace=True)
            except ValueError:
                None
            try:
                receipt.drop('properties', axis=1, inplace=True)
            except ValueError:
                None
            try:
                receipt.drop('modifiers', axis=1, inplace=True)
            except ValueError:
                None
            print "Connection closed:", datetime.datetime.today(), "\n"
        connection.close()
    print "\nGetting data from OFD is finished. Total amount of documents: ", len(data)

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
        receipt.fillna(0, inplace=True)
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
        items = items[['fiscalDocumentNumber', 'fiscalDriveNumber', 'kktRegId', 'shiftNumber', 'numid',
                       'nds0', 'nds10', 'nds18',
                       'name', 'price', 'quantity', 'sum']]
        items['nds0'] /= 100
        items['nds10'] /= 100
        items['nds18'] /= 100
        items['sum'] /= 100
        items['price'] /= 100
        items.fillna(0, inplace=True)
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
