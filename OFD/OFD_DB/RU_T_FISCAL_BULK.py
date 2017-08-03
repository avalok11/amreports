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
    print ("\nNew connection is established.")
    print (response.status_code)
    print (response.url)
    print (response.cookies)
    print ("--------------------------------")
    return response, response.cookies


def list_checks(cooks, reg_id, storage_id, date_from, date_to, inn='7825335145'):
    """

    :param date_to: дата до которой делать забор документо - Обязательный
    :param cooks: получаем после успешного подклчения к ОФД
    :param reg_id:
        regId	String, обязательный	Регистрационный номер ККТ, выданный ФНС	«123»
    :param storage_id:
        storageId	String, обязательный	Номер фискального накопителя	«9999999»
    :param date_from:
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
    if date_to is None:
        response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(reg_id) + '/storages/'
                                + str(storage_id) + '/docs?dateFrom=' + str(date_from) + '&limit=1000',
                                cookies=cooks)  # + '&limit=1000'
    else:
        response = requests.get('https://api.sbis.ru/ofd/v1/orgs/' + str(inn) + '/kkts/' + str(reg_id) + '/storages/'
                                + str(storage_id) + '/docs?dateFrom=' + str(date_from) + '&dateTo=' + str(date_to) +
                                '&limit=1000', cookies=cooks)
    return response.json()


def nds_check(df):
    if 'ndsNo' in df.columns.values:
        df['nds0'] = df['ndsNo']
        df.drop('ndsNo', axis=1, inplace=True)
    if 'nds' in df.columns.values:
        df['nds0'] = df['nds']
        df.drop('nds', axis=1, inplace=True)
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

def drive_time(cursor_ms, fiscal_drive, kkt_reg_id):
    cursor_ms.execute("SELECT TOP 1 dateTime "
                      "FROM RU_T_FISCAL_RECEIPT  "
                      "WHERE fiscalDriveNumber=%s AND kktRegId=%s "
                      "ORDER BY dateTime DESC;", (fiscal_drive, kkt_reg_id))
    return cursor_ms.fetchone()


def collect_data(db_read_write=True,
                 reg_id=None, storage_id=None,
                 date_from=None, date_to=None,
                 file_path='logs\\'):
    """
    :param file_path: путь куда сораняем логи
    :param db_read_write: работаем или нет с базой данных
    :param reg_id: регистрационный номер принтера
    :param storage_id: регистрационный номер ФН
    :param date_from: с какой даты мы хотим начать забирать данные формат пример - 2017-06-18T00:00:00
    :param date_to: по какую дату хоти забирать данные
    :return:
    """

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL
    # ===========================
    # make a connection to MSSQL iBase RU server
    if db_read_write:
        conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                                  database=vl.db_ms, charset='utf8')
        cursor_ms = conn_ms.cursor()

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФИСКАЛЬНЫЙ НАКОПИТЕЛЕЙ
    # ====================
    if reg_id is None or storage_id is None:
        id_fn = reg_drive(cursor_ms)  # списко ФН
    else:
        id_fn = ((reg_id, storage_id),)  # списко ФН
    print 'Всего принтеров и фискальных накопителей', len(id_fn)
    print id_fn

    # ====================
    # определение диапазона дат сбора информации date_from till date_to
    # ====================
    id_fn_time = list()  # списко ФН и времени последнего чека (момент с которого надо опрашивать принтер)
    if date_to is not None:
        today_x = datetime.datetime.strptime(date_to, '%Y-%m-%dT%H:%M:%S')  # используется для проверки диапзаона забора данных из ОФД
        date_to = today_x.isoformat()
    else:
        today_x = datetime.datetime.today()
        date_to = today_x.isoformat()
    if date_from is None:
        for reg_id, storage_id in id_fn:
            if db_read_write:
                time = drive_time(cursor_ms, storage_id, reg_id)
                if time is None:
                    time = (datetime.datetime.today() - datetime.timedelta(hours=48)).isoformat()
                    id_fn_time.append((reg_id, storage_id, time))
                    None
                else:
                    time = datetime.datetime.strftime(time[0], '%Y-%m-%dT%H:%M:%S')
                    id_fn_time.append((reg_id, storage_id, time))
                    None
            else:
                time = (datetime.datetime.today() - datetime.timedelta(hours=152)).isoformat()
                id_fn_time.append((reg_id, storage_id, time))

    else:
        date_from = datetime.datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S').isoformat()
        for reg_id, storage_id in id_fn:
            id_fn_time.append(reg_id, storage_id, date_from)
    if (date_to is not None) and (date_to < date_from):
        print("Error datetime, date From: ", date_from, ", date_to: ", date_to)
        sys.exit()
    day = datetime.datetime.today().date().isoformat()
    hour = datetime.datetime.today().hour
    minute = datetime.datetime.today().minute

    # ======================
    # предварительное определение полей значений
    # ======================
    """
    open_shift = pd.DataFrame(columns=['code', 'dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'fiscalSign',
                                       'kktRegId', 'operator', 'rawData', 'shiftNumber', 'userInn'])
    close_shift = pd.DataFrame(columns=['code', 'dateTime', 'documentsQuantity', 'fiscalDocumentNumber',
                                        'fiscalDriveExhaustionSign', 'fiscalDriveMemoryExceededSign',
                                        'fiscalDriveNumber', 'fiscalDriveReplaceRequiredSign', 'fiscalSign', 'kktRegId',
                                        'notTransmittedDocumentsDateTime', 'notTransmittedDocumentsQuantity',
                                        'ofdResponseTimeoutSign', 'operator', 'rawData', 'receiptsQuantity',
                                        'shiftNumber', 'userInn'])
    """
    receipt = pd.DataFrame(columns=['cashTotalSum', 'dateTime', 'ecashTotalSum', 'fiscalDocumentNumber',
                                    'fiscalDriveNumber', 'fiscalSign', 'kktRegId', 'nds0', 'nds10', 'nds18',
                                    'operationType', 'operator', 'receiptCode', 'requestNumber',
                                    'shiftNumber', 'user', 'userInn'])
    open_shift = pd.DataFrame()
    close_shift = pd.DataFrame()
    # receipt = pd.DataFrame()
    items = pd.DataFrame()
    properties = pd.DataFrame()
    modifiers = pd.DataFrame()
    data = pd.DataFrame()

    amount_of_kkt = len(id_fn_time)
    connection, cooks = connect()
    for printer in id_fn_time:
        print "Осталось проверить кол-во принтеров: ", amount_of_kkt
        amount_of_kkt -= 1
        print "Регистрационный номер принтера и ФН ", printer[0], printer[1]
        data_check = 0
        from_d = printer[2]
        date_from = printer[2]
        length = 0
        while data_check == 0:  # проверяем весь ли диапазон дат проверен
            data_t = list_checks(cooks, printer[0], printer[1], date_from, date_to)
            try:
                data_t = pd.DataFrame(data_t)
            except ValueError:
                data_t = pd.DataFrame(data_t, index=[0])
            try:
                last_row = data_t.iloc[-1]  # выбираем последнее найденное значение, если его нет то значит все данные собрали
                if last_row['receipt'] == last_row['receipt']:
                    rec = last_row['receipt']
                    print rec
                elif last_row['closeShift'] == last_row['closeShift']:
                    rec = last_row['closeShift']
                elif last_row['openShift'] == last_row['openShift']:
                    rec = last_row['openShift']
                #rec = data_t['receipt'].dropna().tolist()
                data_check = 1
                date = datetime.datetime.utcfromtimestamp(rec['dateTime'])
                if date < today_x:
                    data_check = 0
                    print "  собраны данные с ", date_from, " по ", date
                    date_from = (date + datetime.timedelta(minutes = 1)).isoformat()
                else:
                    print "  достигли финальной даты"
            except IndexError:
                data_check = 1
                print "  Больше нет данных."
            length += len(data_t)
            data = pd.concat([data, data_t])
            #data = data_t
        print from_d, "..", today_x, "всего документов", length

        if length != 0:  # проверка получили ли данные (оптимизация скорости)
            # ищем открытые смены
            try:
                t_o = datetime.datetime.today()
                open_shift_tmp = pd.DataFrame(data['openShift'].dropna().tolist())
                open_shift_tmp['dateTime'] = open_shift_tmp['dateTime']. \
                    apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                open_shift = pd.concat([open_shift, open_shift_tmp])
                t_of = datetime.datetime.today()
                print " 1. len of open_shift:", len(open_shift_tmp), "time: ", t_of - t_o
            except KeyError:
                None
            # ищем закрытые смены
            try:
                t_c = datetime.datetime.today()
                close_shift_tmp = pd.DataFrame(data['closeShift'].dropna().tolist())
                close_shift_tmp['dateTime'] = close_shift_tmp['dateTime'] \
                    .apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                close_shift_tmp['notTransmittedDocumentsDateTime'] = close_shift_tmp['notTransmittedDocumentsDateTime'] \
                    .apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                close_shift = pd.concat([close_shift, close_shift_tmp])
                t_cf = datetime.datetime.today()
                print " 2. len of close_shift: ", len(close_shift_tmp), "time: ", t_cf - t_c
            except KeyError:
                None
            # ищем чеки и их содержимое
            try:
                t_r = datetime.datetime.today()
                receipt_tmp = pd.DataFrame(data['receipt'].dropna().tolist())
                receipt_tmp['dateTime'] = receipt_tmp['dateTime'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                receipt_tmp = nds_check(receipt_tmp)
                receipt_tmp.drop('rawData', axis=1, inplace=True)
                # содержимое чека - items
                try:
                    items_tmp = receipt_tmp['items'].dropna().tolist()
                    count = 0
                    for i in items_tmp:
                        dataf = pd.DataFrame(i)
                        dataf['fiscalDriveNumber'] = receipt_tmp.iloc[count]['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp.iloc[count]['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp.iloc[count]['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp.iloc[count]['fiscalDocumentNumber']
                        dataf['numid'] = dataf.index.values
                        items = pd.concat([items, dataf])
                        count += 1
                    items = nds_check(items)
                except KeyError:
                    None
                # properties
                try:
                    count = 0
                    properties_tmp = receipt_tmp['properties'].dropna().tolist()
                    for i in properties_tmp:
                        dataf = pd.DataFrame(i)
                        dataf['fiscalDriveNumber'] = receipt_tmp.iloc[count]['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp.iloc[count]['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp.iloc[count]['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp.iloc[count]['fiscalDocumentNumber']
                        dataf['numid'] = dataf.index.values
                        properties = pd.concat([properties, dataf])
                        count += 1
                except KeyError:
                    None
                # modifiers
                try:
                    count = 0
                    modifiers_tmp = receipt_tmp['modifiers'].dropna().tolist()
                    for i in modifiers_tmp:
                        dataf = pd.DataFrame(i)
                        dataf['fiscalDriveNumber'] = receipt_tmp.iloc[count]['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp.iloc[count]['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp.iloc[count]['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp.iloc[count]['fiscalDocumentNumber']
                        dataf['numid'] = dataf.index.values
                        modifiers = pd.concat([modifiers, dataf])
                        count += 1
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
    print "\nGetting data from OFD is finished."

    # ===========================
    # ПОДГОТОВКА ДАННЫХ ОБ ОТКРЫТИИ СМЕН
    # ===========================
    ind_oshift = False
    try:
        open_shift = open_shift[['code', 'dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'fiscalSign',
                                 'kktRegId', 'operator', 'rawData', 'shiftNumber', 'userInn']]
        open_shift.drop('rawData', axis=1, inplace=True)
        outfile = file_path + 'openshift_' + str(day) + '_' + str(hour) + str(minute) + '.csv'
        outfilex = open(file_path + 'openshift.txt', 'wb')
        open_shift.to_csv(outfile, sep=';', encoding='utf-8')
        open_shift.to_csv(outfilex, sep=';', encoding='windows-1251',
                          header=False, index=False)

        outfilex.flush()
        open_shift_e = [((x[1], x[3], x[5], x[7]) + tuple(x)) for x in open_shift.values.tolist()]
        open_shift = [(tuple(x)) for x in open_shift.values.tolist()]
        print "OPEN SHIFTS", len(open_shift)
        ind_oshift = True
        outfilex.close()
    except KeyError:
        None
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
        close_shift.to_csv(file_path + 'closedshift_' + str(day) + '_' + str(hour) + str(minute) + '.csv', sep=';', encoding='utf-8')
        close_shift.to_csv(file_path + 'closedshift.txt', sep=';', encoding='utf-8',
                           header=False, index=False)
        close_shift_e = [((x[1], x[6], x[9], x[15]) + tuple(x)) for x in close_shift.values.tolist()]
        close_shift = [(tuple(x)) for x in close_shift.values.tolist()]
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
        receipt.to_csv(file_path + 'receipt_' + str(day) + '_' + str(hour) + str(minute) + '.csv', sep=';', encoding='utf-8')
        receipt.to_csv(file_path + 'receipt.txt', sep=';', encoding='utf-8',
                       header=False, index=False)
        receipt_e = [((x[1], x[4], x[6], x[14], x[3]) + tuple(x)) for x in receipt.values.tolist()]
        receipt = [(tuple(x)) for x in receipt.values.tolist()]
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
        items.to_csv(file_path + 'items_' + str(day) + '_' + str(hour) + str(minute) + '.csv', sep=';', encoding='utf-8')
        items.to_csv(file_path + 'items.txt', sep=';', encoding='utf-8',
                     header=False, index=False)
        items_e = [((x[0], x[1], x[2], x[3], x[4]) + tuple(x)) for x in items.values.tolist()]
        items = [(tuple(x)) for x in items.values.tolist()]
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
        properties.to_csv(file_path + 'properties_' + str(day) + '_' + str(hour) + str(minute) + '.csv', sep=';', encoding='utf-8')
        properties.to_csv(file_path + 'properties.txt', sep=';', encoding='utf-8',
                          header=False, index=False)
        properties_e = [((x[0], x[1], x[2], x[3], x[4]) + tuple(x)) for x in properties.values.tolist()]
        properties = [(tuple(x)) for x in properties.values.tolist()]
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
        modifiers.to_csv(file_path + 'modifiers_' + str(day) + '_' + str(hour) + str(minute) + '.csv', sep=';', encoding='utf-8')
        modifiers.to_csv(file_path + 'modifiers.txt', sep=';', encoding='utf-8',
                         header=False, index=False)
        modifiers_e = [((x[0], x[1], x[2], x[3], x[4]) + tuple(x)) for x in modifiers.values.tolist()]
        modifiers = [(tuple(x)) for x in modifiers.values.tolist()]
        print "MODIFIERS", len(modifiers)
        ind_modifier = True
    except KeyError:
        None

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================

    # ДОБАВЛЯЕМ ОТКРЫТЫЕ СМЕНЫ
    if db_read_write is True and db_write is True:
        if ind_oshift:
            print "\n copy open shifts to MSSQL"
            if check_exist and bulk_insert is False:
                cursor_ms.executemany("BEGIN "
                                      "  IF NOT EXISTS "
                                      "    (SELECT 1 FROM RU_T_FISCAL_OSHIFT WHERE dateTime=%s and fiscalDriveNumber=%s"
                                      "                                            and kktRegId=%s and shiftNumber=%s)"
                                      "  BEGIN "
                                      "    INSERT INTO RU_T_FISCAL_OSHIFT "
                                      "         (code, dateTime, fiscalDocumentNumber, fiscalDriveNumber, fiscalSign, "
                                      "          kktRegId, operator, shiftNumber, usrInn) "
                                      "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                      "  END "
                                      "END", open_shift_e)
            elif bulk_insert:
                a = "ruhqapp001"
                b = "GROUPS"
                c = "MIS"
                d = "share_ofd"
                e = "openshift.txt"
                cursor_ms.execute("BULK INSERT [DataWarehouse].[dbo].[RU_T_FISCAL_OSHIFT] "
                                  " FROM '\\{0}\{1}\{2}\{3}\{4}' "
                                  " WITH "
                                  "  ( "
                                  "  FIELDTERMINATOR = ';', "
                                  "  ROWTERMINATOR = '\n' "
                                  "   )".format(a,b,c,d,e))
            else:
                cursor_ms.executemany("    INSERT INTO RU_T_FISCAL_OSHIFT "
                                      "         (code, dateTime, fiscalDocumentNumber, fiscalDriveNumber, fiscalSign, "
                                      "          kktRegId, operator, shiftNumber, usrInn) "
                                      "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", open_shift)


        # ЗАКРЫТЫЕ СМЕНЫ
        if ind_cshift:
            print "copy closed shifts to MSSQL"
            if check_exist:
                cursor_ms.executemany("BEGIN "
                                      "  IF NOT EXISTS "
                                      "    (SELECT 1 FROM RU_T_FISCAL_CSHIFT WHERE dateTime=%s and fiscalDriveNumber=%s"
                                      "                                            and kktRegId=%s and shiftNumber=%s)"
                                      "  BEGIN "
                                      "    INSERT INTO RU_T_FISCAL_CSHIFT "
                                      "           (code, dateTime, documentsQuantity, fiscalDocumentNumber,"
                                      "            fiscalDriveExhaustionSign, fiscalDriveMemoryExceededSign,"
                                      "        fiscalDriveNumber, fiscalDriveReplaceRequiredSign, fiscalSign, kktRegId,"
                                      "            notTransmittedDocumentsDateTime, notTransmittedDocumentsQuantity,"
                                      "            ofdResponseTimeoutSign, operator, receiptsQuantity,"
                                      "            shiftNumber, userInn)"
                                      "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                      "            %s, %s, %s, %s, %s, %s, %s)"
                                      "  END "
                                      "END", close_shift_e)
            else:
                cursor_ms.executemany("INSERT INTO RU_T_FISCAL_CSHIFT "
                                      "           (code, dateTime, documentsQuantity, fiscalDocumentNumber,"
                                      "            fiscalDriveExhaustionSign, fiscalDriveMemoryExceededSign,"
                                      "        fiscalDriveNumber, fiscalDriveReplaceRequiredSign, fiscalSign, kktRegId,"
                                      "            notTransmittedDocumentsDateTime, notTransmittedDocumentsQuantity,"
                                      "            ofdResponseTimeoutSign, operator, receiptsQuantity,"
                                      "            shiftNumber, userInn)"
                                      "    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                                      "            %s, %s, %s, %s, %s, %s, %s ", close_shift)
            if bulk_insert:
                cursor_ms.execute("BULK INSERT [DataWarehouse].[dbo].[RU_T_FISCAL_CSHIFT] "
                                  " FROM '\\ruhqapp001\GROUPS\MIS\share_ofd\closedshift.txt' "
                                  " WITH "
                                  "  ( "
                                  "  FIELDTERMINATOR = ';', "
                                  "  ROWTERMINATOR = '\n' "
                                  "   )")

        # ЧЕКИ
        if ind_receipt:
            print "copy checks to MSSQL"
            if check_exist:
                cursor_ms.executemany("BEGIN "
                                      "  IF NOT EXISTS "
                                      "   (SELECT 1 FROM RU_T_FISCAL_RECEIPT WHERE dateTime=%s and fiscalDriveNumber=%s"
                                      "                                         and kktRegId=%s and shiftNumber=%s and "
                                      "                                         fiscalDocumentNumber=%s)"
                                      "  BEGIN "
                                      "    INSERT INTO RU_T_FISCAL_RECEIPT "
                                      "            (cashTotalSum, dateTime, ecashTotalSum, fiscalDocumentNumber,"
                                      "             fiscalDriveNumber, fiscalSign, kktRegId, nds0, nds10, nds18,"
                                      "             operationType, operator, receiptCode, requestNumber,"
                                      "             shiftNumber, usr, userInn)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                      "             %s, %s, %s, %s, %s, %s, %s)"
                                      "  END "
                                      "END", receipt_e)
            else:
                cursor_ms.executemany("INSERT INTO RU_T_FISCAL_RECEIPT "
                                      "            (cashTotalSum, dateTime, ecashTotalSum, fiscalDocumentNumber,"
                                      "             fiscalDriveNumber, fiscalSign, kktRegId, nds0, nds10, nds18,"
                                      "             operationType, operator, receiptCode, requestNumber,"
                                      "             shiftNumber, usr, userInn)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                      "             %s, %s, %s, %s, %s, %s, %s)", receipt)
            if bulk_insert:
                cursor_ms.execute("BULK INSERT [DataWarehouse].[dbo].[RU_T_FISCAL_RECEIPT] "
                                  " FROM '\\ruhqapp001\GROUPS\MIS\share_ofd\\receipt.txt' "
                                  " WITH "
                                  "  ( "
                                  "  FIELDTERMINATOR = ';', "
                                  "  ROWTERMINATOR = '\n' "
                                  "   )")
        # СОСТАВ ЧЕКА
        if ind_item:
            print "copy items to MSSQL"
            if check_exist:
                cursor_ms.executemany("BEGIN "
                                      "  IF NOT EXISTS "
                                      "    (SELECT 1 FROM RU_T_FISCAL_ITEMS WHERE fiscalDocumentNumber=%s and "
                                      "                                           fiscalDriveNumber=%s"
                                      "                                         and kktRegId=%s and shiftNumber=%s and "
                                      "                                           numid=%s)"
                                      "  BEGIN "
                                      "    INSERT INTO RU_T_FISCAL_ITEMS "
                                      "         (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                      "             nds0, nds10, nds18, name, price, quantity, sum)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, "
                                      "             %s, %s, %s, %s, %s, %s, %s)"
                                      "  END "
                                      "END", items_e)
            else:
                cursor_ms.executemany("INSERT INTO RU_T_FISCAL_ITEMS "
                                      "         (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                      "             nds0, nds10, nds18, name, price, quantity, sum)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, "
                                      "             %s, %s, %s, %s, %s, %s, %s)", items)
            if bulk_insert:
                cursor_ms.execute("BULK INSERT [DataWarehouse].[dbo].[RU_T_FISCAL_ITEMS] "
                                  " FROM '\\ruhqapp001\GROUPS\MIS\share_ofd\\items.txt' "
                                  " WITH "
                                  "  ( "
                                  "  FIELDTERMINATOR = ';', "
                                  "  ROWTERMINATOR = '\n' "
                                  "   )")
        if ind_property:
            print "copy properties to MSSQL"
            if check_exist:
                cursor_ms.executemany("BEGIN "
                                      "  IF NOT EXISTS "
                                      "    (SELECT 1 FROM RU_T_FISCAL_PROPERTIES WHERE fiscalDocumentNumber=%s and "
                                      "                                           fiscalDriveNumber=%s"
                                      "                                         and kktRegId=%s and shiftNumber=%s and "
                                      "                                           numid=%s)"
                                      "  BEGIN "
                                      "    INSERT INTO RU_T_FISCAL_PROPERTIES "
                                      "         (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                      "             keys, value)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, "
                                      "             %s, %s)"
                                      "  END "
                                      "END", properties_e)
            else:
                cursor_ms.executemany("INSERT INTO RU_T_FISCAL_PROPERTIES "
                                      "         (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                      "             keys, value)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, %s, %s)", properties)
            if bulk_insert:
                cursor_ms.execute("BULK INSERT [DataWarehouse].[dbo].[RU_T_FISCAL_PROPERTIES] "
                                  " FROM '\\ruhqapp001\GROUPS\MIS\share_ofd\\properties.txt' "
                                  " WITH "
                                  "  ( "
                                  "  FIELDTERMINATOR = ';', "
                                  "  ROWTERMINATOR = '\n' "
                                  "   )")

        if ind_modifier:
            print("copy modifiers to MSSQL")
            if check_exist:
                cursor_ms.executemany("BEGIN "
                                      "  IF NOT EXISTS "
                                      "    (SELECT 1 FROM RU_T_FISCAL_MODIFIERS WHERE fiscalDocumentNumber=%s and "
                                      "                                           fiscalDriveNumber=%s"
                                      "                                         and kktRegId=%s and shiftNumber=%s and "
                                      "                                           numid=%s)"
                                      "  BEGIN "
                                      "    INSERT INTO RU_T_FISCAL_MODIFIERS "
                                      "         (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                      "             discountsum)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, "
                                      "             %s)"
                                      "  END "
                                      "END", modifiers_e)
            else:
                cursor_ms.executemany("INSERT INTO RU_T_FISCAL_MODIFIERS "
                                      "         (fiscalDocumentNumber, fiscalDriveNumber, kktRegId, shiftNumber, numid,"
                                      "             discountsum)  "
                                      "    VALUES  (%s, %s, %s, %s, %s, %s)", modifiers)

        print("\nCOMMITMENT")
        conn_ms.commit()
        conn_ms.close()
    print("Program is Finished.")


if __name__ == "__main__":
    collect_data()
