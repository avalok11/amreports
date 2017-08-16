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
        dateTo	String	Время окончания периода запрашиваемых документов. Если не указано, берётся текущее время
          «2016-11-19T23:20:45»
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


def main(db_read_write=True,
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
    cursor_ms = False
    if db_read_write:
        conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                                  database=vl.db_ms, charset='utf8')
        cursor_ms = conn_ms.cursor()

    # ====================
    # ПОЛУЧЕНИЕ СПИСКА ФИСКАЛЬНЫЙ НАКОПИТЕЛЕЙ
    # ====================
    if reg_id is None or storage_id is None and db_read_write is True:
        id_fn = reg_drive(cursor_ms)  # списко ФН
    else:
        id_fn = ((reg_id, storage_id),)  # списко ФН
        # id_fn = (('0001104870020004', '8710000100840306'), ('0000734403026836', '8710000100978624'))  # списко ФН
    print 'Всего принтеров и фискальных накопителей', len(id_fn)

    # ====================
    # определение диапазона дат сбора информации date_from till date_to
    # ====================
    id_fn_time = list()  # списко ФН и времени последнего чека (момент с которого надо опрашивать принтер)
    if date_to is not None:
        today_x = datetime.datetime.strptime(date_to, '%Y-%m-%dT%H:%M:%S')  # используется для проверки диапазона дат
        date_to = today_x.isoformat()
        date_is_fixed = 1
    else:
        today_x = datetime.datetime.today()
        date_is_fixed = 0
        date_to = datetime.datetime.today().isoformat()
    if date_from is None:
        for reg_id, storage_id in id_fn:
            if db_read_write:
                time = drive_time(cursor_ms, storage_id, reg_id)
                if time is None:
                    time = (datetime.datetime.today() - datetime.timedelta(hours=48)).isoformat()
                    id_fn_time.append((reg_id, storage_id, time))
                else:
                    time = datetime.datetime.strftime(time[0], '%Y-%m-%dT%H:%M:%S')
                    id_fn_time.append((reg_id, storage_id, time))
            else:
                time = (datetime.datetime.today() - datetime.timedelta(hours=152)).isoformat()
                id_fn_time.append((reg_id, storage_id, time))

    else:
        date_from = datetime.datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S').isoformat()
        for reg_id, storage_id in id_fn:
            id_fn_time.append((reg_id, storage_id, date_from))
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
    closed_shift = pd.DataFrame(columns=['code', 'dateTime', 'documentsQuantity', 'fiscalDocumentNumber',
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
    closed_shift = pd.DataFrame()
    # receipt = pd.DataFrame()
    items = pd.DataFrame()
    properties = pd.DataFrame()
    modifiers = pd.DataFrame()
    # data = pd.DataFrame()

    amount_of_kkt = len(id_fn_time)
    connection, cooks = connect()
    for printer in id_fn_time:
        data = pd.DataFrame()
        print "\nОсталось проверить кол-во принтеров: ", amount_of_kkt
        amount_of_kkt -= 1
        print "Регистрационный номер принтера и ФН ", printer[0], printer[1]
        data_check = 0
        from_d = printer[2]
        date_from = printer[2]
        length = 0
        date = None
        if date_is_fixed == 0:
            today_x = datetime.datetime.today()
            date_to = today_x.isoformat()
        print "Зарпашиваемый диапазон сбора данных: ", date_from, " .. ", date_to
        while data_check == 0:  # проверяем весь ли диапазон дат проверен
            # если диапзаон больше чем 7 дней то надо разбивать диапазон
            try:
                date_s = datetime.datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                date_s = datetime.datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S.%f')
            try:
                date_e = datetime.datetime.strptime(date_to, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                date_e = datetime.datetime.strptime(date_to, '%Y-%m-%dT%H:%M:%S.%f')
            if (date_e-date_s).days > 7:
                date_to_x = datetime.datetime.strftime(date_s + datetime.timedelta(days=7), '%Y-%m-%dT%H:%M:%S')
                print " собираю данные из диапазона", date_from, " до ", date_to_x
            else:
                date_to_x = date_to
                print " собираю данные из диапазона", date_from, " до ", date_to
            data_t = list_checks(cooks, printer[0], printer[1], date_from, date_to_x)
            try:
                data_t = pd.DataFrame(data_t)
            except ValueError:
                data_t = pd.DataFrame(data_t, index=[0])
            try:
                # выбираем последнее найденное значение, если его нет то значит все данные собрали
                last_row = data_t.iloc[-1]
                if last_row['receipt'] == last_row['receipt']:
                    rec = last_row['receipt']
                elif last_row['closeShift'] == last_row['closeShift']:
                    rec = last_row['closeShift']
                elif last_row['openShift'] == last_row['openShift']:
                    rec = last_row['openShift']
                data_check = 1
                date = datetime.datetime.utcfromtimestamp(rec['dateTime'])
                if date < today_x:
                    data_check = 0
                    print "  собраны данные ", date_from, " по ", date
                    date_from = (date + datetime.timedelta(minutes=1)).isoformat()
                else:
                    print "Достигли финальной даты."
            except IndexError:
                data_check = 1
                print "Больше нет данных."
            except KeyError:
                data_check = 1
                print "Больше нет данных."
            length += len(data_t)
            data = pd.concat([data, data_t]) #- приводит к экспотенциальному увеличения массива данных
            # data = data_t
        print "Данные собраны ", from_d, "..", date, "\nВсего документов получено =", length

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
                closed_shift = pd.concat([closed_shift, close_shift_tmp])
                t_cf = datetime.datetime.today()
                print " 2. len of closed_shift: ", len(close_shift_tmp), "time: ", t_cf - t_c
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
            print "Connection closed:", datetime.datetime.today()
    connection.close()
    print "\nGetting data from OFD is finished."

    # ===========================
    # ПОДГОТОВКА ДАННЫХ ОБ ОТКРЫТИИ СМЕН
    # ===========================
    ind_open = False
    try:
        open_shift = open_shift[['code', 'dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'fiscalSign',
                                 'kktRegId', 'operator', 'rawData', 'shiftNumber', 'userInn']]
        open_shift.drop('rawData', axis=1, inplace=True)
        name_open = 'openshift_' + str(day) + '_' + str(hour) + str(minute) + '.txt'
        open_shift.to_csv(file_path + name_open, sep=';',
                          encoding='windows-1251', header=False, index=False, line_terminator='\r\n')
        print "OPEN SHIFTS", len(open_shift)
        ind_open = True
    except KeyError:
        None
    None


    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ЗАКРЫТИИ СМЕН
    # ===========================
    ind_closed = False
    try:
        closed_shift = closed_shift[['code', 'dateTime', 'documentsQuantity', 'fiscalDocumentNumber',
                                     'fiscalDriveExhaustionSign', 'fiscalDriveMemoryExceededSign',
                                     'fiscalDriveNumber', 'fiscalDriveReplaceRequiredSign', 'fiscalSign', 'kktRegId',
                                     'notTransmittedDocumentsDateTime', 'notTransmittedDocumentsQuantity',
                                     'ofdResponseTimeoutSign', 'operator', 'rawData', 'receiptsQuantity',
                                     'shiftNumber', 'userInn']]
        closed_shift.drop('rawData', axis=1, inplace=True)
        name_closed = 'closedshift_' + str(day) + '_' + str(hour) + str(minute) + '.txt'
        closed_shift.to_csv(file_path + name_closed, sep=';',
                            encoding='windows-1251', header=False, index=False, line_terminator='\r\n')
        print "CLOSED SHIFTS", len(closed_shift)
        ind_closed = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ЧЕКАХ
    # ===========================
    ind_receipt = False
    try:
        receipt = receipt[['receiptCode', 'dateTime', 'fiscalDriveNumber', 'kktRegId', 'fiscalSign', 'shiftNumber',
                           'fiscalDocumentNumber', 'userInn', 'user',  'operator', 'operationType',
                           'requestNumber', 'nds0', 'nds10', 'nds18', 'ecashTotalSum', 'cashTotalSum']]
        receipt['nds0'] /= 100
        receipt['nds10'] /= 100
        receipt['nds18'] /= 100
        receipt['cashTotalSum'] /= 100
        receipt['ecashTotalSum'] /= 100
        receipt['receiptCode'] = receipt['receiptCode'].apply(lambda x: int(x))
        receipt['fiscalSign'] = receipt['fiscalSign'].apply(lambda x: str(x))
        receipt['shiftNumber'] = receipt['shiftNumber'].apply(lambda x: str(x))
        receipt['shiftNumber'] = receipt['shiftNumber'].apply(lambda x: int(x))
        receipt['fiscalDocumentNumber'] = receipt['fiscalDocumentNumber'].apply(lambda x: int(x))
        receipt['operationType'] = receipt['operationType'].apply(lambda x: int(x))
        receipt['requestNumber'] = receipt['requestNumber'].apply(lambda x: int(x))
        receipt.fillna(0, inplace=True)
        name_receipt = 'receipt_' + str(day) + '_' + str(hour) + str(minute) + '.txt'
        receipt.to_csv(file_path + name_receipt, sep=';',
                       encoding='windows-1251', header=False, index=False, line_terminator='\r\n')
        print "RECEIPT", len(receipt)
        ind_receipt = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О ПРОДУКТАХ В ЧЕКАХ
    # ===========================
    ind_items = False
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
        name_items = 'items_' + str(day) + '_' + str(hour) + str(minute) + '.txt'
        items.to_csv(file_path + name_items, sep=';',
                     encoding='windows-1251', header=False, index=False, line_terminator='\r\n')
        print "ITEMS", len(items)
        ind_items = True
    except KeyError:
        None

    # ===========================
    # ПОДГОТОВКА ДАННЫХ О СВОЙСТВАХ ЧЕКА
    # ===========================
    ind_property = False
    try:
        properties = properties[['fiscalDocumentNumber', 'fiscalDriveNumber', 'kktRegId', 'shiftNumber', 'numid',
                                 'key', 'value']]
        name_properties = 'properties_' + str(day) + '_' + str(hour) + str(minute) + '.txt'
        properties.to_csv(file_path + name_properties, sep=';',
                          encoding='windows-1251', header=False, index=False, line_terminator='\r\n')
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
        name_modifiers = 'modifiers_' + str(day) + '_' + str(hour) + str(minute) + '.txt'
        modifiers.to_csv(file_path + name_modifiers, sep=';',
                         encoding='windows-1251', header=False, index=False, line_terminator='\r\n')
        print "MODIFIERS", len(modifiers)
        ind_modifier = True
    except KeyError:
        None

    # ===========================
    # ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
    # ===========================

    # ДОБАВЛЯЕМ ОТКРЫТЫЕ СМЕНЫ
    if db_read_write is True:
        if ind_open:
            print "saving indicator for OPEN SHIFTS"
            data = (datetime.datetime.today(), name_open, 'o', 0)
            cursor_ms.execute("INSERT INTO RU_T_FISCAL_FLAG "
                              "  (datetime, file_name, type_file, indicator) "
                              "  VALUES (%s, %s, %s, %d);", data)
        if ind_closed:
            print "saving indicator for CLOSED SHIFTS"
            data = (datetime.datetime.today(), name_closed, 'c', 0)
            cursor_ms.execute("INSERT INTO RU_T_FISCAL_FLAG "
                              "  (datetime, file_name, type_file, indicator) "
                              "  VALUES (%s, %s, %s, %d);", data)
        if ind_property:
            print "saving indicator for PROPERTIES"
            data = (datetime.datetime.today(), name_properties, 'p', 0)
            cursor_ms.execute("INSERT INTO RU_T_FISCAL_FLAG "
                              "  (datetime, file_name, type_file, indicator) "
                              "  VALUES (%s, %s, %s, %d);", data)
        if ind_receipt:
            print "saving indicator for RECEIPTS"
            data = (datetime.datetime.today(), name_receipt, 'r', 0)
            cursor_ms.execute("INSERT INTO RU_T_FISCAL_FLAG "
                              "  (datetime, file_name, type_file, indicator) "
                              "  VALUES (%s, %s, %s, %d);", data)
        if ind_items:
            print "saving indicator for ITEMS"
            data = (datetime.datetime.today(), name_items, 'i', 0)
            cursor_ms.execute("INSERT INTO RU_T_FISCAL_FLAG "
                              "  (datetime, file_name, type_file, indicator) "
                              "  VALUES (%s, %s, %s, %d);", data)
        if ind_modifier:
            print "saving indicator for MODIFIERS"
            data = (datetime.datetime.today(), name_modifiers, 'm', 0)
            cursor_ms.execute("INSERT INTO RU_T_FISCAL_FLAG "
                              "  (datetime, file_name, type_file, indicator) "
                              "  VALUES (%s, %s, %s, %d);", data)
        print("\nCOMMITMENT")
        conn_ms.commit()
        conn_ms.close()
    print("Program is Finished.")


if __name__ == "__main__":
    main()
