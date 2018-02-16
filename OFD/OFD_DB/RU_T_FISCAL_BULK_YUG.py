#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation_yug as vl
import sys
from multiprocessing.pool import ThreadPool
import ast


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
    payload = {'app_client_id': idd, 'login': login, 'password': pwd}
    response = requests.post('https://api.sbis.ru/oauth/service/',
                             json=payload)
                             #data=json.dumps(payload),
                             #headers={'content-type': 'application/json; charset=utf-8'})
    print "\nNew connection is established."
    print response.status_code
    print response.url
    print response.content
    #sid = ast.literal_eval(response.content)['sid']
    cooks = dict(sid=ast.literal_eval(response.content)['sid'])
    print "--------------------------------"
    return response, cooks


def list_checks(cooks, reg_id, storage_id, date_from, date_to, inn='7801330821'):
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
    print response
    None
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


def get_data_from_ofd(id_fn_time, date_is_fixed, date_to, today_x):
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
    global amount_of_kkt
    connection, cooks = connect()  # установка соединения!
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

 #               print " собираю данные из диапазона", date_from, " до ", date_to_x
 #           else:
 #               date_to_x = date_to
 #               print " собираю данные из диапазона", date_from, " до ", date_to
 #           data_t = list_checks(cooks, printer[0], printer[1], date_from, date_to_x)  #запрос в ОФД!
 #           try:
 #               data_t = pd.DataFrame(data_t)
 #           except ValueError:
 #               data_t = pd.DataFrame(data_t, index=[0])
 #           try:
 #               # выбираем последнее найденное значение, если его нет то значит все данные собрали
 #               last_row = data_t.iloc[-1]
 #               if last_row['receipt'] == last_row['receipt']:
 #                   rec = last_row['receipt']
 #               elif last_row['closeShift'] == last_row['closeShift']:
 #                   rec = last_row['closeShift']
 #               elif last_row['openShift'] == last_row['openShift']:
 #                   rec = last_row['openShift']
 #               data_check = 1
 #               date = datetime.datetime.utcfromtimestamp(rec['dateTime'])
 #               if date < today_x:
 #                   data_check = 0
 #                   print "  собраны данные ", date_from, " по ", date
 #                   date_from = (date + datetime.timedelta(minutes=1)).isoformat()
 #               else:
 #                   print "Достигли финальной даты."
 #           except IndexError:
 #               data_check = 1
 #               print "Больше нет данных."
 #           except KeyError:
 #               data_check = 1
 #               print "Больше нет данных."

            print " собираю данные из диапазона1", date_from, " до ", date_to_x
            else:
            date_to_x = date_to
            print " собираю данные из диапазона2", date_from, " до ", date_to
        data_t = list_checks(cooks, printer[0], printer[1], date_from, date_to_x)  # запрос в ОФД!
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
                print "  ==собраны данные== ", date_from, " по ", date
                date_from = (date + datetime.timedelta(minutes=1)).isoformat()
            else:
                print "Достигли финальной даты."
                data_check = 1
        except IndexError:
            # data_check = 1
            print "Данных в диапазоне нет."
            date_from = date_to_x
        except KeyError:
            # data_check = 1
            print "Больше нет данных."
            date_from = date_to_x
        if date_from == date_to:
            print "Достигли финальной даты."
            data_check = 1

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
                # print "ITEMS=="
                # print "  0 start", datetime.datetime.today()
                receipt_tmp = pd.DataFrame(data['receipt'].dropna().tolist())
                # print len(receipt_tmp)
                # print "  1 got receipts", datetime.datetime.today()
                receipt_tmp['dateTime'] = receipt_tmp['dateTime'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                # print "  2 datetime formated", datetime.datetime.today()
                receipt_tmp = nds_check(receipt_tmp)
                # print "  3 nds formated", datetime.datetime.today()
                receipt_tmp.drop('rawData', axis=1, inplace=True)
                # содержимое чека - items
                try:
                    items_tmp = receipt_tmp['items'].dropna().tolist()
                    # print "  4 got items", datetime.datetime.today()
                    # print len(items_tmp)
                    count = 0
                    items_pre = pd.DataFrame()
                    for i in items_tmp:
                        dataf = pd.DataFrame(i)
                        dataf['fiscalDriveNumber'] = receipt_tmp.iloc[count]['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp.iloc[count]['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp.iloc[count]['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp.iloc[count]['fiscalDocumentNumber']
                        dataf['numid'] = dataf.index.values
                        items_pre = pd.concat([items_pre, dataf])
                        count += 1
                    items = pd.concat([items, items_pre])
                    # print "  5 merge receipts data to items", datetime.datetime.today()
                    items = nds_check(items)
                    # print "  6 nds formated", datetime.datetime.today()
                except KeyError:
                    None
                # properties
                try:
                    count = 0
                    properties_tmp = receipt_tmp['properties'].dropna().tolist()
                    # print "  7 gor properties", datetime.datetime.today()
                    properties_pre = pd.DataFrame()
                    for i in properties_tmp:
                        try:
                            dataf = pd.DataFrame(i)
                        except ValueError:
                            dataf = pd.DataFrame(i, index=[0])
                        dataf['fiscalDriveNumber'] = receipt_tmp.iloc[count]['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp.iloc[count]['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp.iloc[count]['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp.iloc[count]['fiscalDocumentNumber']
                        dataf['numid'] = dataf.index.values
                        properties_pre = pd.concat([properties_pre, dataf])
                        count += 1
                    properties = pd.concat([properties, properties_pre])
                    # print "  8 merge receipts data to properties", datetime.datetime.today()
                except KeyError:
                    None
                # modifiers
                try:
                    count = 0
                    modifiers_tmp = receipt_tmp['modifiers'].dropna().tolist()
                    # print "  9 got modifiers", datetime.datetime.today()
                    modifiers_pre = pd.DataFrame()
                    for i in modifiers_tmp:
                        dataf = pd.DataFrame(i)
                        dataf['fiscalDriveNumber'] = receipt_tmp.iloc[count]['fiscalDriveNumber']
                        dataf['kktRegId'] = receipt_tmp.iloc[count]['kktRegId']
                        dataf['shiftNumber'] = receipt_tmp.iloc[count]['shiftNumber']
                        dataf['fiscalDocumentNumber'] = receipt_tmp.iloc[count]['fiscalDocumentNumber']
                        dataf['numid'] = dataf.index.values
                        modifiers_pre = pd.concat([modifiers_pre, dataf])
                        count += 1
                    modifiers = pd.concat([modifiers, modifiers_pre])
                    # print "  10 merge receipts data to modifiers", datetime.datetime.today()
                except KeyError:
                    None
                receipt = pd.concat([receipt, receipt_tmp])
                # print "  11 merge receipts data to receipts", datetime.datetime.today()
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
    return open_shift, closed_shift, receipt, items, properties, modifiers


def main(db_read_write=True,
         reg_id=None, storage_id=None,
         date_from=None, date_to=None,
         file_path='N:\MIS\OFD\\'):
    """
    :type db_read_write: работаем или нет с базой данных
    :type file_path: путь куда сораняем логи
    :type reg_id: регистрационный номер принтера
    :type storage_id: регистрационный номер ФН
    :type date_from: с какой даты мы хотим начать забирать данные формат пример - 2017-06-18T00:00:00
    :type date_to: по какую дату хоти забирать данные
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
    if (reg_id is None or storage_id is None) and db_read_write is True:
        id_fn = reg_drive(cursor_ms)  # списко ФН
    else:
        id_fn = ((reg_id, storage_id),)  # списко ФН
        # id_fn = (('0000546299024021', '8710000100610203'), ('0000909506007263', '8710000100837463'),
        #         ('0000326764018245', '8710000100435361'), ('0000570631019466', '8710000100512301'),
        #         ('0000570300003541', '8710000100512052'), ('0000192250002480', '8710000100082802'))  # списко ФН
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
    # мультипоточность, хотим 10 поток
    # ======================
    # определяем кол-во принтеров
    global amount_of_kkt
    amount_of_kkt = len(id_fn_time)
    threads = 4
    if threads > amount_of_kkt:
        # ======================
        # запрос в ОФД
        # ======================
        open_shift, closed_shift, receipt, items, properties, modifiers = \
            get_data_from_ofd(id_fn_time, date_is_fixed, date_to, today_x)
    else:
        amount_printers = len(id_fn_time)/threads
        pool1 = ThreadPool(processes=1)
        pool2 = ThreadPool(processes=2)
        pool3 = ThreadPool(processes=3)
        pool4 = ThreadPool(processes=4)
        #pool5 = ThreadPool(processes=5)
        #pool6 = ThreadPool(processes=6)
        #pool7 = ThreadPool(processes=7)
        #pool8 = ThreadPool(processes=8)
        #pool9 = ThreadPool(processes=9)
        #pool10 = ThreadPool(processes=10)
        async_result1 = pool1.apply_async(get_data_from_ofd, (id_fn_time[:amount_printers],
                                                              date_is_fixed, date_to, today_x))
        async_result2 = pool2.apply_async(get_data_from_ofd, (id_fn_time[amount_printers:2*amount_printers],
                                                              date_is_fixed, date_to, today_x))
        async_result3 = pool3.apply_async(get_data_from_ofd, (id_fn_time[2*amount_printers:3*amount_printers],
                                                              date_is_fixed, date_to, today_x))
        async_result4 = pool4.apply_async(get_data_from_ofd, (id_fn_time[3*amount_printers:4*amount_printers],
                                                              date_is_fixed, date_to, today_x))
        #async_result5 = pool5.apply_async(get_data_from_ofd, (id_fn_time[4*amount_printers:5*amount_printers],
        #                                                      date_is_fixed, date_to, today_x))
        #async_result6 = pool6.apply_async(get_data_from_ofd, (id_fn_time[5*amount_printers:6*amount_printers],
        #                                                      date_is_fixed, date_to, today_x))
        #async_result7 = pool7.apply_async(get_data_from_ofd, (id_fn_time[6*amount_printers:7*amount_printers],
        #                                                      date_is_fixed, date_to, today_x))
        #async_result8 = pool8.apply_async(get_data_from_ofd, (id_fn_time[7*amount_printers:8*amount_printers],
        #                                                      date_is_fixed, date_to, today_x))
        #async_result9 = pool9.apply_async(get_data_from_ofd, (id_fn_time[8*amount_printers:9*amount_printers],
        #                                                      date_is_fixed, date_to, today_x))
        #async_result10 = pool10.apply_async(get_data_from_ofd, (id_fn_time[9*amount_printers:10*amount_printers],
        #                                                        date_is_fixed, date_to, today_x))
        # get the results
        pool1.close() #'это кажется работает!'
        pool1.join()
        open_shift1, closed_shift1, receipt1, items1, properties1, modifiers1 = async_result1.get()
        pool2.close()
        pool2.join()
        open_shift2, closed_shift2, receipt2, items2, properties2, modifiers2 = async_result2.get()
        pool3.close()
        pool3.join()
        open_shift3, closed_shift3, receipt3, items3, properties3, modifiers3 = async_result3.get()
        pool4.close()
        pool4.join()
        open_shift4, closed_shift4, receipt4, items4, properties4, modifiers4 = async_result4.get()
        #open_shift5, closed_shift5, receipt5, items5, properties5, modifiers5 = async_result5.get()
        #open_shift6, closed_shift6, receipt6, items6, properties6, modifiers6 = async_result6.get()
        #open_shift7, closed_shift7, receipt7, items7, properties7, modifiers7 = async_result7.get()
        #open_shift8, closed_shift8, receipt8, items8, properties8, modifiers8 = async_result8.get()
        #open_shift9, closed_shift9, receipt9, items9, properties9, modifiers9 = async_result9.get()
        #open_shift10, closed_shift10, receipt10, items10, properties10, modifiers10 = async_result10.get()

        open_shift = [open_shift1, open_shift2, open_shift3, open_shift4]
        open_shift = pd.concat(open_shift)
        closed_shift = [closed_shift1, closed_shift2, closed_shift3, closed_shift4]
        closed_shift = pd.concat(closed_shift)
        receipt = [receipt1, receipt2, receipt3, receipt4]
        receipt = pd.concat(receipt)
        items = [items1, items2, items3, items4]
        items = pd.concat(items)
        properties = [properties1, properties2, properties3, properties4]
        properties = pd.concat(properties)
        modifiers = [modifiers1, modifiers2, modifiers3, modifiers4]
        modifiers = pd.concat(modifiers)

        #open_shift = [open_shift1, open_shift2, open_shift3, open_shift4, open_shift5, open_shift6, open_shift7,
        #               open_shift8, open_shift9]
        #open_shift = pd.concat(open_shift)
        #closed_shift = [closed_shift1, closed_shift2, closed_shift3, closed_shift4, closed_shift5, closed_shift6,
        #                closed_shift7, closed_shift8, closed_shift9, closed_shift10]
        #closed_shift = pd.concat(closed_shift)
        #receipt = [receipt1, receipt2, receipt3, receipt4, receipt5, receipt6, receipt7, receipt8, receipt9, receipt10]
        #receipt = pd.concat(receipt)
        #items = [items1, items2, items3, items4, items5, items6, items7, items8, items9, items10]
        #items = pd.concat(items)
        #properties = [properties1, properties2, properties3, properties4, properties5, properties6, properties7,
        #              properties8, properties9, properties10]
        #properties = pd.concat(properties)
        #modifiers = [modifiers1, modifiers2, modifiers3, modifiers4, modifiers5, modifiers6, modifiers7, modifiers8,
        #             modifiers9, modifiers10]
        #modifiers = pd.concat(modifiers)

        if len(id_fn_time)%threads != 0:
            print "\n\n ===\nSTART FINAL THREAD\n=== \n\n"
            pool11 = ThreadPool(processes=11)
            async_result11 = pool11.apply_async(get_data_from_ofd, (id_fn_time[10*amount_printers:],
                                                                   date_is_fixed, date_to, today_x))
            pool11.close()
            pool11.join()
            open_shift11, closed_shift11, receipt11, items11, properties11, modifiers11 = async_result11.get()
            open_shift = pd.concat([open_shift, open_shift11])
            closed_shift = pd.concat([closed_shift, closed_shift11])
            receipt = pd.concat([receipt, receipt11])
            items = pd.concat([items, items11])
            properties = pd.concat([properties, properties11])
            modifiers = pd.concat([modifiers, modifiers11])

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
        print "\nCOMMITMENT"
        conn_ms.commit()
        conn_ms.close()
    print "Program is Finished."


if __name__ == "__main__":
    main()
