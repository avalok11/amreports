#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import datetime
import pymssql
import validation as vl
import xlwt
import mail_send as m


def main(day_frame=2):
    # ===========================
    # ОПРЕДЕЛИТЬ ПОЗАВЧЕРАШНИЙ ДЕНЬ
    # ===========================
    today = datetime.datetime.today()
    day_check = today - datetime.timedelta(days=day_frame)

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL И ПОЛУЧЕНИЕ СПИСКА АКТИВНЫХ ПРИНТЕРОВ
    # ===========================
    # make a connection to MSSQL iBase RU server
    print "connect to MSSQL"
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()

    #cursor_ms.execute("SELECT DISTINCT r.fiscalDriveNumber, r.kktRegId, k.address, m.mpk"
    #                  "  FROM [DataWarehouse].[dbo].[RU_T_FISCAL_RECEIPT] r"
    #                  "  INNER JOIN [DataWarehouse].[dbo].[RU_T_FISCAL_KKT] k"
    #                  "  ON r.kktRegId=k.regId"
    #                  "  INNER JOIN [DataWarehouse].[dbo].[RU_T_FISCAL_DIRVE_MPK] m"
    #                  "  ON k.factoryId=m.factoryId "
    #                  "where nds0 != 0 and (nds10 =0 or nds18 = 0) and dateTime > %s;", day_check)
    print "get list of active KKT from MSSQL"
    cursor_ms.execute("SELECT regId, regId FROM RU_T_FISCAL_KKT WHERE status=2;")
    list_kkt = tuple(cursor_ms.fetchall())

    print "LIST KKT"
    print type(list_kkt)
    print list_kkt
    None

    print "look for KKT with no data for last 2 days"
    cursor_ms.executemany("BEGIN "
                          " IF NOT EXISTS "
                          " (SELECT 1 FROM RU_T_FISCAL_OSHIFT WHERE dateTime > '2017-07-01'"
                          " AND kktRegId=%s) "
                          " BEGIN"
                          "  SELECT k.regId, k.address, k.factoryId, k.model, m.mpk "
                          "  FROM RU_T_FISCAL_KKT k "
                          "  LEFT JOIN RU_T_FISCAL_DIRVE_MPK m "
                          "  ON k.factoryId=m.factoryId "
                          "  WHERE regId=%s "
                          " END "
                          "END;", list_kkt)
    no_oshifts = cursor_ms.fetchall()
    print type(no_oshifts)
    print no_oshifts
    None

    # ====================
    # создаем эксель
    # ====================
    if list_kkt:
        print "save to xls"
        book = xlwt.Workbook(encoding='utf-8')
        sheet1 = book.add_sheet('nds0')
        style1_1 = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                               'align: wrap on, vert centre, horiz center; borders: top thin, right thin, '
                               'left thin, bottom thin;')

        # ЗАГОЛОВОК
        string = 0
        head = ['mpk', 'usr', 'address', 'fiscalDriveNumber', 'kktRegId', 'model']
        for c in range(len(head)):
            sheet1.write(string, c, head[c], style1_1)
        string += 1
        for kkt in list_kkt:
            for col in range(len(kkt)):
                sheet1.write(string, col, kkt[col], style1_1)
            string += 1
        string += 1
        string += 1

        head = ['dateTime', 'fiscalDocumentNumber', 'fiscalDriveNumber', 'kktRegId', 'nds0',
                'nds10', 'nds18', 'operationType', 'name', 'quantity', 'nds0', 'nds10', 'nds18', 'price']
        for c in range(len(head)):
            sheet1.write(string, c, head[c], style1_1)
        string += 1
        for nds in nds0:
            sheet1.write(string, 0, nds[0].strftime('%Y:%m:%d %H:%M:%S'), style1_1)
            None
            for col in range(len(nds)-1):
                sheet1.write(string, col+1, nds[col+1], style1_1)
            string += 1

        book.save("nds0.xls")
        print "send email"
        m.main('nds0.xls', "NDS 0")
    else:
        print "no data to send"
    conn_ms.close()
    print "program is finished"


if __name__ == "__main__":
    main()

