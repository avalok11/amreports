#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import datetime
import pymssql
import validation as vl
import xlwt
import mail_send as m


def main(day_frame=1):
    # ===========================
    # ОПРЕДЕЛИТЬ ВЧЕРАШНИЙ ДЕНЬ
    # ===========================
    today = datetime.datetime.today()
    day_check = today - datetime.timedelta(days=day_frame)

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL
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
    print "get data of list KKT from MSSQL"
    cursor_ms.execute("SELECT DISTINCT m.mpk, r.usr, k.address, r.fiscalDriveNumber, r.kktRegId, k.model"
                      "  FROM [DataWarehouse].[dbo].[RU_T_FISCAL_RECEIPT] r"
                      "  LEFT JOIN [DataWarehouse].[dbo].[RU_T_FISCAL_KKT] k"
                      "  ON r.kktRegId=k.regId"
                      "  LEFT JOIN [DataWarehouse].[dbo].[RU_T_FISCAL_DIRVE_MPK] m"
                      "  ON k.factoryId=m.factoryId"
                      " where r.nds0 != 0 and (r.nds10 =0 or r.nds18 = 0) and r.operationType=1 "
                      " and dateTime > %s;", day_check)
    list_kkt = cursor_ms.fetchall()

    print "get data of severals checks with wrong NDS"
    cursor_ms.execute("select TOP 10 r.dateTime, r.fiscalDocumentNumber, r.fiscalDriveNumber, r.kktRegId, r.nds0, "
                      "r.nds10, r.nds18, r.operationType, i.name, i.quantity, i.nds0, i.nds10, i.nds18, i.price "
                      "   from [DataWarehouse].[dbo].[RU_T_FISCAL_RECEIPT] r"
                      "   inner join [DataWarehouse].[dbo].[RU_T_FISCAL_ITEMS] i"
                      "   on r.fiscalDocumentNumber=i.fiscalDocumentNumber and"
                      "   r.fiscalDriveNumber = i.fiscalDriveNumber and"
                      "   r.shiftNumber=i.shiftNumber"
                      " where dateTime>%s AND r.nds0!=0 AND r.operationType=1 order by i.numid;"
                      , day_check)
    nds0 = cursor_ms.fetchall()

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
            sheet1.write(string, 0, nds[0].strftime('%Y-%m-%d %H:%M:%S'), style1_1)
            None
            for col in range(len(nds)-1):
                sheet1.write(string, col+1, nds[col+1], style1_1)
            string += 1

        book.save("nds0.xls")
        print "send email"
        text = '<p>Во вложении файл с перечисленными Фискальными Принтерами, с некорректными настройками НДС.' \
               '<p>А также пример чеков с данных принтеров.<br><br>' \
               '<p>ЭТО АВТОМАТИЧЕСКАЯ РАССЫЛКА, ПРОСЬБА НЕ ОТВЕЧАТЬ НА ДАННОЕ ПИСЬМО.'
        header_email = 'attachment; filename="nds0.xls"'
        # m.main(file_path='nds0.xls', report_name="NDS 0", text=text, header_email=header_email)
    else:
        print "no data to send"
    conn_ms.close()
    print "program is finished"


if __name__ == "__main__":
    main()

