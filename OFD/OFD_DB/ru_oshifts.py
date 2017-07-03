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
    day_check = (today - datetime.timedelta(days=day_frame)).strftime("%Y-%m-%d")
    print day_check

    # ===========================
    # ПОДКЛЮЧНИЕ БАЗЫ PL И ПОЛУЧЕНИЕ СПИСКА АКТИВНЫХ ПРИНТЕРОВ
    # ===========================
    # make a connection to MSSQL iBase RU server
    print "connect to MSSQL"
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()

    print "get list of active KKT from MSSQL"
    cursor_ms.execute("SELECT regId, regId FROM RU_T_FISCAL_KKT WHERE status=2;")
    list_kkt = tuple(cursor_ms.fetchall())
    list_kkt = tuple((day_check, x, y) for x, y in list_kkt)

    print "LIST KKT"
    print type(list_kkt)
    print list_kkt
    None
    no_oshifts = list()

    print "look for KKT with no data for last ", day_frame, "days"

    for k in list_kkt:
        print k
        try:
            cursor_ms.execute("BEGIN "
                              " IF NOT EXISTS "
                              " (SELECT 1 FROM RU_T_FISCAL_OSHIFT WHERE dateTime > %s"
                              " AND kktRegId=%s) "
                              " BEGIN"
                              "  SELECT k.regId, k.address, k.factoryId, k.model, m.mpk "
                              "  FROM RU_T_FISCAL_KKT k "
                              "  LEFT JOIN RU_T_FISCAL_DIRVE_MPK m "
                              "  ON k.factoryId=m.factoryId "
                              "  WHERE regId=%s "
                              " END "
                              "END;", k)
            res = cursor_ms.fetchone()
            print "RESULT:", res
            no_oshifts.append(res)
            None
        except pymssql.OperationalError:
            None
    print type(no_oshifts)
    print no_oshifts
    None

    # ====================
    # создаем эксель
    # ====================
    if no_oshifts:
        print "save to xls"
        book = xlwt.Workbook(encoding='utf-8')
        sheet1 = book.add_sheet('no data')
        col_width = 256*20
        sheet1.col(0).width = col_width
        sheet1.col(1).width = col_width
        sheet1.col(2).width = col_width
        sheet1.col(3).width = col_width
        style1_1 = xlwt.easyxf('font: name Times New Roman, bold off, height 200; '
                               'align: wrap on, vert centre, horiz center; borders: top thin, right thin, '
                               'left thin, bottom thin;')

        # ЗАГОЛОВОК
        string = 0
        sheet1.write(string, 0, 'For the list of KKT no data. From date:', style1_1)
        sheet1.write(string, 1, day_check, style1_1)
        string += 1
        head = ['regId', 'address', 'factoryId', 'model', 'mpk']
        for c in range(len(head)):
            sheet1.write(string, c, head[c], style1_1)
        string += 1
        for shft in no_oshifts:
            for col in range(len(shft)):
                sheet1.write(string, col, shft[col], style1_1)
            string += 1

        book.save("noshifts.xls")
        print "send email"
        text = '<p>Во вложении список принтеров от которых нет данных в ОФД более чем 2 суток.' \
               '<p><br><br>' \
               '<p>ЭТО АВТОМАТИЧЕСКАЯ РАССЫЛКА, ПРОСЬБА НЕ ОТВЕЧАТЬ НА ДАННОЕ ПИСЬМО.'
        header_email = 'attachment; filename="NoData.xls"'
        m.main(file_path='C:\Users\\aleksey.yarkov\PycharmProjects\\amreports\OFD\OFD_DB\\noshifts.xls',
               report_name="KKT Does Not Send Data", text=text, header_email=header_email)
    else:
        print "no data to send"
    conn_ms.close()
    print "program is finished"


if __name__ == "__main__":
    main()

