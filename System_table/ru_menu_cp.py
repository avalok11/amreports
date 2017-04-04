#!/usr/local/bin/python2
# -*- coding: utf-8 -*-
"""
complete script for transfer data from RU_MIX_4 table to FIN table aggregated by day
"""

import datetime
import pymssql
import pymysql
import validation


def main():
    # make a connection to MSSQL iBase PL server
    # making FULL COPY OF CENTRAL DABASE RU_MIX_4
    conn_ms = pymssql.connect(server=validation.ip_mssql, user=validation.usr_ms, password=validation.pwd_ms,
                              database=validation.db_ms, charset='UTF-8')
    cursor_ms = conn_ms.cursor()

    # make a connection to MySQL ru_bi RU server
    conn_my = pymysql.connect(host=validation.ip_mysql, user=validation.usr_my, password=validation.pwd_my,
                              db=validation.db_my, charset='utf8')
    cursor_my = conn_my.cursor()

    # fetch the data from PL database
    step1 = str("1. Start read data from MSSQL:iBase:ru_menu database. Start time " + str(datetime.datetime.now()))
    print step1

    cursor_ms.execute(
        'SELECT * FROM [iBase].[dbo].[ru_menu];')
    row_ms = list(cursor_ms.fetchall())

    step2 = str("2. Copying data to MySQL:ru_aop:ru_menu. Start time: " + str(datetime.datetime.now()))
    print step2

    cursor_my.executemany('REPLACE INTO ru_menu (id, stv_id, name, ups_name, ups_code) '
                          'VALUES (%s, %s, %s, %s, %s);', row_ms)
    step3 = str("3. Finished download the data MySQL. Finished time: " + str(datetime.datetime.now()))
    print step3

    conn_my.commit()
    conn_ms.close()
    conn_my.close()

    print("   Program is finished.")


if __name__ == "__main__":
    main()
