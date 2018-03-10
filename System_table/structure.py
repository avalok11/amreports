#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import pymysql
import pymssql
from datetime import datetime
# Import smtplib for the actual sending function
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import validation as vl


def main():
    # make a connection to MSSQL iBase PL server
    conn_ms = pymssql.connect(server=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms, database=vl.db_ms,
                              charset='UTF-8')
    cursor_ms = conn_ms.cursor()

    # make a connection to MySQL ru_bi RU server
    conn_my = pymysql.connect(host=vl.ip_mysql, user=vl.usr_my, password=vl.pwd_my, db=vl.db_my,
                              charset='utf8')
    cursor_my = conn_my.cursor()

    today = datetime.today()
    month = today.month
    year = today.year

    # fetch the data from PL database
    step1 = str("1. DISTRICTS. Start time " + str(datetime.now()))
    print step1

    cursor_ms.execute('SELECT d.[name] as id_dis, [d].[name] as dis_name, dc.[name] as dis_dc_name, '
                      'dc.email as dis_dc_email, d.id as dis_id '
                      '     FROM [DataWarehouse].[dbo].[MD_XX_RES_DISTRICTS] d '
                      '     INNER JOIN [DataWarehouse].[dbo].[MD_XX_RES_DISTRICTS_DC] dc ON d.id=dc.id '
                      '     WHERE dc.month=%s AND dc.year=%s;', (month, year))

    row_ms_districts = list(cursor_ms.fetchall())

    step2 = str("2. REGIONS. Start time " + str(datetime.now()))
    print step2

    cursor_ms.execute('SELECT r.id as id_reg, r.[name] as reg_name, rc.[name] as reg_ac_name, '
                      'rc.email as reg_ac_email'
                      '     FROM [DataWarehouse].[dbo].[MD_XX_RES_REGIONS] r '
                      '     INNER JOIN [DataWarehouse].[dbo].[MD_XX_RES_REGIONS_AC] rc ON r.id=rc.id '
                      '     WHERE rc.month=%s and rc.year=%s;', (month, year))

    row_ms_regions = list(cursor_ms.fetchall())

    step3 = str("3. PACESETTER. Start time " + str(datetime.now()))
    print step3

    cursor_ms.execute('SELECT r.mpk, p.[name] as grp '
                      '     FROM [DataWarehouse].[dbo].[MD_XX_RESTAURANTS] r '
                      '     INNER JOIN [DataWarehouse].[dbo].[MD_XX_RES_PACESETTER] p ON r.[pacesetter]=p.id '
                      '     WHERE r.month=%s AND r.year=%s AND r.mpk BETWEEN 403001 AND 404999', (month, year))

    row_ms_pacesetter = list(cursor_ms.fetchall())

    step4 = str("4. UNITS. Start time " + str(datetime.now()))
    print step4

    cursor_ms.execute('SELECT r.mpk as id_sap, c.[name] as unt_intranet_path, r.name1 as unt_name, '
                      'r.region as unt_id_reg, r.district as id_d, r.status as unt_active '
                      '     FROM [DataWarehouse].[dbo].[MD_XX_RESTAURANTS] r '
                      '     INNER JOIN [DataWarehouse].[dbo].[MD_XX_RES_CONCEPT] c ON r.[concept] = c.id '
                      '     WHERE r.month=%s AND r.year=%s AND r.mpk BETWEEN 403001 AND 404999 AND r.status=1',
                      (month, year))

    row_ms_unit = list(cursor_ms.fetchall())

    step5 = str("5. Copying data to MySQL. Start time: " + str(datetime.now()))
    print step5

    cursor_my.execute('SET FOREIGN_KEY_CHECKS = 0;')
    cursor_my.execute('TRUNCATE TABLE districts;')
    cursor_my.execute('TRUNCATE TABLE regions;')
    cursor_my.execute('TRUNCATE TABLE units;')
    cursor_my.execute('TRUNCATE TABLE unt_group_tmp;')
    cursor_my.execute('SET FOREIGN_KEY_CHECKS = 1;')

    cursor_my.executemany('INSERT IGNORE INTO districts '
                          '(id_dis, dis_name, dis_dc_name, dis_dc_email, dis_id) '
                          'VALUES (%s, %s, %s, %s, %s);', row_ms_districts)
    cursor_my.executemany('INSERT IGNORE INTO regions '
                          '(id_reg, reg_name, reg_ac_name, reg_ac_email) '
                          'VALUES (%s, %s, %s, %s);', row_ms_regions)
    cursor_my.executemany('INSERT IGNORE INTO unt_group_tmp '
                          '(mpk, grp) '
                          'VALUES (%s, %s);', row_ms_pacesetter)
    cursor_my.executemany('INSERT IGNORE INTO units '
                          '(id_sap, unt_intranet_path, unt_name, unt_id_reg, id_d, unt_active) '
                          'VALUES (%s, %s, %s, %s, %s, %s);', row_ms_unit)

    #step3 = str("3. Finished download the data MySQL. Finished time: " + str(datetime.datetime.now()))
    #print step3

    conn_my.commit()
    conn_ms.close()
    conn_my.close()

    # sending email notification
    # me == email from
    # you == email to
    #me = "OLAP@amrest.eu"
    #you = "aleksey.yarkov@amrest.eu"

    # Create message container - the correct MIME type is multipart/alternative.
    #msg = MIMEMultipart('alternative')
    #msg['Subject'] = 'iBase:districts -> ru_aop:districts'
    #msg['From'] = me
    #msg['To'] = you

    # Create the body of the message (a plain-text and an HTML version).
    #text = "Hello!\n\nData transfer is finished.\n\n" + str(step1) + "\n\n" + str(step2) + "\n\n" + str(step3) + \
    #       "\n\n\n\n\n\n..big brother is looking after you.."

    # Record the MIME types of both parts - text/plain and text/html.
    #part = MIMEText(text, 'plain')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    #msg.attach(part)

    # Send the message via local SMTP server.
    #s = smtplib.SMTP('192.168.3.7')

    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    #s.sendmail(me, you, msg.as_string())
    #s.quit()

    print("   Program is finished.")


if __name__ == "__main__":
    main()
