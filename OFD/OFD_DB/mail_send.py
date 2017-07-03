#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import pymysql

# Import smtplib for the actual sending function
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import validation


def main(text, header_email, file_path, report_name, no_senders=True):
    # build connection to database
    if not no_senders:
        conn_my = pymysql.connect(host=validation.ip_mysql, user=validation.usr_my, password=validation.pwd_my,
                                  db=validation.db_my, charset='utf8')
        cursor_my = conn_my.cursor()

        # get email addresses for restaurants
        cursor_my.execute('Select unt_email from units where unt_brand=15 and unt_active = 1')
        email_list = list(cursor_my.fetchall())
        # get ACs emails
        cursor_my.execute('Select reg_ac_email from regions where reg_id_brand=15')
        email_list.extend(list(cursor_my.fetchall()))
        # get districts email addresses
        cursor_my.execute('Select dis_dc_email from districts where dis_id_brand = 15')
        email_list.extend(list(cursor_my.fetchall()))

        # close database connection
        conn_my.close()

        # rcpts list with domain names
        rcpts = list()
        for r in email_list:
            for l in r:
                l = str(l).strip()
                if '4' in l:
                    l += "@amrest.eu"
    #           print l
                rcpts.append(l)
    rcpts = ['aleksey.yarkov@amrest.eu']
    rcpts.append('Stepan.Kuznetcov@amrest.eu')
    rcpts.append('Andrey.Tihonov@amrest.eu')


    # sending email notification
    # me == email from
    me = "AmReports@amrest.eu"
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = report_name
    msg['From'] = me
    msg['To'] = ", ".join(rcpts)

    # Create the body of the message (a plain-text and an HTML version).
    #text = '<p>Во вложении файл с перечисленными Фискальными Принтерами, с некорректными настройками НДС.' \
    #       '<p>А также пример чеков с данных принтеров.<br><br>' \
    #       '<p>ЭТО АВТОМАТИЧЕСКАЯ РАССЫЛКА, ПРОСЬБА НЕ ОТВЕЧАТЬ НА ДАННОЕ ПИСЬМО.'

    # Record the MIME types of both parts - text/plain and text/html.
    msg.attach(MIMEText(text, 'html'))


    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(file_path, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', header_email)

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part)

    # Send the message via local SMTP server.
    s = smtplib.SMTP('192.168.3.7')

    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    s.sendmail(me, rcpts, msg.as_string())
    s.quit()

    print "Finished"

if __name__ == "__main__":
    main()
