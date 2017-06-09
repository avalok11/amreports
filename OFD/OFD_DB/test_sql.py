#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl

a = [(u'8710000100612901', '0000612404012879', '2017-06-05T15:09:00', u'\u0424\u041d-1', u'4', u'8710000100612901', None, '0000612404012879'),
     (u'8710000100609863', '0000612219058428', '2017-06-05T15:09:00', u'\u0424\u041d-1', u'4', u'8710000100609863', None, '0000612219058428')]

# ===========================
# ПОДКЛЮЧНИЕ БАЗЫ PL И ЗАГРУЗКА ПОЛУЧЕННОЙ ИНФОРМАЦИИ ИЗ ОФД В ДБ
# ===========================
# make a connection to MSSQL iBase RU server
conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                          database=vl.db_ms, charset='utf8')
cursor_ms = conn_ms.cursor()
# ===========================
# ОБНОВЛЕНИЕ ДАННЫХ В БАЗЕ
# ===========================
print a
#УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
cursor_ms.execute('TRUNCATE TABLE RU_T_FISCAL_DRIVE;')

cursor_ms.executemany("BEGIN "
                      "  IF NOT EXISTS "
                      "    (SELECT 1 FROM RU_T_FISCAL_DRIVE WHERE regId=%s and storageId=%s)"
                      "  BEGIN "
                      "    INSERT INTO RU_T_FISCAL_DRIVE (effectiveFrom, model, status, storageId, effectiveTo, regId) "
                      "    VALUES (%s, %s, %s, %s, %s, %s)"
                      "  END "
                      "END", a)

conn_ms.commit()
conn_ms.close()

