#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl

a = [('0001580632035623', '8710000100537947', '2017-05-31T14:33:00', 0, u'ФН-1',
      '0001580632035623', 2, '8710000100537947')]

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
УДАЛЯЕМ ВСЕ И ПОТОМ ВСТАВЛЯЕМ
cursor_ms.executemany("BEGIN "
                      "  IF NOT EXISTS "
                      "    (SELECT 1 FROM RU_T_FISCAL_DRIVE WHERE regId=%s and storageId=%s)"
                      "  BEGIN "
                      "    INSERT INTO RU_T_FISCAL_DRIVE (effectiveFrom, effectiveTo, model, regId, status, storageId) "
                      "    VALUES (%s, %s, %s, %s, %s, %s)"
                      "  END "
                      "END", a)

conn_ms.commit()
conn_ms.close()

