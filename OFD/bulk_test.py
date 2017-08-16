#!/usr/local/bin/python2
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import datetime
import pymssql
import validation as vl
import sys
import numpy as np
import os


def main():
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()
    path = '\\ruhqbkp003\share\\1.txt'
    a = 'c:'
    b = r'\\ruhqbkp003'
    c = 'share'
    d = '1.txt'
    print (path)
    print
    ospath = os.path.join(b, c, d)
    print (ospath)

    cursor_ms.execute("BULK INSERT [DataWarehouse].[dbo].[RU_T_FISCAL_OSHIFT] "
                      " FROM %s"
                      " WITH "
                      "  ( "
                      "  FIELDTERMINATOR = ';', "
                      "  ROWTERMINATOR = '\n' "
                      "   )", path)
    conn_ms.commit()
    conn_ms.close()

    print('Program is Finished.')

if __name__ == "__main__":
    main()