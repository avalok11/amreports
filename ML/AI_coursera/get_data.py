#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import pymssql
import validation as vl


def main(datas=20170901, twice=0, amount=100):
    # connect to DB
    conn_ms = pymssql.connect(host=vl.ip_mssql, user=vl.usr_ms, password=vl.pwd_ms,
                              database=vl.db_ms, charset='utf8')
    cursor_ms = conn_ms.cursor()

    # read the data SEPARATE PRODUCT
    cursor_ms.execute("select top %s * from ru_mix_4 where unit between 403000 and 403999 and "
                      "datas > %s and pkid2=0;", (amount, datas))
    data = pd.DataFrame(cursor_ms.fetchall())

    # read the data PRODUCT from COMBO
    cursor_ms.execute("select top %s * from ru_mix_4 where unit between 403000 and 403999 and "
                      "datas > %s and pkid2>0;", (amount, datas))
    x_data = pd.concat([data, pd.DataFrame(cursor_ms.fetchall())])
    print(x_data.shape)
    conn_ms.close()
    y_data = x_data[4].apply(lambda x: 1 if x > 0 else 0)
    print(y_data.shape)

    if twice != 0:
        for i in range(twice):
            x_data = pd.concat([x_data, x_data], axis=1)
    print("new X data set: ", x_data.shape)

    return x_data, y_data


if __name__ == "__main__":
    main()
