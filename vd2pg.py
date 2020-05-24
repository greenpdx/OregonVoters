#!/usr/bin/env python
from __future__ import print_function
import os
import csv
import psycopg2 as pg2

conn = pg2.connect(dbname="orvoter", user="ubuntu", password="ubuntu", host="10.0.42.185")
cur = conn.cursor()
#cur.execute("TRUNCATE TABLE img_loc CASCADE")
#cur.execute("ALTER SEQUENCE img_loc_idximg_seq RESTART WITH 1")
#conn.commit()

fnames = (
    'VOTER_ID',
    'FIRST_NAME','MIDDLE_NAME','LAST_NAME','NAME_SUFFIX','BIRTH_DATE','SSN',
    'EFF_REGN_DATE','STATUS','PARTY_CODE',
    'PHONE_NUM','UNLISTED',
    'COUNTY','RES_ADDRESS_1','RES_ADDRESS_2','HOUSE_NUM','HOUSE_SUFFIX','PRE_DIRECTION','STREET_NAME','STREET_TYPE','POST_DIRECTION','UNIT_TYPE','UNIT_NUM','ADDR_NON_STD',
    'CITY','STATE','ZIP_CODE','ZIP_PLUS_FOUR',
    'EFF_ADDRESS_1','EFF_ADDRESS_2','EFF_ADDRESS_3','EFF_ADDRESS_4','EFF_CITY','EFF_STATE','EFF_ZIP_CODE','EFF_ZIP_PLUS_FOUR',
    'ABSENTEE_TYPE','PRECINCT_NAME','PRECINCT','SPLIT', 'X')

parties = (
    {"DEM": "Democrat"},
    {"REP": "Republican"},
    {"CON": "Constitution"},
    {"IND": "Independent Party"},
    {"LBT": "Libertarian"},
    {"NAV": "Nonaffiliated"},
    {"OTH": "Other"},
    {"PGP": "Pacific Green"},
    {"PRO": "Progressive"},
    {"WFP": "Working Families Party of Oregon"})



def rdfile(cd):
    cnt = 0
    with open(cd, newline='') as csvfile:
        votereader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        fields = votereader.__next__()
        print(fields)
        cur = conn.cursor()
        sql = "%s"
        for i in range(39):
            sql = sql + ",%s"
        sql = "INSERT INTO rawvd VALUES (" + sql + ")"
        for row in votereader:
                 #print(sql,"\n",row)
            rtn = cur.execute(sql, row)
            if (cnt > 1000):
                conn.commit()
                cnt = 0
                print(".",end='')
            cnt = cnt + 1
        conn.commit()



def mkrawtab():
    sql = "CREATE TABLE rawvd ("
    for name in fnames:
        f = name + " VARCHAR,"
        sql = sql + f
    sql = sql[:-1] + ")"
    cur = conn.cursor()
    rtn = cur.execute(sql)
    conn.commit()
    print(rtn)

#mkrawtab()

rdfile('CD5-03.csv')

#conn.close()
