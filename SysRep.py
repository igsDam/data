#from airflow import DAG
#from airflow.operators.python import PythonOperator
#from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import Json
import numpy as np
import re
import csv
import sys
import logging
import json

POSTGRES_CONN = {
   "host":"192.168.0.186",
   "dbname":"AdventureWork",
   "user":"intellipaas",
   "password":"password",
   "port":5432,
}

#***************************************************************************************************************************
# CSVspec
# 
# rpt: <name>
# fmt: ("html","csv","pdf")
# ttl: report title
# ftn: "End of report title"
# margin: <if non csv>
# dbs: <connection>
# sql: <sql>
# columns: ((<field>,<header>,<alignment>,<width>),(<field...)...)
#
#***************************************************************************************************************************
jsn = []
with open("spec.json","r") as f:
     jsn = json.load(f)

print (jsn)
print (jsn["margin"])
print (jsn["cols"][0]["lbl"])

conn = psycopg2.connect(**POSTGRES_CONN)
tbl  = conn.cursor()

tbl.execute("SELECT * FROM tmp_stg.sys_dict")
cols = [desc[0] for desc in tbl.description]
rs = tbl.fetchall()

for r in rs:
    row_dict = dict(zip(cols, r))
#    print(row_dict["code"])
    print (jsn["rpt"]," ", end="")
    print (jsn["fmt"]," ", end="")
    for c in jsn["cols"]:
        print (c["lbl"],": ", row_dict[c["lbl"]]," ", end="")
    print()

tbl.close()
conn.close()
