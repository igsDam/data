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

conn_str = {
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
with open("spec.json","r") as f:
    jsn = json.load(f)

conn = psycopg2.connect(**conn_str)
#tbl = conn.cursor()
tbl = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
tbl.execute(jsn["sql"])
if jsn.get("columns"):
   spec = {col["field"]: col["header"] for col in jsn["columns"]}
   spec1 = {col["field"]: col["header"] for col in jsn["columns"]}
else:
   varCols = [desc[0] for desc in tbl.description]
   spec = {col: col for col in varCols}
print (spec, spec.get("header"))

f = open(f"{jsn.get("rpt")}.{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv","a", newline="", encoding="utf-8")
rpt = csv.writer(f)
rpt.writerow(list(spec.values()))

sys.exit()

rs = tbl.fetchall()

for r in rs:
    rec = [r[c] for c in spec.keys()]
    for c in spec.keys():
        rec.append(r[c])
#    for a in rec: print (a,end=",")
#    for a in spec: print (jsn.get("columns").get(a).get("width"),end=",")
#    print (rec)
    rpt.writerow(rec)


tbl.close()
conn.close()