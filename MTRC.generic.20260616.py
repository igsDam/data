from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import Json
import numpy as np
import re
import csv
import sys


conn_str = {
   "host":"localhost",
   "dbname":"itsm_db",
   "user":"itsm_app",
   "password":"itsm_app",
   "port":5432,
}
'''
conn_str = {
   "host":"postgres.magic-creative-dev.svc.cluster.local",
   "dbname":"mcdesk",
   "user":"mcdesk",
   "password":"MagicCreative@2025!",
   "port":5432,
}
'''
varGeneric=("sid,type,code,val,class,form,"
            "created_by,creation_time")

#***Common def**************************************************************************************************************
def syswrite(tbl, junc, spec, val, jsn_data):
    if jsn_data is not None and tbl != "csv":
       spec = spec +",custom_data"
    varSpec = dict(val)
    varCols = tuple(spec.split(","))

    varVal = []
    for col in varCols:
        if col == "custom_data":
            varVal.append(Json(jsn_data))
        else:
            varVal.append(varSpec.get(col, None))
    varVal = tuple(varVal)

    if tbl == "csv":
       tbl.writerow(varVal)
    else:
       cols = ", ".join(varCols)
       fill = ", ".join(["%s"] * len(varCols))
       sql = f"INSERT INTO {tbl} ({cols}) VALUES ({fill})"
       try: junc.execute(sql, varVal)
       except Exception as e:
            print("Execution failed:", e)
            print(spec)
            print("Values:", varVal)
            sys.exit(1)
#***************************************************************************************************************************
def copy(src, spec, tbl, dft="None"):
    conn = psycopg2.connect(**conn_str)
    dbr = conn.cursor()
    varCols = tuple(spec.split(","))

    if src.startswith("http"):
       rsCSV = pd.read_csv(src).to_dict(orient="records")
    else: 
       f = open(src, newline="", encoding="utf-8") 
       rsCSV = list(csv.DictReader(f))

    if dft=="trunc":
       dbr.execute(f"truncate table {tbl};")

    i = 0
    for r in rsCSV:
        i += 1
        rs = []
        for c in varCols:
            rs.append((c, r.get(c)))
#             rs.append(("sid",               i))
#             rs.append(("type",              r["type"]))
#             rs.append(("code",              r["code"]))
#             rs.append(("val",               r["val"]))
        syswrite(tbl, dbr, varGeneric, rs, None)

    if not src.startswith("http"): f.close()
    conn.commit()
    dbr.close()
    conn.close()
    print(f"*** {src} * {i} rec written to {tbl} \n\n")          
#***************************************************************************************************************************
#copy("https://raw.githubusercontent.com/igsDam/data/main/generic_code.csv", varGeneric, "tmp_stg.sg_generic", "ignore")
#***************************************************************************************************************************
def upload(**kwargs):
    copy("https://raw.githubusercontent.com/igsDam/data/main/generic_code.csv", varGeneric, "tmp_stg.sg_generic", "ignore")

with DAG(
    dag_id="MTRC_REFTBL",
    catchup=False,
    tags=["MTRCv1.0", "MTRC"],
) as dag:

    runcpy = PythonOperator(
        task_id="runcpy",
        python_callable=upload,
    )
