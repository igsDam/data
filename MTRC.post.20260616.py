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
import logging
import json
import glob
import os

conn_str = {
   "host":"localhost",
   "dbname":"itsm_db",
   "user":"itsm_app",
   "password":"itsm_app",
   "port":5432,
}

#***************************************************************************************************************************
def runSQL(conn, junc,meta):
    for i in meta:
        try:
           print (f"*** {datetime.now()} : {i}") 
#           junc.execute(i)
        except Exception as e:
           print(f"*** Error found in {e}")
           conn.rollback()
           continue

def sysSQL(txt,dft="***"):
    sql = ""
    varSQL = []
    with open(txt, "r") as f:
        for line in f:
            if line.strip().startswith(dft):
               varSQL.append(sql)
               sql = ""
            else:
               sql += line.strip() + " "
    conn = psycopg2.connect(**conn_str)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    runSQL(conn, cur, varSQL)
    conn.commit()
    cur.close()
    conn.close()
#***************************************************************************************************************************
#sysSQL("./post.sql")
#***************************************************************************************************************************
def run():
    sysSQL("https://raw.githubusercontent.com/igsDam/data/main/mtr.post.sql")

with DAG(
    dag_id="MTRC_POST",
    catchup=False,
    tags=["MTRCv1.0", "MTRC"],
) as dag:
    
    runsql = PythonOperator(
        task_id="PostSQL",
        python_callable=run,
    )