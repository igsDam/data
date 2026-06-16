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

varCSS = """
<style>
  body {margin: 12px;font-family: Arial, sans-serif;}
  h2 {text-align: center;}
  footer {margin-top: 20px;padding: 10px;text-align: center;}
  th {background-color: steelblue;color: white;padding: 4px;text-align: left;}
  #list tr:nth-child(odd) {background-color: white;}
  #list tr:nth-child(even) {background-color: lightgray;}
</style>
"""
#***************************************************************************************************************************
def genRep(spec,rpt,ref,row):
    if ref == "header":
       if spec.get("fmt") == "html":
          rpt.write(varCSS)
          rpt.write("<body>")
          rpt.write(f"<h2>{spec.get("title")}</h2><label>ID: {spec.get("rpt")}</label>")
          rpt.write(f"<table id=\"list\"><thead>\n")
          for idx, data in row.items():
              col_spec = next((c for c in spec["columns"] if c["field"] == idx), None)
              header = col_spec.get("header")
              align = col_spec.get("alignment", None)
              width = col_spec.get("width", None)
              width = f"{width}px" if width is not None else None
              rpt.write(f"<th style=\"width:{width}; text-align:{align};\">{header}</th>")
          rpt.write("</thead>\n")
       else:
          csvRpt = csv.writer(rpt)
          csvRpt.writerow([f"ID: {spec.get('rpt')}"])
          header = []
          for idx, data in row.items(): header.append(data)
          csvRpt.writerow(header)
    if ref == "line":
       rs = {k: v for d in row for k, v in d.items()}
       if spec.get("fmt") == "html":
          rpt.write(f"<tr>")
          for r in row:
              for idx, data in r.items():
                  col_spec = next((c for c in spec["columns"] if c["field"] == idx), None)
                  align = col_spec.get("alignment", None)
                  width = col_spec.get("width", None)
                  width = f"{width}px" if width is not None else None
                  max = col_spec.get("max", None)
                  max = f"{max}px" if max is not None else None
                  func = col_spec.get("func", None)
                  style = f"width:{width};max-width:{max}; text-align:{align};"
                  val = data
                  if func and func.startswith("cmp."): 
                     col1, col2 = func.replace("cmp.","").strip("()").split(",")
                     val1 = rs.get(col1)
                     val2 = rs.get(col2)
                     is_equal = (val1 == val2)
                     if not is_equal: style += "color:red;"
                     val = is_equal
                  rpt.write(f"<td style=\"{style};\">{val}</td>")
          rpt.write(f"</tr>\n")
       else:
          csvRpt = csv.writer(rpt)
          line = []
#          for r in row: line.append(list(r.values())[0])
          for r in row:
              for idx, data in r.items():
                  col_spec = next((c for c in spec["columns"] if c["field"] == idx), None)
                  func = col_spec.get("func", None)
                  if func and func.startswith("cmp."):
                     col1, col2 = func.replace("cmp.","").strip("()").split(",")
                     line.append(rs.get(col1) == rs.get(col2))
                  else:
                     line.append(data)
          csvRpt.writerow(line)
    if ref == "end":
       if spec.get("fmt") == "html":
          rpt.write(f"</table>\n")
          rpt.write((f"<footer>{spec.get('footer')}</footer></body>\n").replace("{rec}", str(row.get("rec"))))          
       else:
          csvRpt = csv.writer(rpt)
          csvRpt.writerow([f"<footer>{spec.get('footer')}</footer></body>\n".replace("{rec}", str(row.get("rec")))])
       print (row.get("rec"),end=": ")

def sysRep(rep):
    with open(rep,"r") as f:
         spec = json.load(f)

    rpt = open(f"{spec.get("rpt")}.{datetime.now().strftime("%Y%m%d_%H%M%S")}.{spec.get("fmt")}","w", newline="", encoding="utf-8")
    conn = psycopg2.connect(**conn_str)
    tbl = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    tbl.execute(spec["sql"])
    rs = tbl.fetchall()
    if spec.get("columns"):
       varCols = {col["field"]: col["header"] for col in spec["columns"]}
    else:
       varCols = {col: col for col in [desc[0] for desc in tbl.description]}
    genRep(spec,rpt,"header",varCols)

    rec = 0
    for r in rs:
         rec += 1
         row = []
         for field in varCols.keys(): row.append({field: r.get(field)})
         genRep(spec,rpt,"line",row)
    varVal = {"rec": rec}
    genRep(spec,rpt,"end",varVal)

    f.close()
    tbl.close()
    conn.close()
    print(f" Process report {spec["rpt"]} complete by {datetime.now()}\n\n")
#***************************************************************************************************************************
#sysRep("./mtr.rej01.json")
#sysRep("./mtr.inc01.json")
#***************************************************************************************************************************
def run():
    sysRep("https://raw.githubusercontent.com/igsDam/data/main/mtr.rej01.json")
    sysRep("https://raw.githubusercontent.com/igsDam/data/main/mtr.inc01.json")

with DAG(
    dag_id="MTRC_LIST",
    catchup=False,
    tags=["MTRCv1.0", "MTRC"],
) as dag:
    
    genrep = PythonOperator(
        task_id="ValidationReport",
        python_callable=run,
    )