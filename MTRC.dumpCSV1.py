#from airflow import DAG
#from airflow.operators.python import PythonOperator
#from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
#import pandas as pd
#import psycopg2
#from psycopg2.extras import Json
#import numpy as np
#import re
import csv
import sys

#url = "https://mtr-dev-restapi.onbmc.com"
hdr = {
    "X-Requested-BY":"X",
    "Content-Type": "application/x-www-form-urlencoded"
}
varJSN = ("*")
#***Common def**************************************************************************************************************
def login():
    rsp = requests.post(url+"/api/jwt/login", headers=hdr, data=pwd)
    return {"token": rsp.text}
#*************************************************************************************
def empty(tbl):
    with open(tbl, "w", newline="", encoding="utf-8") as f:
      writer = csv.writer(f)
#*************************************************************************************
def sysval(typ, val):
  try:
    if typ == "form":
       if val == "HPD":      varRtn = "HPD:Help Desk"
       elif val == "PBM":    varRtn = "PBM:Problem Investigation"
       elif val == "PBE":    varRtn = "PBM:Known Error"
       elif val == "CHG":    varRtn = "CHG:Infrastructure Change"
       elif val == "WOI":    varRtn = "WOI:WorkOrder"
       elif val == "HPD_WL": varRtn = "HPD:WorkLog"
       elif val == "PBM_WL": varRtn = "PBM:Investigation WorkLog"
       elif val == "PBE_WL": varRtn = "PBM:Known Error WorkLog"
       elif val == "CHG_WL": varRtn = "CHG:WorkLog"
       elif val == "WOI_WL": varRtn = "WOI:WorkOrder WorkLog"
       elif val == "CTM":    varRtn = "CTM:People"
       elif val == "HPD_AS": varRtn = "HPD:Associations"
       elif val == "PBM_AS": varRtn = "PBM:Investigation Associations"
       elif val == "CHG_AS": varRtn = "CHG:Associations"
       elif val == "AST_BS": varRtn = "BMC.CORE:BMC_BusinessService"
       elif val == "AST_CS": varRtn = "BMC.CORE:BMC_ComputerSystem"
       elif val == "PCT_PC": varRtn = "PCT:ProductCatalogJoin"
       elif val == "CTM_SG": varRtn = "CTM:Support Group"
       elif val == "CFG_AS": varRtn = "CFG:Assignment"
       else: varRtn = f"xx{val}"
    else: varRtn = f"xx{typ} {val}"
    return varRtn
  except Exception as e:
       print(f"Execution failed in {val}:", e)
       sys.exit(1)
#***************************************************************************************************************************
def size(frm, env):
    varVal = sysval("form", frm)
    auth = {"Authorization": "AR-JWT "+env}

    size   = 0
    offset = 0
    limit  = 2000
    rsRsp = []
    while True:
      req  = requests.get(f"{url}/api/arsys/v1/entry/{frm}", headers=auth,params={"limit":limit, "offset": offset}).json()
      rs_task = req["entries"]
      if not rs_task:
         break
      size += len(rs_task)
      offset += limit
      break
    print(f"***** Total ({size}) records count for {frm} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} *****")
#***************************************************************************************************************************
def dump_backup(tbl, frm, spec, env):
    varVal = sysval("form", frm)
    print(f"***** Begin {frm} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} *****")
    auth = {"Authorization": "AR-JWT "+env}

    rec = 0
    offset = 0
    limit  = 500
    rsRsp = []
    while True:
#      req  = requests.get(url+'/api/arsys/v1/entry/'+varVal, headers=auth,params={"limit":limit, "offset": offset}).json()
      req  = requests.get(f"{url}/api/arsys/v1/entry/{frm}", headers=auth,params={"limit":limit, "offset": offset}).json()
      rs_task = req["entries"]
      if not rs_task:
         break
      rsRsp.extend(rs_task)
      rec += len(rs_task)
      offset += limit

    rsKeys = rsRsp[0]["values"]

    f = open(tbl, "a", newline="", encoding="utf-8") 
    writer = csv.writer(f)

    varCols = ""
    if spec != "*":
       varCols = tuple(spec.split(","))
    else:
       rec = 0
       for key, val in rsKeys.items():
           if rec == 0:
              varList = key
           else: 
              varList += "," + key
           rec += 1
       varCols = tuple(varList.split(","))
       writer.writerow(varCols)

    rec = 0
    for r in rsRsp:        
        jsn  =r.get("values",{})
        rec += 1
        rs   = []
        for i in varCols:
            rs.append(jsn.get(i,None))
        writer.writerow(rs)

    f.close()
    print(f"*** {frm} * {rec} rec written to {tbl} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n")          
#***************************************************************************************************************************
def genCSV(tbl, cols, jsn):
    f = open(tbl, "a", newline="", encoding="utf-8") 
    writer = csv.writer(f)

    rec = 0
#    print (jsn["entries"])
#    print(cols)
    for r in jsn["entries"]:
        rec += 1
        rtn  = []
        val  = r.get("values")
        for i in cols:
            rtn.append(val.get(i,None))
        writer.writerow(rtn)

    f.close()
    return rec
#***************************************************************************************************************************
def dump(tbl, frm, spec, env, size=None):
    varVal = sysval("form", frm)
    print(f"***** Begin {frm} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} *****")
    auth = {"Authorization": "AR-JWT "+env}

    req  = requests.get(f"{url}/api/arsys/v1/entry/{frm}", headers=auth,params={"limit":1, "offset": 0}).json()
    jsn  = req.get("entries",[])
    with open(tbl, "w", newline="", encoding="utf-8") as f:
         writer = csv.writer(f)
         if spec != "*":
            varCols = tuple(spec.split(","))
            writer.writerow(varCols)
         else:
            if jsn:
               first_entry = jsn[0]
               values = first_entry.get("values", {})
               varCols = tuple(values.keys())
               writer.writerow(varCols)

    cnt    = 0
    offset = 0
    limit  = 2000
#size=2001    
    while True:
      if size and (offset + limit) > size:
         limit = size - offset
      jsn = requests.get(f"{url}/api/arsys/v1/entry/{frm}", headers=auth,params={"limit":limit, "offset": offset}).json()
      if not jsn["entries"]: break
      cnt += genCSV(tbl, varCols, jsn)
      offset += limit
      if size and cnt >= size: break
      print (cnt, datetime.now())

    print(f"*** {frm} * ({cnt}) rec written to {tbl} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n")          
#***************************************************************************************************************************
#varJSN = ("Entry ID,User_Type,Submitter")
#varJSN = ("*")


url = "https://mtr-dev-restapi.onbmc.com"
pwd = {
    "username":"migrate",
    "password":"0000abc!"
}
vCols = ("*")

#env = login()["token"]
env = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIhPSF7ZW5jfSE9IUFBQUFEQ09jZ2JQZ2RpZkwwUmxZMW01Q1VqcCtyRjYxcDMzNHMvTGg2Q2JRYTAvVi8xaz0iLCJfbG9jYWxlTGFuZ3VhZ2UiOiJlbiIsIm5iZiI6MTc4MTU4MDY5OCwiaXNzIjoicGxhdGZvcm0taW50LTAucGxhdGZvcm0taW50IiwiX2xvY2FsZUNvdW50cnkiOiJVUyIsIl9hdXRoU3RyaW5nIjoiIT0he2VuY30hPSFBQUFBREdLZXc0QzBoUzFaeVNsNzhNa2pvQzVOWjNXR3plSGJjNnlPbjFRPSIsImV4cCI6MTc4MTYwOTYxOCwiX2NhY2hlSWQiOjk0NzI4MiwiaWF0IjoxNzgxNTgwODE4LCJqdGkiOiJJREdGSVIwVEQ4VTJDQVQ2UDZLQ1Q2UDZLQzBYOEwiLCJfYWJzb2x1dGVFeHBpcmF0aW9uVGltZSI6MTc4MTY2NzIxOH0.FHq3jyZ-52TVBWy-GEeyuRSO9al3dsUa4XDDeIeYq00"
empty("BMCBS.20260616.csv")                                      # recreate csv file 
dump("BMCBS.20260616.csv", "BMC.CORE:BMC_BaseRelationship", "Name,ClassId,DatasetId,MarkAsDeleted,InstanceId,Source.InstanceId,Source.ClassId,Destination.InstanceId,Destination.ClassId", env)    # data to csv
empty("BMCBE.20260616.csv")
dump("BMCBE.20260616.csv","BMC.CORE:BMC_BaseElement","Name,ClassId,DatasetId,MarkAsDeleted,InstanceId",env)
#size("BMC.CORE:BMC_BaseRelationship",env)                                     # count table size
#size("BMC.CORE:BMC_BaseElement",env)                                     # count table size
