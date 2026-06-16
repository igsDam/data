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

POSTGRES_CONNx = {
   "host":"localhost",
   "dbname":"itsm_db",
   "user":"itsm_app",
   "password":"itsm_app",
   "port":5432,
}
POSTGRES_CONN = {
   "host":"postgres.magic-creative-dev.svc.cluster.local",
   "dbname":"mcdesk",
   "user":"mcdesk",
   "password":"MagicCreative@2025!",
   "port":5432,
}


varITSM=("filter,id,number,call_id_sys_user_id,description,short_description,"
         "configuration_item_service,configuration_item,"
         "business_service_id,type,impact,urgency,priority,channel,comments,"
         "assignment_group_id,assigned_sys_user_id,assigned_to_user_id,"
         "third_party_type,third_party_key,assignment_group,item_for_user_id,"
         "assigned_to,state,resolution_notes,workaround,fix_notes,"
         "category,sub_category,resolved_at,"
         "start_date,end_date,due_date,work_start,work_end,"
         "opened_sys_user_id,opened_by_id,item_open_by_user_id,"
         "opened_at,requested_by_id,updated_by,updated_time")
varITSM_Assoc=("filter,updated_by,updated_time,channel,comments,description,short_description,"
               "form01,form02,req_id01,req_id02,"
               "status_asset,status_inc,status_crq,status_pbi,status_req,"
               "status_tsk,status_rlm,status_aot,status_kdb,status_wor,"
               "status_pke,status_prq,status_acm,status_con,status_sla,status_ola")
varUser=("first_name,last_name,customer,job_title,nickname,"
          "profile_status,contract_type,client_sensitivity,user_type,support_staff,"
          "pin,company,organization,department,business,email_address,"
          "site_group,site,support_mobile,login_id,license_type,application_permission,"
          "home_information,acd,corporate_email,accounting_number,support_group")
varCI=("filter,id,created_by,created_time,updated_by,updated_time,"
       "ci_name,ci_type,hierarchy_path")
varCMDB=("filter,asset_name,description,ci_id,tag_number,serial_number,part_number,"
          "supported,asset_description,company,system_role,primary_capability,"
          "capability_list,status,status_reason,impact,urgency,priority,users_affected,"
          "prod_category_tier1,prod_category_tier2,prod_category_tier3,"
          "product_name,model,manufacturer,supplier_name,"
          "region,site_group,site,floor,room,"
          "received_date,installation_date,available_date,inactive_date,"
          "disposal_date,last_scan_date,"
          "owner_name,owner_contact,environment_specification,configuration_options,"
          "system_type,reset_capability,expansion,expansion_interface,impact_computation_model,"
          "virtual_system_type,is_virtual,dns_host_name,workgroup,domain,number_of_slots,"
          "ports_per_slot,total_physical_memory,flash_memory,"
          "work_infocontracts,people")
varGeneric=("type,code,sid,class,val,form,"
            "created_by,creation_time,"
            "desc1,desc2,desc3,desc4,desc5,"
            "data1,data2,data3,data4,data5")


url = "https://mtr-dev-restapi.onbmc.com"
hdr = {
    "X-Requested-BY":"X",
    "Content-Type": "application/x-www-form-urlencoded"
}
pwd = {
    "username":"migrate",
    "password":"0000abc!"
}
#***Common def**************************************************************************************************************
def login():
    rsp = requests.post(url+"/api/jwt/login", headers=hdr, data=pwd)
    print ("******************************************************")
    print (rsp.text)
    print ("******************************************************\n")
    return {"token": rsp.text}

def init(ref):
  if ref == "csv":
    with open("itsm.csv", "w", newline="", encoding="utf-8") as f:
      writer = csv.writer(f)
      writer.writerow(varITSM.split(","))
    with open("itsm_assoc.csv", "w", newline="", encoding="utf-8") as f:
      writer = csv.writer(f)        
      writer.writerow(varITSM_Assoc.split(","))
    with open("sys_user.csv", "w", newline="", encoding="utf-8") as f:
      writer = csv.writer(f)        
      writer.writerow(varUser.split(","))
    with open("cmdb.csv", "w", newline="", encoding="utf-8") as f:
      writer = csv.writer(f)
      writer.writerow(varCMDB.split(","))
  elif ref == "json":
    pass
  else:
    try: 
       conn = psycopg2.connect(**POSTGRES_CONN)
       writer = conn.cursor()
       if ref == "genTbl":
          genTbl(writer)
       else: writer.execute(f"truncate table {ref};")
       conn.commit()
    except Exception as e:
       print("Execution failed:", e)
       sys.exit(1)

def genTbl(writer):
    varTbl = "tmp_stg.sg_generic"
    varSQL = f"insert into {varTbl}(sid, type, code, created_by, creation_time) values (%s, %s, %s, %s, %s);"

    writer.execute(varSQL, (1, "tbl","seq",                        "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (2, "tbl","configuration_item_service", "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (3, "tbl","configuration_item",         "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (4, "tbl","impact",                     "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (5, "tbl","urgency",                    "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (6, "tbl","priority",                   "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (7, "tbl","risk",                       "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (8, "tbl","uid",                        "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (9, "tbl","gid",                        "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (10,"tbl","category",                   "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (11,"tbl","subcategory",                "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (12,"tbl","channel",                    "airflow.genTbl",datetime.now()))
    writer.execute(varSQL, (13,"tbl","type",                       "airflow.genTbl",datetime.now()))

def syscast(ref, frm, typ, tbl, val):
    varVal = ""
    if ref == "csv":
       return val
    elif val is None: return None
#    elif typ in ("uid"):
#       return 936390600155037696
#    elif typ in ("gid"):
#       return 870088239434772480
#    elif typ in ("*configuration_item_service","configuration_item","business_service_id"):
#       return None
#    elif typ in ("ci_id"):
#       return None
#    elif typ in ("category","subcategory","channel"):
#       return f"*{val}"
    elif typ == "form":
       if val == "HPD":      varVal = "HPD:Help Desk"
       elif val == "PBM":    varVal = "PBM:Problem Investigation"
       elif val == "PBE":    varVal = "PBM:Known Error"
       elif val == "CHG":    varVal = "CHG:Infrastructure Change"
       elif val == "WOI":    varVal = "WOI:WorkOrder"
       elif val == "HPD_WL": varVal = "HPD:WorkLog"
       elif val == "PBM_WL": varVal = "PBM:Investigation WorkLog"
       elif val == "PBE_WL": varVal = "PBM:Known Error WorkLog"
       elif val == "CHG_WL": varVal = "CHG:WorkLog"
       elif val == "WOI_WL": varVal = "WOI:WorkOrder WorkLog"
       elif val == "CTM":    varVal = "CTM:People"
       elif val == "HPD_AS": varVal = "HPD:Associations"
       elif val == "PBM_AS": varVal = "PBM:Investigation Associations"
       elif val == "CHG_AS": varVal = "CHG:Associations"
       elif val == "AST_BS": varVal = "BMC.CORE:BMC_BusinessService"
       elif val == "AST_CS": varVal = "BMC.CORE:BMC_ComputerSystem"
       elif val == "PCT_PC": varVal = "PCT:ProductCatalogJoin"
       elif val == "CTM_SG": varVal = "CTM:Support Group"
       elif val == "CFG_AS": varVal = "CFG:Assignment"
       else: varVal = f".{val}"
    elif typ == "seq":
       if   val.startswith("INC"): varVal = "1"
       elif val.startswith("PBI"): varVal = "2"
       elif val.startswith("CRQ"): varVal = "3"
       elif val.startswith("PKE"): varVal = "4"
       elif val.startswith("PBM"): varVal = "5"
       elif val.startswith("PDC_SEED"): varVal = "8"
       elif val.startswith("PDC0"): varVal = "8"
       else : varVal = "9"
    elif typ == "state":
       if   val == "New":         varVal = "1"
       elif val == "On hold":     varVal = "2"
       elif val == "Resolved":    varVal = "3"
       elif val == "Closed":      varVal = "4"
       elif val == "In Progress": varVal = "5"
       elif val == "Cancelled":   varVal = "5"
       else: varVal = f".{val}"
    elif ref == "map": valVal = f".{val}"
    else:
       tbl.execute(f"select 1 seq, val from tmp_stg.sg_generic where type = %s and code = %s", (typ, val))
       rec = tbl.fetchone()
       if rec is None:
          varSeq = None
          varVal = None
       else: 
          varSeq = rec[0]
          varVal = rec[1]
       if nvl(varSeq,None) is None:
#          logging.warning(f" ## cast *{typ} failure for ({val})")
#          if typ in ("gid","uid"): return 0
#          else: return f"*{val}"
          varCHK = f"select 1 seq, segment, code from tmp_stg.sg_msg where segment = 'cast' and code = '{val}'"
          varSQL = ("insert into tmp_stg.sg_msg (segment, code, type, class, form, desc0, created_by, creation_time) values " 
                   "('cast', %s, %s, %s, %s, 'type/code not found', 'etl.syscast', now()) on conflict do nothing")
          tbl.execute(varCHK)
          rs = tbl.fetchone()
          if rs is None:
#             msg_conn = psycopg2.connect(**POSTGRES_CONN)
#             msg_tbl  = msg_conn.cursor()

#             msg_tbl.execute(varSQL,(val, typ, ref, frm))
#             msg_conn.commit()
#             msg_conn.close()
             tbl.execute(varSQL,(val, typ, ref, frm))
             logging.warning(f" ## {frm} casting failure for code {val} on {typ}")

          if typ in ("gid","uid","business_service_id","configuration_item_service","configuration_item"):
             varVal = "-1"
          else: varFal = f"*{val}"
       return varVal
    return varVal

def seq(val):
    varPrx = syscast(None,None,"seq",None,val)
    varNew = "0000000000"+re.sub(r"[^0-9.]", "", val)
    return varPrx+varNew[-9:]

def chkspec(dct, jsn):
    for key, (lbl, col, meta) in dct.items():
        if col is not None and col not in jsn:
            logging.warning(f" ## Column NotFound ## {col}")

def syswrite(tbl, frm, writer, rs, spec, seed, flag):
    if not tbl.endswith("csv") and flag != None:
       spec = spec +",custom_data"
    varRow = dict(rs.get("values",{}))
    varCols = tuple(spec.split(","))
#    varDict = {lbl: (lbl, col, fmt) for lbl, col, fmt in seed}
    varDict = seed

    varVal = []
    for i in varCols:               #ETL - translation
        vLbl, vCol, vFmt = varDict.get(i, (None, None, None))
        if tbl.endswith("csv"):
           varVal.append(varRow.get(vCol, None))
        if i == "custom_data":
           varVal.append(Json(rs))
#        elif vCol == None:
#           varVal.append(None)
        elif vFmt is None:     varVal.append(varRow.get(vCol, None))
        elif vFmt == "None":   varVal.append(varRow.get(vCol, None))
        elif vFmt == "seq":    varVal.append(seq(varRow.get(vCol, None)))
        elif vFmt.startswith("join."): 
             varETL = tuple(vFmt.replace("join.(","").replace(")").split(","))
             varList = ""
             for j in varETL:
                 varList = varList + "," + varRow.get(j, None)
        elif vFmt.startswith("dft."):  varVal.append(vFmt.replace("dft.", ""))
        elif vFmt.startswith("map."):  varVal.append(syscast("map", frm, vFmt.replace("map.", ""),  None,   varRow.get(vCol, None)))
        elif vFmt.startswith("cast."): varVal.append(syscast(tbl,   frm, vFmt.replace("cast.", ""), writer, varRow.get(vCol, None)))
        else:  varVal.append(varRow.get(vCol, None))

    varRec = tuple(varVal)

    if tbl.endswith(".csv"):
       writer.writerow(varRec)
    else:
       cols = ", ".join(varCols)
       fill = ", ".join(["%s"] * len(varCols))          
       sql = f"INSERT INTO {tbl} ({cols}) VALUES ({fill})"
       try: 
            writer.execute(sql, varRec)
            return True
       except Exception as e:
#            print("Execution failed:", e)
#            print(spec)
#            print("Values:", varRec)
#            sys.exit(1)
            logging.error  (f" ## Last write error {e}")
#            conn.rollback()
#            continue
            if hasattr(writer, "connection"):
               writer.connection.rollback()
            return False

def tfr(dest, env, frm, spec, dict, custom, bulk):
    varVal = syscast(None,frm,"form",None,frm)
    print(f"***** Begin {varVal} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ****{datetime.now()}*")
    auth = {"Authorization": "AR-JWT " + env["token"]}
    rec    = 0
    limit  = 2000
    offset = bulk if bulk else 0
    rsRsp = []
    while True:
      if bulk and rec > 10000: break
      req  = requests.get(url+'/api/arsys/v1/entry/'+varVal, headers=auth,params={"limit":limit, "offset": offset}).json()
      rs_task = req["entries"]
      if not rs_task: break
      rsRsp.extend(rs_task)
      rec += len(rs_task)
      offset += limit
    rsKeys = rsRsp[0]["values"]
    chkspec(dict,rsKeys)
    
    if dest.endswith("csv"):
       f = open(dest, "a", newline="", encoding="utf-8") 
       writer = csv.writer(f)
    else:
       conn = psycopg2.connect(**POSTGRES_CONN)
       writer = conn.cursor()

    rec    = 0
    for r in rsRsp:
        rec += 1
        syswrite(dest,frm,writer,r,spec,dict,custom)

    if dest == "csv":
       f.close()
    else:
       conn.commit()
       writer.close()
       conn.close()
    varPage = f"/ page {bulk}" if bulk else None
    print(f"*** {varVal} * {rec} rec {varPage} written to {dest} {datetime.now()}\n\n")
#*dict***********************************************************************************************************************
def nvl(val,rtn):
    if pd.isna(val): return rtn
    return val
#*dict***********************************************************************************************************************
def etl_dict(frm):
    rsCsv = pd.read_csv(frm).to_dict(orient="records")
    if not nvl(rsCsv[0].get("enabled"),None): return None
    
    rs = []
    for row in rsCsv:
        rs.append((nvl(row.get("dest"),None), nvl(row.get("src"),None), nvl(row.get("def"),None)))
    
    return {lbl: (lbl, col, fmt) for lbl, col, fmt in rs}
'''
#***************************************************************************************************************************
env = login()
dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/HPD.csv")
if dbs: tfr(dbs["tbl"][2], env, "HPD", varITSM, dbs, 1, None)
#init("tmp_stg.sg_itsm")
#init("tmp_stg.sg_user")
#init("tmp_stg.sg_itsm_assoc")
#init("tmp_stg.sg_cmdb")

#dbs = hpd_map("HPD")
#tfr(dbs["tbl"][2], env, "HPD", varITSM, dbs, 1, None)
'''
#*airflow script************************************************************************************************************
def run_init(**kwargs):
#    init("genTbl")
    init("tmp_stg.sg_itsm")
    init("tmp_stg.sg_user")
    init("tmp_stg.sg_itsm_assoc")
    init("tmp_stg.sg_cmdb")   
    env = login()
    kwargs['ti'].xcom_push(key="env", value=env)
def run_hpd(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
#    dbs = hpd_map("HPD")
#    tfr(dbs["tbl"][2], env, "HPD", varITSM, dbs, 1, None)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/HPD.csv")
    if dbs: tfr(dbs["tbl"][2], env, "HPD", varITSM, dbs, 1, None)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/HPD_WL.csv")
    if dbs: tfr(dbs["tbl"][2], env, "HPD_WL", varITSM, dbs, 1, None)
def run_pbm(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/PBM.csv")
    if dbs: tfr(dbs["tbl"][2], env, "PBM", varITSM, dbs, 1, None)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/PBM_WL.csv")
    if dbs: tfr(dbs["tbl"][2], env, "PBM_WL", varITSM, dbs, 1, None)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/PBE.csv")
    if dbs: tfr(dbs["tbl"][2], env, "PBE", varITSM, dbs, 1, None)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/PBE_WL.csv")
    if dbs: tfr(dbs["tbl"][2], env, "PBE_WL", varITSM, dbs, 1, None)
def run_chg(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/CHG.csv")
    if dbs: tfr(dbs["tbl"][2], env, "CHG", varITSM, dbs, 1, None)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/CHG_WL.csv")
    if dbs: tfr(dbs["tbl"][2], env, "CHG_WL", varITSM, dbs, 1, None)
def run_woi(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/WOI.csv")
    if dbs: tfr(dbs["tbl"][2], env, "WOI", varITSM, dbs, 1, 1)
    if dbs: tfr(dbs["tbl"][2], env, "WOI", varITSM, dbs, 1, 2)
    if dbs: tfr(dbs["tbl"][2], env, "WOI", varITSM, dbs, 1, 3)
    if dbs: tfr(dbs["tbl"][2], env, "WOI", varITSM, dbs, 1, 4)
    if dbs: tfr(dbs["tbl"][2], env, "WOI", varITSM, dbs, 1, 5)
def run_assoc(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/HPD_AS.csv")
    if dbs: tfr(dbs["tbl"][2], env, "HPD_AS", varITSM_Assoc, dbs, 1, 1)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/PBM_AS.csv")
    if dbs: tfr(dbs["tbl"][2], env, "PBM_AS", varITSM_Assoc, dbs, 1, 1)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/CHG_AS.csv")
    if dbs: tfr(dbs["tbl"][2], env, "CHG_AS", varITSM_Assoc, dbs, 1, 1)
def run_ctm(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/CTM.csv")
    if dbs: tfr(dbs["tbl"][2], env, "CTM", varUser, dbs, 1, 1)
def run_itm(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/PCT_PC.csv")
    if dbs: tfr(dbs["tbl"][2], env, "PCT_PC", varCI, dbs, 1, 1)
def run_ast(**kwargs):
    ti  = kwargs['ti']
    env = ti.xcom_pull(key="env", task_ids="run_init")
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/AST_BS.csv")
    if dbs: tfr(dbs["tbl"][2], env, "AST_BS", varCMDB, dbs, 1, 1)
    dbs = etl_dict("https://raw.githubusercontent.com/igsDam/data/main/AST_CS.csv")
    if dbs: tfr(dbs["tbl"][2], env, "AST_PC", varCMDB, dbs, 1, 1)


with DAG(
    dag_id="MTRC_ETL",
    catchup=False,
    tags=["MTRCv12.7", "MTRC"],
) as dag:

    init0 = PythonOperator(
        task_id="run_init",
        python_callable=run_init,
    )
    hpd = PythonOperator(
        task_id="run_hpd",
        python_callable=run_hpd,
    )
    pbm = PythonOperator(
        task_id="run_pbm",
        python_callable=run_pbm,
    )
    chg = PythonOperator(
        task_id="run_chg",
        python_callable=run_chg,
    )
    woi = PythonOperator(
        task_id="run_woi",
        python_callable=run_woi,
    )
    relationship = PythonOperator(
        task_id="run_assoc",
        python_callable=run_assoc,
    )
    ctm = PythonOperator(
        task_id="run_ctm",
        python_callable=run_ctm,
    )
    itm = PythonOperator(
        task_id="run_itm",
        python_callable=run_itm,
    )
    ast = PythonOperator(
        task_id="run_ast",
        python_callable=run_ast,
    )

    init0 >> hpd >> pbm >> chg >> woi >> relationship >> ctm >> itm >> ast