from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pytz
import requests
import logging
import pymssql
from utils.config import config

import json

# config
ENDPOINT_UPDATE_ALL = f"{config['search_endpoint_url']}/update-all-properties"

DB_CONFIG = {
    "server": config["sql_host"],
    "port": config["sql_port"],
    "database": config["sql_name"],
    "user": config["sql_user"],
    "password": config["sql_password"],
}

# query
QUERY = """
WITH FilteredDeposits AS (
SELECT
d.Id,
d.Title,
d.Description,
d.DepositCategoryId,
d.PropertyTypeId,
d.StatusId,
d.CityId,
d.RegionId,
d.Createdtime,
d.ModifiedDate,
d.MainStreet,
d.Price,
d.RentalPrice
FROM Deposits d
WHERE d.StatusId =1247
	AND d.ModifiedDate > DATEADD(MONTH, -1, GETDATE())
),
PivotCustomFields AS (
SELECT
cfv.DepositId,
MAX(CASE WHEN cfv.CustomFieldId IN (1224, 1225, 1226, 1227, 1228, 1229, 1230, 1231, 1232, 1233, 1234, 1235, 1236, 1237, 1238, 1239, 1240, 1241, 1242, 1243, 1200, 1167, 1159, 1117, 1125, 1133, 1174, 1181,1162, 1150, 1141, 1203, 1261, 1196, 1188, 1199, 1195, 1260, 1202, 1244, 1245, 1149, 1155, 1158, 1163, 1161)
THEN COALESCE(cfv.Value, cfo.Value) END) AS meter,
MAX(CASE WHEN cfv.CustomFieldId In (1189, 1142, 1126, 1118, 1134, 1175, 1182, 1168)
THEN COALESCE(cfv.Value, cfo.Value) END) AS floor,
MAX(CASE WHEN cfv.CustomFieldId In (1143, 1135, 1127, 1119, 1176, 1169, 1166, 1151, 1197, 1183, 1190, 1262)
THEN COALESCE(cfv.Value, cfo.Value) END) AS rooms,
MAX(CASE WHEN cfv.CustomFieldId In (1136, 1152, 1184, 1191, 1198, 1263, 1170, 1177, 1144, 1120, 1128)
THEN COALESCE(cfv.Value, cfo.Value) END) AS age,
MAX(CASE WHEN cfv.CustomFieldId In (1185, 1192, 1171, 1178, 1121, 1129, 1137, 1145)
THEN COALESCE(cfv.Value, cfo.Value) END) AS parking,
MAX(CASE WHEN cfv.CustomFieldId In (1193, 1186, 1179, 1172, 1146, 1138, 1130, 1122)
THEN COALESCE(cfv.Value, cfo.Value) END) AS warehouse,
MAX(CASE WHEN cfv.CustomFieldId IN (1123, 1131, 1139, 1147, 1173, 1180, 1187, 1194)
THEN COALESCE(cfv.Value, cfo.Value) END) AS elevator,
MAX(CASE WHEN cfv.CustomFieldId In (1148, 1140, 1132, 1124)
THEN COALESCE(cfv.Value, cfo.Value) END) AS loan
FROM CustomFieldValues cfv
LEFT JOIN CustomFieldOptions cfo
ON cfv.CustomFieldOptionId = cfo.Id
GROUP BY cfv.DepositId
)
SELECT
d.Id,
d.Title,
d.Description,
dc.Link AS DepositCategoryId,
bi.Title AS PropertyTypeId,
d.StatusId,
d.CityId,
r.Name AS RegionId,
d.CreatedTime,
d.ModifiedDate,
d.MainStreet,
d.Price,
d.RentalPrice,
p.meter,
p.floor,
p.rooms,
p.age,
p.parking,
p.warehouse,
p.elevator,
p.loan
FROM FilteredDeposits d
LEFT JOIN DepositCategories dc
ON d.DepositCategoryId = dc.Id
LEFT JOIN BaseInfos bi
ON d.PropertyTypeId = bi.Id
LEFT JOIN Regions r
ON d.RegionId = r.Id
LEFT JOIN PivotCustomFields p
ON d.Id = p.DepositId
ORDER BY d.Id DESC;
"""

# UTILS
def get_cursor():
    conn = pymssql.connect(
        server=DB_CONFIG["server"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        login_timeout=30,
        timeout=300,
        autocommit=True
    )
    return conn, conn.cursor(as_dict=True)


def safe_int(value):
    try:
        return int(float(value))
    except Exception:
        return 0

def age_to_build_year(age):
    try:
        age = int(age)
    except Exception:
        return None

    current_gyear = datetime.now().year
    current_jyear = current_gyear - 621  
    if age > 30:
        return current_jyear - 31
    elif age > 20:
        return current_jyear - 21
    else:
        return 1404

def normalize_property_type(property_type):
    if not property_type:
        return None

    pt = str(property_type).strip()

    if "مشارکت" in pt:
        return None  

    if "زمین" in pt:
        return "باغ باغچه و زمین"

    if "صنعتی" in pt or "زراعی" in pt:
        return "باغ باغچه و زمین"

    allowed = {
        "آپارتمان مسکونی",
        "آپارتمان اداری",
        "خانه - ویلا",
        "مغازه - تجاری",
        "مستغلات",
        "باغ باغچه و زمین",
    }

    return pt if pt in allowed else pt


def extract(**_):
    conn, cursor = get_cursor()
    cursor.execute(QUERY)
    rows = cursor.fetchall()
    conn.close()

    logging.info("Extracted %s rows", len(rows))
    return rows

def transform(ti, **_):
    rows = ti.xcom_pull(task_ids="extract")
    if not rows:
        logging.warning("No rows received from extract")
        return []

    tehran_tz = pytz.timezone("Asia/Tehran")
    transformed = []
    
    for row in rows:
        row_dict = dict(row)
        raw_property_type = row_dict.get("PropertyTypeId")
        normalized_property_type = normalize_property_type(raw_property_type)

        if normalized_property_type is None:
            continue
        
        db_modified = row_dict.get("ModifiedDate")
        if db_modified:
            iran_dt = tehran_tz.localize(db_modified)
            utc_dt = iran_dt.astimezone(pytz.UTC)
            modified_date_utc = utc_dt.isoformat()
        else:
            modified_date_utc = None
            
        db_created = row_dict.get("CreatedTime")
        if db_created:
            iran_dt = tehran_tz.localize(db_created)
            utc_dt = iran_dt.astimezone(pytz.UTC)
            created_time_utc = utc_dt.isoformat()
        else:
            created_time_utc = None
            
        raw_age = safe_int(row_dict.get("age"))
        build_year = age_to_build_year(raw_age)

        api_row = {
            "id": int(row_dict.get("Id")),
            "property_type": normalized_property_type,
            "deposit_category": str(row_dict.get("DepositCategoryId") or ""), 
            "city_id": int(row_dict.get("CityId") or 0),
            "title": str(row_dict.get("Title") or ""),
            "created_time": created_time_utc,
            "modified_time": modified_date_utc,
            "region": str(row_dict.get("RegionId") or ""),
            "price": int(row_dict.get("Price") or 0),
            "rental_price": int(row_dict.get("RentalPrice") or 0),
            "meter": safe_int(row_dict.get("meter")),
            "floor": str(row_dict.get("floor") or ""),
            "rooms": str(row_dict.get("rooms") or ""),
            "age": build_year ,
            "parking": bool(row_dict.get("parking")),
            "warehouse": bool(row_dict.get("warehouse")),
            "elevator": bool(row_dict.get("elevator")),
            "loan": bool(row_dict.get("loan")),
            "description": str(row_dict.get("Description") or ""),
            "status": "active" ,
        }

        transformed.append(api_row)
        
    logging.info(f"Transformed {len(transformed)} records")
    return transformed

def update_all(ti, **_):
    properties = ti.xcom_pull(task_ids="transform")

    if not properties:
        logging.warning("No data to send to update-all")
        return

    response = requests.post(
        ENDPOINT_UPDATE_ALL,
        json={"properties": properties},
        timeout=120,
    )

    if not response.ok:
        logging.error(
            "Update-all failed | status=%s | response=%s",
            response.status_code,
            response.text,
        )
        response.raise_for_status()

    logging.info("Update-all succeeded | count=%s", len(properties))

# DAG
with DAG(
    dag_id="sql-search-index-full-rebuild",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["search-engine", "db-update", "nightly"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    update_all_task = PythonOperator(
        task_id="update_all",
        python_callable=update_all,
    )

    extract_task >> transform_task >> update_all_task