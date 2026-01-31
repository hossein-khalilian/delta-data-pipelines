from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
from utils.config import config

from search_engine.utils.utils_of_searchengine import (
    get_db_cursor,
    normalize_property_type,
    safe_int,
    age_to_build_year,
    send_batches,
    iran_datetime_to_utc_iso,
    check_search_health,
)

# config
ENDPOINT_UPDATE_ALL = f"{config['search_engine_endpoint_url']}/update-all-properties"
ENDPOINT_HEALTH = f"{config['search_engine_endpoint_url']}/health"
BATCH_SIZE = 200

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
d.UserId,
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
),
MinUserRole AS (
    SELECT
        UserId,
        MIN(RoleId) AS RoleId
    FROM usr.UserRoles
    GROUP BY UserId
)
SELECT
d.Id,
d.Title,
d.Description,
dc.Link AS DepositCategoryId,
bi.Title AS PropertyTypeId,
d.StatusId,
ur.RoleId AS UserId,
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
LEFT JOIN MinUserRole ur ON d.UserId = ur.UserId
ORDER BY d.Id DESC;
"""
    
def extract_function(ti, **context):
    check_search_health(ENDPOINT_HEALTH)
    with get_db_cursor() as cursor:
        cursor.execute(QUERY)
        logging.info("SQL query executed")
        rows = cursor.fetchall()
        
    logging.info(f"Extracted {len(rows)} rows")
    ti.xcom_push(key="raw_rows", value=rows)

def transform_function(ti, **context):
    rows = ti.xcom_pull(task_ids="extract_task", key="raw_rows")
    if not rows:
        logging.warning("No rows to transform")
        ti.xcom_push(key="transformed_rows", value=[])
        return

    transformed = []
    
    for row in rows:
        row_dict = dict(row)
        prop_type = normalize_property_type(row_dict.get("PropertyTypeId"))
        if not prop_type:

            continue

        transformed.append({
            "id": int(row_dict.get("Id")),
            "property_type": prop_type,
            "deposit_category": str(row_dict.get("DepositCategoryId") or ""),
            "user_role_id": int(row_dict.get("UserId") or 13),
            "city_id": int(row_dict.get("CityId") or 0),
            "title": str(row_dict.get("Title") or ""),
            "created_time": iran_datetime_to_utc_iso(row_dict.get("CreatedTime")),
            "modified_time": iran_datetime_to_utc_iso(row_dict.get("ModifiedDate")),
            "region": str(row_dict.get("RegionId") or ""),
            "price": int(row_dict.get("Price") or 0),
            "rental_price": int(row_dict.get("RentalPrice") or 0),
            "meter": safe_int(row_dict.get("meter")),
            "floor": str(row_dict.get("floor") or ""),
            "rooms": str(row_dict.get("rooms") or ""),
            "age": age_to_build_year(safe_int(row_dict.get("age"))),
            "parking": bool(row_dict.get("parking")),
            "warehouse": bool(row_dict.get("warehouse")),
            "elevator": bool(row_dict.get("elevator")),
            "loan": bool(row_dict.get("loan")),
            "description": str(row_dict.get("Description") or ""),
            "status": "active",
        })

    logging.info(f"Transformed {len(transformed)} rows")
    ti.xcom_push(key="transformed_rows", value=transformed)

def load_function(ti, **context):
    rows = ti.xcom_pull(task_ids="transform_task", key="transformed_rows")
    send_batches(endpoint=ENDPOINT_UPDATE_ALL, items=rows, batch_size=BATCH_SIZE)

# DAG
with DAG(
    dag_id="sql-search-index-full-rebuild",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["search-engine", "full-rebuild", "nightly"],
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_function,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_function,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_function,
    )

    extract_task >> transform_task >> load_task