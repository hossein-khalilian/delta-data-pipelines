from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
from decimal import Decimal

from datetime import datetime, timedelta, timezone
import pytz
import pymssql
import requests
import logging
from utils.config import config

class DateTimeAndDecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)
    
# config
START_TIME_VAR = "last_successful_run"
DEFAULT_START_TIME = (datetime.now() - timedelta(days=1)).replace(microsecond=0).isoformat()
ENDPOINT_URL = config.get("search_endpoint_url")
BATCH_SIZE = 200

DB_CONFIG = {
    "server": config.get('sql_host'),
    "port": config.get('sql_port'),
    "database": config.get('sql_name'),
    "user": config.get('sql_user'),
    "password": config.get('sql_password'),
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
d.ModifiedDate,
d.MainStreet,
d.Price,
d.RentalPrice
FROM Deposits d
WHERE d.StatusId <> 1254
	AND d.ModifiedDate > '2025-06-17 11:30:00.000'
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

def get_cursor():
    conn = pymssql.connect(**DB_CONFIG)
    return conn, conn.cursor(as_dict=True)

def chunkify(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]


# TASKS
def last_runtime_function(**context):
    value = Variable.get(START_TIME_VAR, default_var=DEFAULT_START_TIME)
    return datetime.fromisoformat(value)

def extract_function(**context):
    last_run = context["ti"].xcom_pull(task_ids="last_runtime_task")

    conn, cursor = get_cursor()
    cursor.execute(QUERY, (last_run, last_run))
    rows = cursor.fetchall()
    conn.close()

    logging.info(f"Extracted {len(rows)} rows from SQL Server")
    return rows

def to_camel_case(s):
    parts = s.split('_')
    return parts[0].lower() + ''.join(word.capitalize() for word in parts[1:])

def transform_function(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_task")
    if not rows:
        return []

    tehran_tz = pytz.timezone("Asia/Tehran")
    
    transformed = []
    for row in rows:
        row_dict = dict(row)

        modified_date = row_dict.get("ModifiedDate")
        if modified_date:
            modified_date = tehran_tz.localize(modified_date).astimezone(pytz.UTC).isoformat()

        api_row = {
            "id": int(row_dict.get("Id")),
            "property_type": str(row_dict.get("PropertyTypeId") or ""),
            "deposit_category": str(row_dict.get("DepositCategoryId") or ""), 
            "city_id": int(row_dict.get("CityId") or 0),
            "title": str(row_dict.get("Title") or ""),
            "modified_date": modified_date,
            "region": str(row_dict.get("RegionId") or ""),
            "price": int(row_dict.get("Price") or 0),
            "rental_price": int(row_dict.get("RentalPrice") or 0),
            "meter": int(row_dict.get("meter") or 0),
            "floor": str(row_dict.get("floor") or ""),
            "rooms": str(row_dict.get("rooms") or ""),
            "age": int(row_dict.get("age") or 0),
            "parking": bool(row_dict.get("parking")),
            "warehouse": bool(row_dict.get("warehouse")),
            "elevator": bool(row_dict.get("elevator")),
            "loan": bool(row_dict.get("loan")),
            "description": str(row_dict.get("Description") or ""),
            "status": "active" if row_dict.get("StatusId") == 1247 else "inactive"
        }

        transformed.append(api_row)

    logging.info(f"Transformed {len(transformed)} records")
    return transformed

def load_function(**context):
    data = context["ti"].xcom_pull(task_ids="transform_task")
    if not data:
        logging.info("No data to send")
        return None

    for batch in chunkify(data, BATCH_SIZE):
        payload = {
            "properties": batch
        }
        response = requests.post(
            ENDPOINT_URL,
            data=json.dumps(payload, cls=DateTimeAndDecimalEncoder),
            timeout=20
        )
        
        if not response.ok:
            logging.error(
                "Failed to send batch. Status=%s Response=%s",
                response.status_code,
                response.text
            )

        response.raise_for_status()

    logging.info("All batches sent successfully")
    return datetime.now().replace(microsecond=0).isoformat()

def update_time_function(**context):
    new_value = context["ti"].xcom_pull(task_ids="load_function")
    if new_value:
        Variable.set(START_TIME_VAR, new_value)

# DAG
with DAG(
    dag_id="delta-data-extractor",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["extract data from database"],
) as dag:

    last_runtime_task = PythonOperator(
        task_id="last_runtime_task",
        python_callable=last_runtime_function,
    )

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

    update_time_task = PythonOperator(
        task_id="update_time_task",
        python_callable=update_time_function,
    )

    last_runtime_task >> extract_task >> transform_task >> load_task >> update_time_task
