from airflow import DAG
from airflow.operators.python import PythonOperator
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import pytz
import requests
import logging
from utils.config import config
import json

from search_engine.utils.utils_of_searchengine import get_cursor, safe_int, age_to_build_year, normalize_property_type

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
ENDPOINT_URL = (f"{config["search_engine_endpoint_url"]}/add-properties")
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
d.CreatedTime,
d.ModifiedDate,
d.MainStreet,
d.Price,
d.RentalPrice
FROM Deposits d
WHERE d.StatusId <> 1254
	AND d.ModifiedDate > %s
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
ur.RoleId AS UserroleId,
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
LEFT JOIN MinUserRole ur
ON d.UserId = ur.UserId
ORDER BY d.Id DESC;
"""

# TASKS
def get_time_function(**context):
    url = f"{config['search_endpoint_url']}/last-modified-property"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json() or {}
        modified_date_str = data.get("modified_time")
    except Exception as e:
        logging.warning(f"Failed to fetch last_modified: {e}")
        modified_date_str = None

    tehran_tz = pytz.timezone("Asia/Tehran")

    if modified_date_str:
        utc_dt = datetime.fromisoformat(modified_date_str)
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=timezone.utc)

        iran_dt = (
            utc_dt
            .astimezone(tehran_tz)
            .replace(tzinfo=None)
            - timedelta(hours=1)
        )
        
        logging.info(
            f"API UTC: {utc_dt.isoformat()} | "
            f"Iran DB time (minus 1h): {iran_dt.isoformat()}"
        )

        return iran_dt

    fallback_dt = (datetime.now(tehran_tz) - timedelta(days=1)).replace(tzinfo=None)
    logging.warning(f"No modified_date found. Falling back to: {fallback_dt.isoformat()}")
    return fallback_dt


def extract_function(ti, **context):
    last_run = ti.xcom_pull(task_ids="get_time_task")
    conn, cursor = get_cursor()
    try: 
        cursor.execute(QUERY, (last_run,))
        rows = cursor.fetchall()

        logging.info(f"Extracted {len(rows)} rows from SQL Server")
        ti.xcom_push(key="raw_rows", value=rows)

        return None
    finally:
        conn.close()

def to_camel_case(s):
    parts = s.split('_')
    return parts[0].lower() + ''.join(word.capitalize() for word in parts[1:])

def transform_function(ti, **context):
    rows = ti.xcom_pull(task_ids="extract_task", key="raw_rows")
    if not rows:
        logging.warning("No rows received from extract")
        ti.xcom_push(key="transformed_rows", value=[])
        return

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
            "user_role_id":int(row_dict.get("UserroleId") or 13),
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
            "age": build_year,
            "parking": bool(row_dict.get("parking")),
            "warehouse": bool(row_dict.get("warehouse")),
            "elevator": bool(row_dict.get("elevator")),
            "loan": bool(row_dict.get("loan")),
            "description": str(row_dict.get("Description") or ""),
            "status": "active" if row_dict.get("StatusId") == 1247 else "inactive"
        }

        transformed.append(api_row)
        
    logging.info("Sample transformed: %s", json.dumps(transformed[:3], ensure_ascii=False))
    logging.info(f"Transformed {len(transformed)} records")
    
    ti.xcom_push(key="transformed_rows", value=transformed)

    return None

def load_function(ti, **context):
    properties = ti.xcom_pull(task_ids="transform_task", key="transformed_rows")
    if not properties:
        logging.info("No data to send")
        return None

    total = len(properties)
    total_batches = (total - 1) // BATCH_SIZE + 1
    successful_batches = 0

    logging.info("Total batch count = %s", total_batches)
    
    
    for i in range(0, total, BATCH_SIZE):
        batch = properties[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        batch_count = len(batch)

        logging.info("Sending batch %s/%s", batch_num, total_batches)

        try:
            response = requests.post(
                ENDPOINT_URL,
                json={
                    "properties": batch,
                    "batch_number": batch_num,
                    "total_batches": total_batches,
                },
                headers = {
                    "Authorization": f"Bearer {config["search_engine_access_token"]}",
                    "accept": "/"
                },
                timeout=60,
            )

            if response.ok:
                logging.info("Batch %s sent successfully | count=%s | status=%s",
                             batch_num, batch_count, response.status_code)
                successful_batches += 1
                data = response.json()
                logging.info(f"(response) added count: {data.get('added_count')}")    
                logging.info(f"(response) message : {data.get('message')}") 

                if data.get('added_count') == 0:
                    logging.error("added_count is 0, failing task")
                    raise Exception("API error: added_count is 0")

            else:
                logging.error("Batch %s failed | status=%s | response=%s",
                              batch_num, response.status_code, response.text)
                response.raise_for_status()     

        except requests.exceptions.RequestException as e:
            logging.error("Batch %s request failed: %s", batch_num, str(e))
            raise 

    logging.info("All batches completed | Total sent: %s properties in %s batches",
                 total, successful_batches)

# DAG
with DAG(
    dag_id="sql-search-index-incremental",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["extract data", "delta database"],
) as dag:

    get_time_task = PythonOperator(
        task_id="get_time_task",
        python_callable=get_time_function,
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

    get_time_task >> extract_task >> transform_task >> load_task
