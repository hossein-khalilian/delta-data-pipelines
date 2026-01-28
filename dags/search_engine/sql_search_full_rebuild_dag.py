from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pytz
import requests
import logging
from utils.config import config

from search_engine.utils.utils_of_searchengine import get_cursor, safe_int, age_to_build_year, normalize_property_type

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

def health_check_function(**context):
    try:
        response = requests.get(ENDPOINT_HEALTH, timeout=30)
        if response.status_code == 200:
            logging.info("Search service health check passed | status=%s", response.status_code)
            return
        else:
            logging.error(
                "Health check failed | status=%s | response=%s",
                response.status_code,
                response.text,
            )
            raise RuntimeError("Search service health check failed")
    except Exception as e:
        logging.error("Health check connection failed: %s", str(e))
        raise
    
def extract_function(ti, **context):
    
    health_check_function()
    
    conn, cursor = get_cursor()
    try:
        logging.info("Executing SQL query...")
        cursor.execute(QUERY)
        logging.info("SQL query executed")
        rows = cursor.fetchall()
        row_count = len(rows)
        
        logging.info("Extracted %s rows from database", row_count)
        
        ti.xcom_push(key="extracted_rows", value=rows)
        ti.xcom_push(key="extracted_count", value=row_count)
        
        return None
    finally:
        conn.close()

def transform_function(ti, **context):
    rows = ti.xcom_pull(task_ids="extract_task", key="extracted_rows")
    if not rows:
        logging.warning("No rows to transform")
        ti.xcom_push(key="transformed_properties", value=[])
        ti.xcom_push(key="transformed_count", value=0)
        return None

    tehran_tz = pytz.timezone("Asia/Tehran")
    transformed = []
    
    for row in rows:
        row_dict = dict(row)
        
        normalized_property_type = normalize_property_type(row_dict.get("PropertyTypeId"))
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
            
        build_year = age_to_build_year(safe_int(row_dict.get("age")))

        api_row = {
            "id": int(row_dict.get("Id")),
            "property_type": normalized_property_type,
            "deposit_category": str(row_dict.get("DepositCategoryId") or ""), 
            "user_role_id":int(row_dict.get("UserId") or 13),
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
    
    ti.xcom_push(key="transformed_properties", value=transformed)
    ti.xcom_push(key="transformed_count", value=len(transformed))
    
    return None

def load_function(ti, **context):
    properties = ti.xcom_pull(task_ids="transform_task", key="transformed_properties")
    
    if not properties:
        logging.warning("No data to send")
        return

    total = len(properties)
    successful_batches = 0
    total_batches = (total - 1) // BATCH_SIZE + 1

    logging.info("Total batch count = %s",total_batches)

    for i in range(0, total, BATCH_SIZE):
        batch = properties[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        batch_count = len(batch)

        logging.info("Sending batch %s/%s", batch_num, total_batches)

        try:
            response = requests.post(
                ENDPOINT_UPDATE_ALL,
                json={
                    "properties": batch,
                    "batch_number": batch_num,
                    "total_batches":total_batches
                },
                headers = {
                    "Authorization": f"Bearer {config["search_engine_access_token"]}",
                    "accept": "/"
                },
                timeout=180, 
            )

            if response.ok:
                logging.info("Batch %s sent successfully | count=%s | status=%s",
                             batch_num, batch_count, response.status_code)
                successful_batches += 1
                data = response.json()
                logging.info(f"(response) added count: {data.get('added_count')}")    
                logging.info(f"(response) message : {data.get('message')}") 
                
            else:
                logging.error("Batch %s failed | status=%s | response=%s",
                              batch_num, response.status_code, response.text)
                response.raise_for_status()  # fail 
                
        except requests.exceptions.RequestException as e:
            logging.error("Batch %s request failed: %s", batch_num, str(e))
            raise  # fail 

    logging.info("All batches completed | Total sent: %s properties in %s batches",
                 total, successful_batches)

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