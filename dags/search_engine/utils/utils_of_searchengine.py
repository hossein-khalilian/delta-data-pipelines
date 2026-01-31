from contextlib import contextmanager
from datetime import datetime
import logging
import pytz
import requests
import pymssql

from utils.config import config

# CONSTANTS
TEHRAN_TZ = pytz.timezone("Asia/Tehran")

DB_CONFIG = {
    "server": config["sql_host"],
    "port": config["sql_port"],
    "database": config["sql_name"],
    "user": config["sql_user"],
    "password": config["sql_password"],
}

DEFAULT_BATCH_SIZE = 200

# DATABASE

@contextmanager
def get_db_cursor(as_dict: bool = True):
    """
    Safe SQL Server cursor context manager.
    Ensures connection is always closed and transaction-safe.
    """
    conn = pymssql.connect(
        server=DB_CONFIG["server"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        login_timeout=30,
        timeout=300,
    )
    try:
        cursor = conn.cursor(as_dict=as_dict)
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# TIME & DATE

def iran_datetime_to_utc_iso(dt: datetime | None) -> str | None:
    """
    Convert naive Iran datetime (DB) to UTC ISO string.
    """
    if not dt:
        return None

    iran_dt = TEHRAN_TZ.localize(dt)
    return iran_dt.astimezone(pytz.UTC).isoformat()

# DATA NORMALIZATION

def safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def age_to_build_year(age: int | None) -> int | None:
    """
    Convert property age to approximate Jalali build year.
    """
    if age is None:
        return None

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


def normalize_property_type(property_type: str | None) -> str | None:
    if not property_type:
        return None

    pt = str(property_type).strip()

    if "مشارکت" in pt:
        return None
    if "زمین" in pt or "صنعتی" in pt:
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

# TRANSFORMATION

def build_property_payload(row: dict, status_override: str | None = None) -> dict | None:
    """
    Build unified API payload from DB row.
    Used by both incremental & full rebuild DAGs.
    """
    normalized_property_type = normalize_property_type(row.get("PropertyTypeId"))
    if normalized_property_type is None:
        return None

    return {
        "id": int(row.get("Id")),
        "property_type": normalized_property_type,
        "deposit_category": str(row.get("DepositCategoryId") or ""),
        "user_role_id": int(row.get("UserroleId") or row.get("UserId") or 13),
        "city_id": int(row.get("CityId") or 0),
        "title": str(row.get("Title") or ""),
        "created_time": iran_datetime_to_utc_iso(row.get("CreatedTime")),
        "modified_time": iran_datetime_to_utc_iso(row.get("ModifiedDate")),
        "region": str(row.get("RegionId") or ""),
        "price": int(row.get("Price") or 0),
        "rental_price": int(row.get("RentalPrice") or 0),
        "meter": safe_int(row.get("meter")),
        "floor": str(row.get("floor") or ""),
        "rooms": str(row.get("rooms") or ""),
        "age": age_to_build_year(safe_int(row.get("age"))),
        "parking": bool(row.get("parking")),
        "warehouse": bool(row.get("warehouse")),
        "elevator": bool(row.get("elevator")),
        "loan": bool(row.get("loan")),
        "description": str(row.get("Description") or ""),
        "status": status_override
        or ("active" if row.get("StatusId") == 1247 else "inactive"),
    }


# API & BATCH SENDER

def check_search_health(health_endpoint: str, timeout: int = 30):
    response = requests.get(health_endpoint, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"Search service unhealthy | status={response.status_code}"
        )


def send_batches(
    *,
    endpoint: str,
    items: list[dict],
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout: int = 60,
):
    """
    Generic batch sender with logging & failure handling.
    """
    if not items:
        logging.info("No data to send")
        return

    total = len(items)
    total_batches = (total - 1) // batch_size + 1
    token = config["search_engine_endpoint_access_token"]

    logging.info("Sending %s items in %s batches", total, total_batches)

    for i in range(0, total, batch_size):
        batch = items[i : i + batch_size]
        batch_num = i // batch_size + 1

        logging.info("Sending batch %s/%s | count=%s",
                     batch_num, total_batches, len(batch))

        response = requests.post(
            endpoint,
            json={
                "properties": batch,
                "batch_number": batch_num,
                "total_batches": total_batches,
            },
            headers={
                "Authorization": f"Bearer {token}",
                "accept": "/",
            },
            timeout=timeout,
        )

        if not response.ok:
            logging.error(
                "Batch %s failed | status=%s | response=%s",
                batch_num,
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        data = response.json()
        logging.info(
            "Batch %s success | added_count=%s | message=%s",
            batch_num,
            data.get("added_count"),
            data.get("message"),
        )
