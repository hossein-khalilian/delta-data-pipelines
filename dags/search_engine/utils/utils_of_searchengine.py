from utils.config import config
import pymssql
from datetime import datetime

ENDPOINT_UPDATE_ALL = f"{config['search_endpoint_url']}/update-all-properties"
ENDPOINT_HEALTH = f"{config['search_endpoint_url']}/health"
BATCH_SIZE = 200

DB_CONFIG = {
    "server": config["sql_host"],
    "port": config["sql_port"],
    "database": config["sql_name"],
    "user": config["sql_user"],
    "password": config["sql_password"],
}

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