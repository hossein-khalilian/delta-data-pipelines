from utils.config import config

CONFIG_PATH = "/opt/airflow/dags/web_scraping/websites.yaml"

with open(CONFIG_PATH, "r") as f:
    yaml_config = yaml.safe_load(f)

site_names = [site["name"] for site in yaml_config["websites"]]

MONGO_URI = config["mongo_uri"]
MONGO_DB = config["mongo_db"]

#.yaml
COLLECTIONS = config["mongo_collection"] 
MONGO_COLLECTION = [f"{name}-{COLLECTIONS}" for name in site_names]


# MinIO configuration
MINIO_ENDPOINT = config["minio_endpoint"]
MINIO_ACCESS_KEY = config["minio_access_key"]
MINIO_SECRET_KEY = config["minio_secret_key"]
MINIO_BUCKET = config["minio_bucket"]