import pymssql
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_deposit_by_id(deposit_id: int, conn_params: dict):
    query = """
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
            d.CreatedTime,
            d.ModifiedDate,
            d.MainStreet,
            d.Price,
            d.RentalPrice
        FROM Deposits d
        WHERE d.Id = %s AND d.StatusId = 1247
    ),
    PivotCustomFields AS (
        SELECT
            cfv.DepositId,
            MAX(CASE WHEN cfv.CustomFieldId IN (1117,1125,1133,1141,1149,1155,1158,1159,1174)
                     THEN ISNULL(cfv.Value, cfo.Value) END) AS meter,
            MAX(CASE WHEN cfv.CustomFieldId = 1118 THEN ISNULL(cfv.Value, cfo.Value) END) AS floor,
            MAX(CASE WHEN cfv.CustomFieldId = 1119 THEN ISNULL(cfv.Value, cfo.Value) END) AS rooms,
            MAX(CASE WHEN cfv.CustomFieldId = 1120 THEN ISNULL(cfv.Value, cfo.Value) END) AS age,
            MAX(CASE WHEN cfv.CustomFieldId = 1121 THEN ISNULL(cfv.Value, cfo.Value) END) AS parking,
            MAX(CASE WHEN cfv.CustomFieldId = 1122 THEN ISNULL(cfv.Value, cfo.Value) END) AS warehouse,
            MAX(CASE WHEN cfv.CustomFieldId = 1123 THEN ISNULL(cfv.Value, cfo.Value) END) AS elevator,
            MAX(CASE WHEN cfv.CustomFieldId = 1124 THEN ISNULL(cfv.Value, cfo.Value) END) AS loan
        FROM CustomFieldValues cfv
        LEFT JOIN CustomFieldOptions cfo ON cfv.CustomFieldOptionId = cfo.Id
        GROUP BY cfv.DepositId
    )
    SELECT
        d.Id,
        d.Title,
        dc.Link AS DepositCategoryId,
        bi.Title AS PropertyTypeId,
        d.CityId,
        r.Name AS RegionId,
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
    LEFT JOIN DepositCategories dc ON d.DepositCategoryId = dc.Id
    LEFT JOIN BaseInfos bi ON d.PropertyTypeId = bi.Id
    LEFT JOIN Regions r ON d.RegionId = r.Id
    LEFT JOIN PivotCustomFields p ON d.Id = p.DepositId;
    """

    conn = None
    try:
        conn = pymssql.connect(
            server=conn_params['server'],
            port=conn_params.get('port', 1433),
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database']
        )
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query, (deposit_id,))
        results = cursor.fetchall()
        return results

    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    db_config = {
        "server": os.getenv("SQL_HOST"),
        "port": os.getenv("SQL_PORT"),
        "database": os.getenv("SQL_NAME"),
        "user": os.getenv("SQL_USER"),
        "password": os.getenv("SQL_PASSWORD"),
    }

    deposit_id = 87115

    data = get_deposit_by_id(deposit_id, db_config)

    if data:
        print(f"Number of deposits found: {len(data)}")
        print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    else:
        print("No deposit found")
