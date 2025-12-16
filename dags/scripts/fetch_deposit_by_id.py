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
            port=conn_params.get['port'],
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
