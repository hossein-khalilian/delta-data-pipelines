import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def fa_to_en(text):
    new_text = ''
    for c in text:
        chr_code = ord(c)
        new_text += chr(chr_code - 1728) if (chr_code >= 1776 and chr_code <= 1785) else c
    return new_text

def fix_floor(x):
    if type(x['Floor']) != int and type(x['Floor']) != float:
        if 'همکف' in x['Floor']:
            x['Floor'] = 'همکف'
        if 'از' in x['Floor']:
            x['Floor'] = x['Floor'].split('از')[0]
    return x

def date_and_street_extractor(x):
    date = x['CreatedTime']
    if date.split()[1] == 'روز':
        days_ago = int(fa_to_en(date.split()[0]))
    elif date.split()[1] == 'ماه':
        days_ago = int(fa_to_en(date.split()[0])) * 30
    elif date.split()[1] == 'هفته':
        days_ago = int(fa_to_en(date.split()[0])) * 7
    else:
        days_ago = 0
    x['CreatedTime'] = datetime.now() - timedelta(days=days_ago)
    if '،' in date:
        x['MainStreet'] = date.split('، ')[1]
    else:
        x['MainStreet'] = ' '
    return x

def fix_url(x):
    x = x[-8:]
    return x


city = input('Enter file name: ')
df = pd.read_excel(f'C:/Users/m.khodadadi/Documents/GitHub/divar_data_mapping/data/{city}.xlsx')
city_id = int(input(f'Enter {city} id: '))

df = df.dropna(subset=['Rooms'])
df = df.dropna(subset=['Parking'])
df = df.fillna(value=0)
df = df.drop(columns=['count'])
df['category'].unique()

df = df.rename(columns={'description': 'Description'})
df = df.rename(columns={'category': 'DepositCategoryId'})
df = df.rename(columns={'type': 'PropertyTypeId'})
df = df.rename(columns={'time_region': 'CreatedTime'})
df = df.rename(columns={'url': 'CoverId'})

df = df.apply(fix_floor, axis=1)

floor_mapping = {
    "زیرهمکف": 1129,
    "همکف": 1130,
    1: 1131,
    2: 1132,
    3: 1133,
    4: 1134,
    5: 1135,
    6: 1136,
    7: 1137,
    8: 1138,
    9: 1139,
    10: 1140,
    11: 1141,
    12: 1142,
    13: 1143,
    14: 1144,
    15: 1145,
    16: 1146,
    17: 1147,
    18: 1148,
    19: 1149,
    20: 1150,
    "بالا تر از 20": 1151
}

df['Floor'] = df['Floor'].map(floor_mapping)

elevator_mapping = {
    0: 1091,
    1: 1090
}
df['Elevator'] = df['Elevator'].map(elevator_mapping)

df['Age'] = 1404 - df['Age']

age_mapping = {
    1404: 2214,
    1403: 1098,
    1402: 1099,
    1401: 1100,
    1400: 1101,
    1399: 1102,
    1398: 1103,
    1397: 1104,
    1396: 1105,
    1395: 1106,
    1394: 1107,
    1393: 2224,
    1392: 1108,
    1391: 1109,
    1390: 1110,
    1389: 1111,
    1388: 1112,
    1387: 1113,
    1386: 1114,
    1385: 1115,
    1384: 1116,
    1383: 1117,
    1382: 1118,
    1381: 1119,
    1380: 1120,
    1379: 1121,
    1378: 2234,
    1377: 4883,
    1376: 1122,
    1375: 1123,
    1374: 1124,
    1373: 4884,
    1372: 1126,
    'بیش از 30 سال': 1128
}
df['Age'] = df['Age'].map(age_mapping)

loan_mapping = {
    0: 1097,
    1: 1096
}
df['Loan'] = df['Loan'].map(loan_mapping)

parking_mapping = {
    0: 1095,
    1: 1094
}
df['Parking'] = df['Parking'].map(parking_mapping)

rooms_mapping = {
    'بدون اتاق': 1152,
    1: 1153,
    2: 1154,
    3: 1155,
    4: 1156,
    5: 1157
}
df['Rooms'] = df['Rooms'].map(rooms_mapping)

warehouse_mapping = {
    0: 1093,
    1: 1092
}
df['WareHouse'] = df['WareHouse'].map(warehouse_mapping)

category_mapping = {
 'فروش مسکونی': 6,
 'اجارهٔ مسکونی': 7
}
df['DepositCategoryId'] = df['DepositCategoryId'].map(category_mapping)

df['PropertyTypeId'] = 1257

problem = False
for col in ['Age', 'Elevator', 'Floor', 'Loan', 'Parking', 'Rooms', 'WareHouse', 'DepositCategoryId', 'PropertyTypeId']:
    print(f"Unique values of {col}: {df[col].unique()}\n")
    for t in df[col].unique():
        if np.isnan(t):
            problem = True
            break

if problem:
    raise Exception("Values went wrong!")

df = df.apply(date_and_street_extractor, axis=1)

df['url'] = df['url'].apply(fix_url)

df['ModifiedDate'] = df['CreatedTime']
df['Disabled'] = 0
df['ModifiedBy'] = None
df['id'] = df.index + 90001
df['OldId'] = None
df['StatusId'] = 1247
df['CityId'] = city_id
df['RegionId'] = None
df['IsDeleted'] = 0
df['phone'] = df['phone'].map(fa_to_en)
df['phone'] = df['phone'].astype('string')
df['UserId'] = df['phone']
df['Coordinates'] = None
df['CoverId'] = None
df['CreatedBy'] = df['phone']
df['Code'] = df['id']

new_order = ['id', 'Title', 'Description', 'DepositCategoryId', 'PropertyTypeId', 'OldId', 'StatusId', 'UserId', 'CityId', 'CoverId', 'url', 'RegionId', 'Coordinates', 'CreatedTime', 'ModifiedDate', 'CreatedBy', 'ModifiedBy', 'Disabled', 'IsDeleted', 'MainStreet', 'Price', 'RentalPrice', 'Code']
df_new = df[new_order]

df_custom_field = pd.DataFrame(columns=[
    'CustomFieldId',
    'CustomFieldOptionId',
    'DepositId',
    'Value',
    'CreatedTime',
    'ModifiedDate',
    'CreatedBy',
    'ModifiedBy',
    'Disabled',
    'IsDeleted',
    'CityId'
])

custom_fields = {
    1117: 'Meter',
    1118: 'Floor',
    1119: 'Rooms',
    1120: 'Age',
    1121: 'Parking',
    1122: 'WareHouse',
    1123: 'Elevator',
    1124: 'Loan'
}

new_rows = []
for index, row in df.iterrows():
    for c in [1117, 1118, 1119, 1120, 1121, 1122, 1123, 1124]:
        if c != 1117:
            CustomFieldOptionId = row[custom_fields[c]]
            Value = None
        else:
            CustomFieldOptionId = None
            Value = row['Meter']

        new_rows.append({
            'CustomFieldId': c,
            'CustomFieldOptionId': CustomFieldOptionId,
            'DepositId': row['id'],
            'Value': Value,
            'CreatedTime': row['CreatedTime'],
            'ModifiedDate': row['ModifiedDate'],
            'CreatedBy': None,
            'ModifiedBy': None,
            'Disabled': 0,
            'IsDeleted': 0,
            'CityId': None
        })

df_custom_field = pd.concat([df_custom_field, pd.DataFrame(new_rows)], ignore_index=True)

df_custom_field['CustomFieldId'] = df_custom_field['CustomFieldId'].astype('int')
df_custom_field['DepositId'] = df_custom_field['DepositId'].astype('int')
df_custom_field['Disabled'] = df_custom_field['Disabled'].astype('int')
df_custom_field['IsDeleted'] = df_custom_field['IsDeleted'].astype('int')

df_new.to_csv(f'{city}_Deposits.csv', index=False)
df_custom_field.to_csv(f'{city}_CustomFieldValues.csv', index=False)