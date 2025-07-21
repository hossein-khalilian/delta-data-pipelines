import requests
from pathlib import Path


def upload_files(df):
    url = "https://apicloud.delta.ir/api/v1/"

    token_type = 'Bearer'
    token_user = Path('api_token.txt').read_text()

    header = {
        "accept": "*/*",
        "Authorization": token_type + " " + token_user
    }

    batch_size = 5

    for i in range(0, len(df), batch_size):
        print("Index= ", i, end='')
        hrefs = df[i:i + batch_size]['CoverId'].tolist()

        base_path = "C:/Users/m.khodadadi/Documents/GitHub/divar_data_mapping/data/images/"
        # base_path = "data/images/"

        files = []
        for name in hrefs:
            try:
                file_path = f"{base_path}{name}.jpg"
                files.append(("file", (f"{name}.jpg", open(file_path, "rb"), "image/jpeg")))
            except:
                hrefs.remove(name)

        response = requests.post(url=url + "files", headers=header, files=files)

        if response.status_code == 200:
            data = response.json()

            data_dict = dict(zip(hrefs, data['data']))
            # df_new['CoverId'] = df_new['CoverId'].map(lambda x: data_dict.get(x, x))
            df.loc[:, 'CoverId'] = df['CoverId'].map(lambda x: data_dict.get(x, x))
            print(f"\threfs= {hrefs}\tData= {data['data']}")

        else:
            print('\t', response)