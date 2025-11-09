from pathlib import Path

import pandas as pd
import requests


def upload_files(df, path):
    url = "https://apicloud.delta.ir/api/v1/"

    token_type = "Bearer"
    token_user = Path("api_token.txt").read_text()

    header = {"accept": "*/*", "Authorization": token_type + " " + token_user}

    batch_size = 5

    for i in range(0, len(df), batch_size):
        print("Index= ", i, end="")
        hrefs = df[i : i + batch_size]["CoverId"].tolist()

        files = []
        for name in hrefs:
            try:
                file_path = f"{path}{name}.jpg"
                files.append(
                    ("file", (f"{name}.jpg", open(file_path, "rb"), "image/jpeg"))
                )
            except:
                hrefs.remove(name)

        response = requests.post(url=url + "files", headers=header, files=files)

        if response.status_code == 200:
            data = response.json()
            data_dict = dict(zip(hrefs, data["data"]))
            df.loc[:, "CoverId"] = df.apply(
                lambda row: data_dict.get(row["url"], row["CoverId"]), axis=1
            )
            print(f"\threfs= {hrefs}\tData= {data['data']}")

        else:
            print("\t", response)
    df["CoverId"] = df["CoverId"].astype("int")
    return df


city = input("Enter file name: ")
dataframe = pd.read_csv(f"{city}_Deposits.csv")
# dataframe = upload_files(dataframe, city)
dataframe.to_csv(f"{city}_Deposits.csv", index=False)
