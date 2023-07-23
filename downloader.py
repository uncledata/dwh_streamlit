import requests
import os
import pandas as pd

def get_monthly_data(mth:str) -> None:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{mth}.parquet"
    r = requests.get(url)
    with open(f"{os.getcwd()}/src/yellow_taxi_{mth}.parquet", "wb") as f:
        f.write(r.content)
    

if __name__ == '__main__':

    lst = [mth.strftime("%Y-%m") for mth in pd.date_range(start='1/1/2020', end='1/05/2023', freq='M')]
    [get_monthly_data(mth) for mth in lst]