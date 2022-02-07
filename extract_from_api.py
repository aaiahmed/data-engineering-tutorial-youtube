#!/usr/bin/env python3
import json
import os
import requests
import pandas as pd
from sqlalchemy.engine.base import Engine
from extract import read_config, get_section_config, get_connection


API_KEY = os.getenv('API_KEY')
PASSWORD = os.getenv('PASSWORD')

url = f"http://dataservice.accuweather.com/currentconditions/v1/topcities/150?apikey={API_KEY}"
target = 'postgresql'
table = 'current_conditions'


def get_date_from_api(url: str) -> pd.DataFrame:
    r = requests.get(url=url)
    data = r.json()
    return pd.json_normalize(data=data, sep='_')


def load_table(con: Engine, df: pd.DataFrame, table: str):
    df.to_sql(con=con, name=table, if_exists='append')


if __name__ == '__main__':
    df = get_date_from_api(url=url)

    config = read_config()
    target_config = get_section_config(config=config, section=target)
    con = get_connection(target_config)
    load_table(con=con, table=table, df=df)



