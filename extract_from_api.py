#!/usr/bin/env python3

import logging
import os
import requests
import pandas as pd
from sqlalchemy.engine.base import Engine
from extract import read_config, get_section_config, get_connection
from airflow.models import Variable


def get_api_key() -> str:
    api_key = os.getenv('API_KEY')
    if api_key is None:
        return Variable.get('api_key')
    return api_key

url = f"http://dataservice.accuweather.com/currentconditions/v1/topcities/150?apikey={get_api_key()}"
target = 'postgresql'
table = 'current_conditions'


def get_date_from_api(url: str) -> pd.DataFrame:
    r = requests.get(url=url)
    data = r.json()
    return pd.json_normalize(data=data, sep='_')


def load_table(con: Engine, df: pd.DataFrame, table: str):
    df.to_sql(con=con, name=table, if_exists='append')


def extract_from_api():
    logging.info("Extractive data from api into postgresql")
    df = get_date_from_api(url=url)
    config = read_config()
    target_config = get_section_config(config=config, section=target)
    con = get_connection(target_config)
    load_table(con=con, table=table, df=df)


if __name__ == '__main__':
    extract_from_api()



