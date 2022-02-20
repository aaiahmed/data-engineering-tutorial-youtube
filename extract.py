#!/usr/bin/env python3

from configparser import ConfigParser
from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine
from airflow.models import Variable
from operator import itemgetter
import pandas as pd
import logging
import os


EXTRACT_LOCATION = "extracted_files"
CONFIG_FILE = "config.ini"


def get_password() -> str:
    password = os.getenv('PASSWORD')
    if password is None:
        return Variable.get('password')
    return password


def read_config() -> ConfigParser:
    config = ConfigParser()
    config.read(filenames=CONFIG_FILE)
    return config


def get_section_config(config: ConfigParser, section: str) -> dict:
    return dict(config.items(section=section))


def get_connection(config: dict) -> Engine:
    host, port, user, db, dbtype, library, driver = \
        itemgetter('host', 'port', 'user', 'db', 'dbtype', 'library', 'driver')(config)
    connection_uri = f"{dbtype}+{library}://{user}:{get_password()}@{host}:{port}/{db}{driver}"
    return create_engine(connection_uri)


def extract_table(con: Engine, schema: str, table: str) -> pd.DataFrame:
    sql = f"select * from {schema}.{table}"
    return pd.read_sql(con=con, sql=sql)


def create_directory(section: str) -> str:
    file_directory = f"{EXTRACT_LOCATION}/{section}"
    os.makedirs(file_directory, exist_ok=True)
    return file_directory


def write_csv(df: pd.DataFrame, file_path: str):
    df.to_csv(path_or_buf=file_path, sep=',', index=False)


def load_table(con: Engine, table: str, file_path: str):
    df = pd.read_csv(filepath_or_buffer=file_path, sep=',')
    df.columns = df.columns.str.lower()
    df.to_sql(con=con, name=table, if_exists="replace", index=False)


def extract_and_load_all_tables(source: str, target: str):
    config = read_config()
    source_config = get_section_config(config=config, section=source)
    source_connection = get_connection(source_config)
    source_schema = source_config['schema']
    source_tables = source_config['tables'].split(',')

    target_config = get_section_config(config=config, section=target)
    target_connection = get_connection(target_config)

    file_directory = create_directory(section=source)

    for table in source_tables:
        df = extract_table(con=source_connection, schema=source_schema, table=table)
        file_path = f"{file_directory}/{table.strip()}.csv"
        write_csv(df=df, file_path=file_path)
        load_table(con=target_connection, table=table.strip(), file_path=file_path)


def extract_tables_from_mysql():
    logging.info("Extracting data from mysql into postgresql")
    extract_and_load_all_tables(source='mysql', target='postgresql')


def extract_tables_from_sqlserver():
    logging.info("Extracting data from sqlserver into postgresql")
    extract_and_load_all_tables(source='sqlserver', target='postgresql')


if __name__ == '__main__':
    extract_tables_from_mysql()
    extract_tables_from_sqlserver()
