#!/usr/bin/env python3

import logging
from sqlalchemy.engine.base import Engine
from extract import read_config, get_section_config, get_connection

target = 'postgresql'


def execute_query(con: Engine, query: str):
    con.execute(query)


def transform_employee():
    logging.info("Transforming into dim_employee in postgresql")
    config = read_config()
    target_config = get_section_config(config=config, section=target)
    con = get_connection(target_config)
    execute_query(con=con, query="call public.transform_dim_employees()")


if __name__ == '__main__':
    transform_employee()



