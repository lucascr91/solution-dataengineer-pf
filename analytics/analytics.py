'''ETL process of the analytics service'''
from os import environ
from time import sleep

from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from utils import (
    extract_temp_data_points,
    extract_distances,
    load_data2mysql,
    print_results,
)

print("Waiting for the data generator...")
sleep(20)
print("ETL Starting...")

while True:
    try:
        psql_engine = create_engine(
            environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10
        )
        break
    except OperationalError:
        sleep(0.1)
print("Connection to PostgresSQL successful.")


Session = sessionmaker(bind=psql_engine)
session = Session()
mysql_conn_str = environ["MYSQL_CS"]

print("Extracting data...")
df1 = extract_temp_data_points(session=session)
df2 = extract_distances(session=session)
print("Transforming data...")
df = pd.merge(df1, df2, on=["device_id", "hour"])
print("Loading data...")
load_data2mysql(data=df, connection_string=mysql_conn_str)
print("Printing results...")
print_results()
