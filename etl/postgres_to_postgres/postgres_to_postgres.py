#!/usr/bin/env python3
import os
from os.path import join, dirname
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import pandas.io.sql as psql

dotenv_path = join(dirname(__file__), '../.env')
load_dotenv(dotenv_path)

input_host = os.environ.get("IN_DB_HOST")
input_user = os.environ.get("IN_DB_USER")
input_pass = os.environ.get("IN_DB_PASS")
input_db = os.environ.get("IN_DB_DB")

output_host = os.environ.get("OUT_DB_HOST")
output_user = os.environ.get("OUT_DB_USER")
output_pass = os.environ.get("OUT_DB_PASS")
output_db = os.environ.get("OUT_DB_DB")
output_schema = os.environ.get("OUT_DB_SCHEMA")

in_conn = create_engine("postgresql+psycopg2://" + input_user + ":" + quote_plus(input_pass) + "@" + input_host + "/" + input_db)

out_conn = create_engine("postgresql+psycopg2://" + output_user + ":" + quote_plus(output_pass) + "@" + output_host + "/" + output_db)

#######################################
############# EXAMPLE 1 ###############
#read the input sql statement from the file
sql_command = Path(os.path.dirname(os.path.abspath(__file__)) + '/sql/example1.sql').read_text()

#read the data from the input database
data = pd.read_sql(sql_command, in_conn)

#delete old data
sql_command = "truncate table " + output_schema + ".example1"
out_conn.execute(sql_command)

#write the data to the output database
data.to_sql('example1', con=out_conn, schema=output_schema, if_exists='append', index=False, method='multi')


#######################################
############# EXAMPLE 2 ###############
#read the input sql statement from the file
sql_command = Path(os.path.dirname(os.path.abspath(__file__)) + '/sql/example2.sql').read_text()

#read the data from the input database
data = pd.read_sql(sql_command, in_conn)

#delete old data
sql_command = "truncate table " + output_schema + ".example2"
out_conn.execute(sql_command)

#write the data to the output database
data.to_sql('example2', con=out_conn, schema=output_schema, if_exists='append', index=False, method='multi')

##################################
########## Log Success ###########
#delete old logs
sql_command = "delete from " + output_schema + ".etl_log where updated < now() - interval '30 days'"
out_conn.execute(sql_command)

#add new log
sql_command = "insert into " + output_schema + ".etl_log default values"
out_conn.execute(sql_command)

print("Success!")