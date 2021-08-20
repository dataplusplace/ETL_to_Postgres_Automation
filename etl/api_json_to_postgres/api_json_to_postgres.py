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
import base64 as b64
import http.client
import datetime
import json

dotenv_path = join(dirname(__file__), '../.env')
load_dotenv(dotenv_path)

input_user = os.environ.get("API_USER")
input_pass = os.environ.get("API_PASS")

output_host = os.environ.get("OUT_DB_HOST")
output_user = os.environ.get("OUT_DB_USER")
output_pass = os.environ.get("OUT_DB_PASS")
output_db = os.environ.get("OUT_DB_DB")
output_schema = os.environ.get("OUT_DB_SCHEMA")

#delete data from previous 7 days
today = datetime.date.today()
start_day = today - datetime.timedelta(days = 7)
end_day = today - datetime.timedelta(days = 1)

sql_command = "delete from " + output_schema + ".duration " + "where epoch_hr_ms >= extract(epoch from '" + \
    start_day.strftime('%Y-%m-%d') + " 00:00:00'::timestamp at time zone 'America/New York') * 1000"
out_conn = create_engine("postgresql+psycopg2://" + output_user + ":" + quote_plus(output_pass) + "@" + output_host + "/" + output_db)
out_conn.execute(sql_command)

#pull new data via api
in_conn = http.client.HTTPSConnection("api.apihost.com")

encoded = b64.b64encode(bytes(input_user + ":" + input_pass, encoding="ascii")).decode("ascii")
headers = {
    'Cache-Control': 'no-cache',
    'Authorization': 'Basic %s' % encoded
    }

api = "/metrics/1.0/metrics.json"
query = f"?metrics=viewer_hours&filter_ids=11111&start_day={start_day}&end_day={end_day}"
path = api + query
in_conn.request("GET", path, headers=headers)

res = in_conn.getresponse()
api_data = res.read()
json_data = json.loads(api_data.decode('utf-8'))

#convert json to sql readable dataframe

df_timestamps = pd.json_normalize(data=json_data['viewer_hours'], record_path='timestamps')
df_viewership = pd.json_normalize(data=json_data['viewer_hours'], record_path=['filters','11111'])

df_merge = df_timestamps.merge(df_viewership, left_index=True, right_index=True)
df_insert = df_merge.rename(columns={"0_x": "epoch_hr_ms", "0_y": "duration_hrs"})

#write the data to the output database
df_insert.to_sql('duration', con=out_conn, schema=output_schema, if_exists='append', index=False, method='multi')

print("Success!")
