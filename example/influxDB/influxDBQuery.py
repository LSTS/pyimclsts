from influxdb_client_3 import (
  InfluxDBClient3, InfluxDBError, Point, WritePrecision,
  WriteOptions, write_client_options)

import pandas as pd
import argparse
import os
import matplotlib.pyplot as plt
import numpy as np
    
## MACROS
PSA_HIGH_THRESHOLD = 4
INFLUX_URL = "http://localhost:8181"
TOKEN = "apiv3_0dBzgcClxleVzVrjECsEis0f7KRGQC-XbnIShOyMT_8HZigx6mbRgGXmCPe6HCOrV8VWrGOc099zyTSG1A_6ig"
DATABASE = "WHISTLE"
ORG = "LSTS"

host = os.getenv('INFLUX_HOST', INFLUX_URL)
token = os.getenv('INFLUX_TOKEN', TOKEN)
database = os.getenv('INFLUX_DATABASE', DATABASE)
org = os.getenv("INFLUX_ORG", ORG)

client = InfluxDBClient3(
    host=host,
    token=token, 
    org=org, 
    database=database
)

# Example: get the last 100 points from measurement "WHISTLE"
query = """
SELECT "LATITUDE", "LONGITUDE", "time"
FROM 'WHISTLE_6_test' 
WHERE time >= '2025-10-15T11:59:00.000Z' AND time <= '2025-10-16T10:30:00.000Z'
"""

df = client.query(query)
df = df.to_pandas()

print(df)