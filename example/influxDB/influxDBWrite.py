from example.netCDF.utils import *
from example.netCDF.core import *

from influxdb_client_3 import (
  InfluxDBClient3, InfluxDBError, Point, WritePrecision,
  WriteOptions, write_client_options)

import pandas as pd
import argparse
import os

## MACROS
PSA_HIGH_THRESHOLD = 4
INFLUX_URL = "http://localhost:8181"
DATABASE = "WHISTLE"

host = os.getenv('INFLUX_HOST', INFLUX_URL)
database = os.getenv('INFLUX_DATABASE', DATABASE)

# Callback methods receive the configuration and data sent in the request.
def success(self, data: str):
    print(f"Successfully wrote batch: data: {data} \n")

def error(self, data: str, exception: InfluxDBError):
    print(f"Failed writing batch: config: {self}, data: {data} due: {exception} \n ")

def retry(self, data: str, exception: InfluxDBError):
    print(f"Failed retry writing batch: config: {self}, data: {data} retry: {exception} \n")

# Configure option for batch The current temperature is {{ temperature }} °C.writing
write_options = WriteOptions(batch_size=500,
                                    flush_interval=10_000,
                                    jitter_interval=2_000,
                                    retry_interval=5_000,
                                    max_retries=5,
                                    max_retry_delay=30_000,
                                    exponential_base=2)

# Create an options dict that sets callbacks and WriteOptions.
wco = write_client_options(success_callback=success,
                          error_callback=error,
                          retry_callback=retry,
                          write_options=write_options)

if __name__ == '__main__':

    # Parser for command line
    parser = argparse.ArgumentParser(description="Process arguments for Concatenation Script")
    
    # Path to the mission argument
    parser.add_argument('-p', '--mission_csv_path', type=str, default=os.getcwd(),
                        help="Specify path to the log csv. Preset is your current location")


    # Parse the argument and save it 
    args = parser.parse_args()
    csv_path= args.mission_csv_path

    # Get the current directory
    current_dir = os.getcwd()

    # Token is saved under this file
    with open(current_dir + "/example/influxDB/tokens/influx_token.txt") as f:
        token = f.read().strip()

    ## Small fix in some data
    df = pd.read_excel(current_dir + "/" + csv_path)

    # For now, lets keep the location and vehicle state data in a seperate dataframe and perphaps in a seperate table altogether
    # Build a new dataframe with only relevant data 
    """
    selected_columns = ["TIME", "LATITUDE", "LONGITUDE", "DEPH", "ROLL", "PTCH", "HDNG", "APDA"]
    environment_df = df[selected_columns].copy()
    environment_df["TIME"] = pd.to_datetime(environment_df["TIME"], unit='s')
    environment_df = environment_df.set_index("TIME")
    # Fill a column with
    environment_df['vehicle'] = "lauv-xplore-5"

    selectedColumns2 =  ["TIME","TEMP", "CNDC", "SVEL", "PSAL", "VOLT", "SPEED"]
    df_2 = df[selectedColumns2].copy()
    df_2["TIME"] = pd.to_datetime(df_2["TIME"], unit="s")
    df_2 =  df_2.set_index("TIME")
    df_2['vehicle'] = "lauv-xplore-5"
    """

    ts = "2025-10-15 12:00:00"
    
    selected_columns = ["TIME", "LATITUDE"]
    environment_df = df[selected_columns].copy()
    environment_df["TIME"] = pd.to_datetime(environment_df["TIME"], unit='s')
    environment_df = environment_df.set_index("TIME")
    print(environment_df)
    environment_df = environment_df[:ts]
    # Fill a column with
    environment_df['vehicle'] = "lauv-xplore-5"

    print(environment_df)

    selectedColumns2 =  ["TIME","LONGITUDE"]
    df_2 = df[selectedColumns2].copy()
    df_2["TIME"] = pd.to_datetime(df_2["TIME"], unit="s")
    df_2 =  df_2.set_index("TIME")
    df_2['vehicle'] = "lauv-xplore-5"
    df_2 = df_2[ts:]


    # Write the value into the Database
    with InfluxDBClient3(host=host, token=token, database=database, write_client_options=wco) as client:

        client.write(org="LSTS", record=environment_df, data_frame_measurement_name="WHISTLE_6_test", data_frame_tag_columns=["vehicle"])
        #client.write(org="LSTS", record=df_2, data_frame_measurement_name="WHISTLE_6_test", data_frame_tag_columns=["vehicle"])









    

