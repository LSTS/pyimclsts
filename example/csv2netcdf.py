from example.netCDF.utils import *
from example.netCDF.core import *
import pandas as pd

"""
  This script turns a csv file into a .nc file. If something went wrong with the transformation of your final 
  csv file or you to want to edit something in it, you can do so, and then use this script to turn it into a 
  netcdf file. 

"""

"""

#file_path = '/home/ruben/Workspace/pyimclsts/outdata/WHISTLE/lauv-xplore-3_2025_10_13'
file_path = '/home/ruben/Workspace/pyimclsts/outdata/WHISTLE/lauv-xplore-5_2025_10_13'

PSA_HIGH_THRESHOLD = 4
TEMP_HIGH_THRESHOLD = 4

## Small fix in some data
df = pd.read_excel(file_path + ".xlsx")


## Test 1 is to filter out values of salinity between 14 and 40
df = df[df["PSAL"] >= 15.0]
df = df[df["PSAL"] <= 40.0]
df = df[df["TEMP"] >= 8]
df = df[df["TEMP"] <= 30]

df = df.sort_values("TIME")

## Test 2 is a rolling window mean of 3
df["REF_PSAL"] = df["PSAL"].rolling(window=3, center=True).mean()
df["REF_TEMP"] = df["TEMP"].rolling(window=3, center=True).mean()

# Find rows that exceed the absolute differente of 4
mask = (
    (df["PSAL"] - df["REF_PSAL"]).abs() > PSA_HIGH_THRESHOLD
) | (
    (df["TEMP"] - df["REF_TEMP"]).abs() > TEMP_HIGH_THRESHOLD
)

# Seperate the dataframes
outliers_df = df[mask].copy()

clean_df = df[~mask].copy()
clean_df.drop(columns=['REF_PSAL'])
clean_df.drop(columns=['REF_TEMP'])

print(outliers_df)

clean_df.to_excel("new.xlsx")

"""

file_path = '/home/ruben/Workspace/pyimclsts/outdata/PICO_LOOP/lauv-xplore-5_2025_07_03'

# Now we create the actual netCDF file based on the name of the system
netCDF = netCDFExporter(file_path)
netCDF.build_netCDF()
netCDF.replace_json_metadata()
netCDF.to_netCDF()
