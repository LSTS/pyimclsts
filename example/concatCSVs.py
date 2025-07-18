from example.netCDF.utils import *
from example.netCDF.core import *
from datetime import datetime 
from shapely import wkt
from shapely.geometry import Point, Polygon 
import pyimclsts.network as n
import pyimc_generated as pg
import argparse
import os
import sys
import pandas as pd
import plotly.express as px
from example.powerlog import VoltAmpGatherer
from pathlib import Path
import matplotlib.pyplot as plt

rejected_files = ['/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250630/083525_cmd-caravel/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250630/083651/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250630/084210/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250630/084931/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250630/091138/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250630/093758/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250701/195622/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250703/213119/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250705/133217/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250705/141632/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250706/033507/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250708/050423/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250708/093515/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250708/183946/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250709/160022/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250709/234619/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250710/081528_cmd-caravel/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250710/121530_cmd-caravel/Data.lsf', 
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250710/121531/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250710/121550/Data.lsf',
                  '/home/ruben/workspace/2025-06-30_apdl_out_caravel/logs/caravel/20250710/124447_cmd-caravel/Data.lsf']

lsf_files = gather_log_paths('/mnt/sdb1/2025-06-30_apdl_out_caravel/logs/caravel')


# Build a set of (date, filename) stems to remove
remove_stems = {
    "/".join(Path(p).parts[-3:-1])  # e.g., "20250710/121530_cmd-caravel"
    for p in rejected_files
}

# Filter mnt_paths
filtered = [
    p for p in lsf_files
    if "/".join(Path(p).parts[-3:-1]).replace(".gz", "") not in remove_stems
]

# Show result
print(f"Filtered ({len(filtered)} paths remain):")
for path in filtered:
    print(path)

# Replace each part 
converted = [ 
              str(Path(p).with_name("mra") / "Data.xlsx")
              for p in filtered
            ]

print(f"Converted ({len(converted)} paths): ")

for path in converted:
   print(path)

  
# Check if paths exist
xlsx_paths = [Path(p) for p in converted if Path(p).exists()]

df_all = pd.concat([pd.read_excel(p) for p in xlsx_paths], ignore_index=True)

# Show basic info 
print(f"Loaded {len(df_all)} rows from {len(xlsx_paths)} files.")

print(df_all.head())

# Ensure TIME is converted from epoch to datetime
df_all['DATETIME'] = pd.to_datetime(df_all['TIME'], unit='s')
df_all = df_all.sort_values('DATETIME')

# Compute power in watts
df_all['POWER'] = df_all['VOLT'] * df_all['CURR']

df_all.to_csv("caravel_power_log.csv", index=False)

# Plot 
plt.figure(figsize=(14,6))
plt.plot(df_all['DATETIME'], df_all['POWER'], linewidth=0.5)

plt.title('Power Over Time')
plt.xlabel('Time')
plt.ylabel('Power (W)')
plt.grid(True)
plt.tight_layout()
plt.show()



