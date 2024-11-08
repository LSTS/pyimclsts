from example.netCDF.utils import *
from example.netCDF.core import *


"""
  This script takes an existing .nc file and changes its metadata based on json files.
  You can edit these json files to able to edit metadata on said files

"""
file_path = '~/workspace/pyimclsts/outdata/lauv-xplore-5_2024_10_29.nc'

# Create a base xarray dataset
xrds = xr.open_dataset(file_path, engine="netcdf4")
# Load global attributes from json
with open('metadata/global_attrs.json', 'r') as f:
    global_attrs = json.load(f)
# Load coordinates attributes from json
with open('metadata/coor_attrs.json', 'r') as f:
    coor_attrs = json.load(f)
# Load data attributes from json
with open('metadata/var_attrs.json', 'r') as f:
    data_attrs = json.load(f)

xrds.attrs = global_attrs

xrds.to_netcdf('new_file.nc')






