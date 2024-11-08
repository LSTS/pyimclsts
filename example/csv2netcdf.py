from example.netCDF.utils import *
from example.netCDF.core import *

"""
  This script turns a csv file into a .nc file. If something went wrong with the transformation of your final 
  csv file or you to want to edit something in it, you can do so, and then use this script to turn it into a 
  netcdf file. 

"""

file_path = '~/workspace/pyimclsts/outdata/lauv-xplore-2_2024_10_29'

# Now we create the actual netCDF file based on the name of the system
netCDF = netCDFExporter(file_path)
netCDF.build_netCDF()
netCDF.replace_json_metadata()
netCDF.to_netCDF()