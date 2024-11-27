from example.netCDF.utils import *
from example.netCDF.core import *
from geopy.distance import geodesic
from datetime import datetime
import plotly.express as px 
import pandas as pd 


"""
  This script takes an existing .nc file and changes its metadata based on json files.
  You can edit these json files to able to edit metadata on said files

"""
file_path = '/mnt/sdb1/pyimclsts/outdata/lauv-xplore-2_2024_10_29.nc'

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

for key, value in xrds.attrs.items():
    if key == 'time_coverage_start':
        start_time = value
    if key == 'time_coverage_end':
        end_time = value

    print(f"{key}: {value}")

lat = xrds.coords['LATITUDE']
lon = xrds.coords['LONGITUDE']

total_distance = 0.0

for i in range(1, len(lat)):

    point1 = (lat[i - 1], lon[i - 1])
    point2 = (lat[i], lon[i])
    distance = geodesic(point1, point2).kilometers  # You can use .miles if needed
    total_distance += distance

# Define the datetime format
fmt = "%Y-%m-%d %H:%M:%S.%f"

# Convert strings to datetime objects
start_time = datetime.strptime(start_time, fmt)
end_time = datetime.strptime(end_time, fmt)

# Calculate the difference between the two times
time_diff = end_time - start_time

# Extract the difference in hours, minutes, and seconds
hours = time_diff.days * 24 + time_diff.seconds // 3600
minutes = (time_diff.seconds % 3600) // 60
seconds = time_diff.seconds % 60 + time_diff.microseconds / 1e6


print("Total Distance: {}".format(total_distance))
print("Hours: {} Minutes: {} Seconds: {}".format(hours, minutes, seconds))

color_scale = [(0, 'orange'), (1, 'red')]

concat_data = pd.DataFrame(
    {
        'lat':lat,
        'lon':lon
    }
)

fig = px.scatter_mapbox(concat_data, 
                        lat='lat',
                        lon='lon',
                        color_continuous_scale=color_scale,
                        zoom=8, 
                        height=800,
                        width=800)

fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()


xrds.to_netcdf('new_file.nc')






