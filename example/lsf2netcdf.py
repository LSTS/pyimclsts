from example.netCDF.utils import *
from example.netCDF.core import *
from datetime import datetime 
from shapely import wkt
from shapely.geometry import Point, Polygon 
import shutil
import pyimclsts.network as n
import pyimc_generated as pg
import argparse
import os
import sys
import pandas as pd
import plotly.express as px

if __name__ == '__main__':

    # Parser for command line
    parser = argparse.ArgumentParser(description="Process arguments for Concatenation Script")

    # Minimum time argument
    parser.add_argument('-t','--min_time', type=int, default=5,
                        help="Minimum length of log (in min) to be used. Preset is 5 min")
    # Path to the mission argument
    parser.add_argument('-p', '--mission_path', type=str, default=os.getcwd(),
                        help="Specify path to the actual logs. Preset is your current location")
    
    # Area in the form of a polygon
    parser.add_argument('-d', '--delimiter_path', type=str, 
                        help = "Input csv file with list of points. You can do this using My Maps (Google) Rules: \n" + 
                        "A polygon is required so at least 3 points should be entered. \n" +
                        "A odd number of points will result in an error")
    
    # Add a boolean flag to force th deletion of data files
    parser.add_argument('--force', action='store_true', help="Call argument if you want to generate new data files, evne if they exist")

    # Add a boolean flag to clean up the data.csv files left behing 
    parser.add_argument('--clean', action='store_true', help='Call argument if you want to clean all excel files after netcdf file has been generated')

    # Add a boolean flag to clean up the data.csv files left behing 
    parser.add_argument('--filter_underwater', action='store_true', help='Call arguemnt if you want to FILTER OUT data where the vehicle is not on the water')
    
    # Parse the argument and save it 
    args = parser.parse_args()
    min_time= args.min_time
    delimiter_path =  args.delimiter_path
    mission_path = args.mission_path
    force = args.force
    clean = args.clean
    filter_underwater = args.filter_underwater
    
    # If a polygon was specified
    if delimiter_path:

        delimiter_df =  pd.read_csv(delimiter_path)
        delimiter_df['geometry'] = delimiter_df['WKT'].apply(wkt.loads)
        
        if delimiter_df['geometry'].iloc[0].geom_type == 'Point':

            polygon_points = [(point.x, point.y) for point in delimiter_df['geometry']]
            
            # Check the if the number of points is sufficient to build a polygon
            if (len(polygon_points) < 4):
                print("Number of points is not enough to define a polygon. Please enter at least 4 points")
                sys.exit()

            # if the first point isn't the same as the first one we correct that to close the polygon
            if polygon_points[-1] != polygon_points[0]:
                polygon_points.append(polygon_points[0])

            delimiter_polygon = Polygon(polygon_points)

            print("Polygon {} built from given list of points".format(delimiter_polygon))
        
        else:
             
            delimiter_polygon = delimiter_df['geometry'].iloc[0]
            print("Full {} provided".format(delimiter_polygon))

    else: 

        delimiter_polygon = False

    ## Find all Data.lsf.gz
    compressed_files_path = gather_log_paths(mission_path)
    compressed_files_path.sort()

    ## Decompress them 
    export_logs(compressed_files_path)

    checkable_files = []

    # If the data files already exist, remove them
    if(force):

        for path in compressed_files_path: 
            
            if os.path.isdir(path + '/mra'):
                shutil.rmtree(path + '/mra')

            checkable_files.append(path)

    # else, only go through data files without data xlsx
    else: 
        for path in compressed_files_path:

            if not os.path.isfile(path + '/mra/Data.xlsx') and os.path.isfile(path + '/Data.lsf'):

                checkable_files.append(path)

    rejected_files = []

    ## Get needed data into xlsv file
    for path in checkable_files:

        if not os.path.isdir(path + '/mra'):

            os.makedirs(path + '/mra')
        
        logData = logDataGatherer(path + '/mra/Data.xlsx')
        src_file = path + '/Data.lsf'

        try:

            # Connect to the actual file
            print("\n*** NEW LOG ***")
            sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)
            print("EXPORTING: {} to xlsx file \n".format(src_file))

            # Subscribe to specific variables and provide sub with a callback function
            sub.subscribe_async(logData.update_state, msg_id =pg.messages.EstimatedState)
            sub.subscribe_async(logData.update_temperature, msg_id =pg.messages.Temperature)
            sub.subscribe_async(logData.update_sound_speed, msg_id=pg.messages.SoundSpeed)
            sub.subscribe_async(logData.update_conductivity, msg_id=pg.messages.Conductivity)
            sub.subscribe_async(logData.update_salinity, msg_id=pg.messages.Salinity)
            sub.subscribe_async(logData.update_turbidity, msg_id=pg.messages.Turbidity)
            sub.subscribe_async(logData.update_chloro, msg_id=pg.messages.Chlorophyll)
            sub.subscribe_async(logData.update_vehicle_medium, msg_id=pg.messages.VehicleMedium)

            # Run the even loop (This is asyncio witchcraft)
            sub.run()

            # Go through the Entity info and check for the vehicle name
            key_with_entity_list = next((key for key, value in sub._peers.items() if 'EntityList' in value), None)
            key_with_entity_list = str(key_with_entity_list)
            if 'lauv' in key_with_entity_list:
                print("Log is coming from vehicle {}".format(key_with_entity_list))
                logData.name = key_with_entity_list
        
            if not 'lauv' in logData.name:
                raise Exception("No Vehile found in EntityList")

            # Gather the remaining positions and place the remaining data in a readable formar
            logData.finish_positions()
            # Create dataframes based on data collected from file
            logData.create_dataframes()
            # Merge that data into a single dataframe
            logData.merge_data()
            # Parse that data
            logData.filter_data(delimiter_polygon, min_time, filter_underwater)
            # Actually write to a csv file
            logData.write_to_file()

        except Exception as e:
            
            if e == 'EntityList':
                print("DISCARDED: Log does not include a readable Entity List")

            else: 
                print("DISCARDED: {}".format(e))

            rejected_files.append(path)

    if rejected_files:
        print("For whatever reason these files were not used: {}".format(rejected_files))

    # Remove rejected files from original file list
    for path in rejected_files:
        compressed_files_path.remove(path)

    # Now we concatenate all of the created excel files into a single one
    concat_data = pd.DataFrame()

    for index, path in enumerate(compressed_files_path):
        
        print("Concatenating file: {}".format(path))

        if not os.path.isdir(path + '/mra'):

            os.makedirs(path + '/mra', exist_ok=True)

        logData = path + '/mra/Data.xlsx'
        
        # Read data from excel file
        all_data = pd.read_excel(logData, sheet_name='DATA')
        # Concatenate said file with the previous ones
        concat_data = pd.concat([concat_data, all_data])

        if index == len(compressed_files_path) - 1:
            
            metadata_df = pd.read_excel(logData, sheet_name='METADATA')
            system_name = metadata_df['system'].iloc[0]

    concat_data.sort_values(by='TIME')

    color_scale = [(0, 'orange'), (1, 'red')]

    fig = px.scatter_mapbox(concat_data, 
                            lat='LATITUDE',
                            lon='LONGITUDE',
                            color_continuous_scale=color_scale,
                            zoom=8, 
                            height=800,
                            width=800)
    
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.show()

    # Create an outdata folder    
    outdata_path = os.getcwd() + '/outdata'
    if not os.path.isdir(outdata_path):
        os.mkdir(outdata_path)

    # Gather correct naming for files
    dt = datetime.fromtimestamp(concat_data['TIME'].min())
    dt = dt.date()
    dt = str(dt).replace("-","_")
    file_name = "{}_{}".format(system_name,dt)
    file_path = "{}/{}".format(outdata_path, file_name)

    with pd.ExcelWriter("{}.xlsx".format(file_path), engine='xlsxwriter') as writer:

        # Before writing to file let's add some general metadata
        metadata = {
        'system' : system_name,
        'date_created' : datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        'time_coverage_start' : datetime.fromtimestamp(concat_data['TIME'].min()),
        'time_coverage_end' : datetime.fromtimestamp(concat_data['TIME'].max()),
        'geospatial_lat_min' : concat_data['LATITUDE'].min(),
        'geospatial_lat_max' : concat_data['LATITUDE'].max(),
        'geospatial_lon_min' : concat_data['LONGITUDE'].min(),
        'geospatial_lon_max' : concat_data['LONGITUDE'].max(),
        'geospatial_vertical_min' : concat_data['DEPH'].min(),
        'geospatial_vertical_max' : concat_data['DEPH'].max()
        }

        concat_data.to_excel(writer, sheet_name='DATA', index=False)
        
        workbook = writer.book 
        metadata_sheet = workbook.add_worksheet('METADATA')

        for i, (key, value) in enumerate(metadata.items()):
            metadata_sheet.write(0, i, key)
            metadata_sheet.write(1, i, str(value))

    # Now we create the actual netCDF file based on the name of the system
    netCDF = netCDFExporter(file_path)
    netCDF.build_netCDF()
    netCDF.replace_json_metadata()
    netCDF.to_netCDF()

    if clean:

        print("\n## Clearing Data.xlsx files used ## \n ")

        for path in compressed_files_path: 

            if os.path.isfile(path + '/mra/Data.xlsx'):
                os.remove(path + '/mra/Data.xlsx')
                print("Cleared {} ".format(path))



