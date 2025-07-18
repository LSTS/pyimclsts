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
from example.powerlog import VoltAmpGatherer

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

    # Parse the argument and save it 
    args = parser.parse_args()
    min_time= args.min_time
    delimiter_path =  args.delimiter_path
    mission_path = args.mission_path
    force = args.force
    clean = args.clean

    
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

    ### Completed, now logs full paths are coming out of it.
    compressed_files_path = gather_log_paths(mission_path)
    compressed_files_path.sort()

    ## Decompress them 
    decompressed_files_path = export_logs(compressed_files_path)

    checkable_files = []

    # If the data files already exist, remove them
    if(force):

        for path in compressed_files_path: 
            
            path_dir = os.path.dirname(path.rstrip('/'))

            if os.path.isdir(path_dir + '/mra'):
                shutil.rmtree(path_dir + '/mra')

            if path.endswith('.gz'):
                
                checkable_files.append(path[:-3])

            else:

                checkable_files.append(path)

    # else, only go through data files without data xlsx
    else: 

        for path in compressed_files_path:

            path_dir = os.path.dirname(path.rstrip('/'))

            if not os.path.isfile(path_dir + '/mra/Data.xlsx'):
                
                if path.endswith('.gz'):

                    checkable_files.append(path[:-3])
                
                else:

                    checkable_files.append(path)

    rejected_files = []

    print("Checkable files: {}".format(checkable_files))

    ## Get needed data into xlsv file
    for path in checkable_files:

        path_dir = os.path.dirname(path.rstrip('/'))

        if not os.path.isdir(path_dir + '/mra'):

            os.makedirs(path_dir + '/mra')
        
        logData = VoltAmpGatherer(path_dir + '/mra/Data.xlsx')
        src_file = path 

        try:

            # Connect to the actual file
            print("\n*** NEW LOG ***")
            sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)
            print("EXPORTING: {} to xlsx file \n".format(src_file))

            sub.subscribe_async(logData.update_voltage, msg_id=pg.messages.Voltage)
            sub.subscribe_async(logData.update_current, msg_id=pg.messages.Current)

            # Run the even loop (This is asyncio witchcraft)
            sub.run()

            # Go through the Entity info and check for the vehicle name
            key_with_entity_list = next((key for key, value in sub._peers.items() if 'EntityList' in value), None)

            print("Log is coming from vehicle {}".format(key_with_entity_list))
            
            # Corrent names
            if 'lauv' or 'autonaut' or 'caravel' or '2052' in key_with_entity_list:

                logData.name = str(key_with_entity_list)
                # Once found the correct vehicle we will load up the Entity List dictionary
                entity_list = sub._peers[key_with_entity_list]

            if 'lauv' in logData.name or 'autonaut' in logData.name or 'caravel' in logData.name or '2052' in logData.name:
                print("Valid vehicle found in EntityList")

            else: 
                raise Exception("No Vehile found in EntityList")
                        
            logData.batteries_id = entity_list['EntityList'].get('Batteries')
            
            # Create dataframes based on data collected from file
            logData.create_dataframes()
            # Merge that data into a single dataframe
            logData.merge_data()
            # Parse that data
            logData.filter_data(delimiter_polygon, min_time)
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
    for path_dir in rejected_files:
        decompressed_files_path.remove(path_dir)

    # Now we concatenate all of the created excel files into a single one
    concat_data = pd.DataFrame()

    for index, path in enumerate(decompressed_files_path):
        
        path_dir = os.path.dirname(path.rstrip('/'))

        print("Concatenating file: {}".format(path))

        logData = path_dir + '/mra/Data.xlsx'
        
        # Read data from excel file
        all_data = pd.read_excel(logData, sheet_name='DATA')
        # Concatenate said file with the previous ones
        concat_data = pd.concat([concat_data, all_data])

        if index == len(decompressed_files_path) - 1:
            
            metadata_df = pd.read_excel(logData, sheet_name='METADATA')
            system_name = metadata_df['system'].iloc[0]

    concat_data.sort_values(by='TIME')

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

    MAX_EXCEL_ROWS = 500_000

    with pd.ExcelWriter("{}.xlsx".format(file_path), engine='xlsxwriter') as writer:

        # Before writing to file let's add some general metadata
        metadata = {
        'system' : system_name,
        'date_created' : datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        'time_coverage_start' : datetime.fromtimestamp(concat_data['TIME'].min()),
        'time_coverage_end' : datetime.fromtimestamp(concat_data['TIME'].max()),
        }

        concat_data.to_excel(writer, sheet_name='DATA', index=False)
        
        workbook = writer.book 
        metadata_sheet = workbook.add_worksheet('METADATA')

        for i, (key, value) in enumerate(metadata.items()):
            metadata_sheet.write(0, i, key)
            metadata_sheet.write(1, i, str(value))

        total_rows = len(concat_data)

        if total_rows <= MAX_EXCEL_ROWS: 

            concat_data.to_excel(writer, sheet_name='DATA', index=False)

        else:

            for i in range(0, total_rows, MAX_EXCEL_ROWS):
                chunk = concat_data.iloc[i:i, MAX_EXCEL_ROWS]
                sheet_name = f'DATA_Part_{i // MAX_EXCEL_ROWS + 1}'
                chunk.to_excel(writer, sheet_name=sheet_name, index=False)




