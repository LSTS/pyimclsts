from example.netCDF.utils import *
from example.netCDF.core import *
from datetime import datetime 
from shapely import wkt
import pyimclsts.network as n
import pyimc_generated as pg


## Find all Data.lsf.gz
mission_path  = "/mnt/sdb1/Missions/FRESNEL/lauv-xplore-5/20241030/162259_soi_plan"
compressed_files_path = gather_log_paths(mission_path)
compressed_files_path.sort()

## Decompress them 
export_logs(compressed_files_path)

## Get needed data into xlsv file
for path in compressed_files_path:

    if not os.path.isdir(path + '/mra'):

        os.makedirs(path + '/mra')
    
    logData = logDataGatherer(path + '/mra/ctd.xlsx')
    src_file = path + '/Data.lsf'

    # Connect to the actual file
    print("\n*** NEW LOG ***")
    sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)
    print("EXPORTING: {} to xlsx file \n".format(src_file))

    # Subscribe to specific variables and provide sub with a callback function
    sub.subscribe_async(logData.update_state, msg_id=pg.messages.EstimatedState)
    sub.subscribe_async(logData.update_vehicle_medium, msg_id=pg.messages.VehicleMedium)
    sub.subscribe_async(logData.update_temperature, msg_id =pg.messages.Temperature)
    sub.subscribe_async(logData.update_pressure, msg_id=pg.messages.Pressure)
    sub.subscribe_async(logData.update_salinity, msg_id=pg.messages.Salinity)

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
    logData.computeDensity()


