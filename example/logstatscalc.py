import argparse
import os
import sys
import pandas as pd

# This allows the script to be run directly by adding the project root to sys.path.
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from example.util.logstats import LogStats
from example.util.units import *
import pyimclsts.network as n
import pyimc_generated as pg

from example.netCDF.utils import *
from example.netCDF.core import *


global source_id
source_id = 0x0000

global globalStats
globalStats = LogStats()


if __name__ == '__main__':

    # Parser for command line
    parser = argparse.ArgumentParser(description="Process arguments for Concatenation Script")

    # Path to the mission argument
    parser.add_argument('-p', '--mission-paths', nargs='+', type=str, default=os.getcwd(),
                        help="Specify path(s) to the actual logs. Preset is your current location. For better ordering run this script whem multiple path on a base common folder.")
   
    parser.add_argument('-x', '--mission-paths-ignore', nargs='+', type=str,
                        help="Specify path(s) to the actual logs to ignore.")
   
    # Add argument for adding IMC id (integer)
    parser.add_argument('-i', '--imc_id', type=str, help="Specify the IMC id to be used, decimal or hexadecimal (0x...)")

    ## Add a boolean flag to force th deletion of data files
    #parser.add_argument('--force', action='store_true', help="Call argument if you want to generate new data files, even if they exist")

    parser.add_argument('--batteries', action='store_true', help="Call argument if you want to generate battery stats")

    parser.add_argument('--voltage-entities', nargs='+', type=str, help="Specify the entities for which to generate voltage statistics. Use entity names or IDs from IMC. Example: '72' or 'Batteries'")
    
    parser.add_argument('--temperature-entities', nargs='+', type=str, help="Specify the entities for which to generate temperature statistics. Use entity names or IDs from IMC. Example: '97' or 'Thermal Zone'")

    # Minimum time argument
    parser.add_argument('--jump_time', type=int, default=10,
                        help="Minimum time jump to reset distance calculation. Preset is 10 sec")
    # Verbose output argument
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Enable verbose output for debugging purposes")

    parser.add_argument('-o', '--output', type=str, default='Statistics',
                        help="Output file name (omit extension). Preset is 'Statistics.xlsx'")

    parser.add_argument('--smooth-filter', action='store_true', help="Apply a sliding window filter to speed and vertical speed calculations for smoothing.")

    parser.add_argument('--smooth-filter-window', type=int, default=10,
                        help="Sliding window size for smoothing. Preset is 10")

    # Parse the argument and save it 
    args = parser.parse_args()

    mission_path_list = args.mission_paths
    mission_paths_ignore = args.mission_paths_ignore
        
    #force = args.force
    verbose = args.verbose

    smooth_filter = args.smooth_filter
    smooth_filter_window = args.smooth_filter_window

    voltage_entities = []
    temperature_entities = []
    if args.batteries:
        voltage_entities += ['Batteries']
    if args.voltage_entities is not None:
        voltage_entities += args.voltage_entities
    if args.temperature_entities is not None:
        temperature_entities += args.temperature_entities
    voltage_entities.sort()
    temperature_entities.sort()

    jump_time_millis = args.jump_time * 1000
    
    # parse arg source as integer or hexadecimal
    argSourceID = None
    if args.imc_id is not None:
        try:
            argSourceID = int(args.imc_id, 0)  # Automatically detects hex or decimal
        except ValueError:
            print("Error: IMC ID must be a valid integer or hexadecimal value.")
            sys.exit(1)
    
    source_id = argSourceID if argSourceID is not None else source_id

    if (source_id <= 0x0000 or source_id >= 0xFFFF):
        print("Error: IMC ID must be between 0x0000 and 0xFFFF (exclusive).")
        sys.exit(1)
    
    output_name = args.output
    output_name = output_name if output_name != '' else 'Statistics'


    # Create a function that takes the arguments and prints them
    def debug(args):
        if verbose:
            print(args)
    
    if verbose:
        print("Verbose mode is enabled.")

    print(mission_path_list)

    # clean duplicates of mission_path_list entries
    mission_path_tmp = []
    for path in mission_path_list:
        if path not in mission_path_tmp and \
                path.rstrip('/') not in mission_path_tmp and \
                "{}/".format(path) not in mission_path_tmp:
            mission_path_tmp.append(path)
    mission_path_list = mission_path_tmp
    mission_path_list.sort()

    if mission_paths_ignore is not None:
        mission_paths_ignore_tmp = []
        for path in mission_paths_ignore:
            if path not in mission_paths_ignore_tmp and \
                    path.rstrip('/') not in mission_paths_ignore_tmp and \
                    "{}/".format(path) not in mission_paths_ignore_tmp:
                mission_paths_ignore_tmp.append(path)
        mission_paths_ignore = mission_paths_ignore_tmp
        mission_paths_ignore.sort()
        # remove mission_paths_ignore from mission_path_list
        mission_path_list = [path for path in mission_path_list if path not in mission_paths_ignore and \
                             path.rstrip('/') not in mission_paths_ignore and \
                             "{}/".format(path) not in mission_paths_ignore]
    else:
        mission_paths_ignore = []
    

    compressed_files_path = []
    for path in mission_path_list:
        ## Find all Data.lsf.gz
        paths = gather_log_paths(path)
        paths.sort()
        compressed_files_path += paths
    compressed_files_path.sort()

    compressed_files_path_ignore = []
    for path in mission_paths_ignore:
        ## Find all Data.lsf.gz
        paths = gather_log_paths(path)
        paths.sort()
        compressed_files_path_ignore += paths
    compressed_files_path_ignore.sort()
    # remove mission_paths_ignore from compressed_files_path
    compressed_files_path = [path for path in compressed_files_path if path not in compressed_files_path_ignore and \
                             path.rstrip('/') not in compressed_files_path_ignore and \
                             "{}/".format(path) not in compressed_files_path_ignore]
    compressed_files_path.sort()

    # remove duplicates from compressed_files_path
    compressed_files_path_tmp = []
    for path in compressed_files_path:
        if path not in compressed_files_path_tmp and \
                path.rstrip('/') not in compressed_files_path_tmp and \
                "{}/".format(path) not in compressed_files_path_tmp:
            compressed_files_path_tmp.append(path)
    compressed_files_path = compressed_files_path_tmp
    compressed_files_path.sort()

    
    debug(compressed_files_path)

    if len(compressed_files_path) == 0:
        print("No .lsf.gz files found in the specified mission paths.")
        sys.exit(0)
        

    ## Decompress them 
    export_logs(compressed_files_path)

    #checkable_files = []

    ## If the data files already exist, remove them
    #if(force):
    #    for path in compressed_files_path: 
    #        if os.path.isdir(path + '/mra'):
    #            shutil.rmtree(path + '/mra')

    #        checkable_files.append(path)

    ## else, only go through data files without data xlsx
    #else: 
    #    for path in compressed_files_path:
    #        if not os.path.isfile(path + '/mra/Statistics.xlsx') and os.path.isfile(path + '/Data.lsf'):
    #            checkable_files.append(path)

    #rejected_files = []

    # mark processing start time
    processing_start_time = datetime.now()
    processing_end_time = datetime.now()
    

    globalStats = LogStats(source_id = source_id, jump_time_millis = jump_time_millis,
                           smooth_filter=smooth_filter, sliding_window_size=smooth_filter_window)
    globalStats.voltageEntities = voltage_entities
    globalStats.temperatureEntities = temperature_entities

    #script_dir = os.path.dirname(os.path.abspath(__file__))
    #src_global_xlsx_file = script_dir + '/' + output_name + '.xlsx'
    output_dir = os.getcwd()
    src_global_xlsx_file = os.path.join(output_dir, f"{output_name}.xlsx")

    with pd.ExcelWriter(src_global_xlsx_file, engine='xlsxwriter') as writer_global:
        sheet = writer_global.book.add_worksheet("Global Statistics") # Add to be the first sheet

        ## Get needed data into xlsv file
        for path in compressed_files_path:
            # mark processing start time
            log_processing_start_time = datetime.now()

            print("\n-------------------------------------------------------------------------------\n"
                  " Processing path: {}\n"
                  "-------------------------------------------------------------------------------\n"
                  .format(path))

            globalStats.entitiesMappings.clear()
            logStats = LogStats(source_id = source_id, jump_time_millis = jump_time_millis,
                                smooth_filter = smooth_filter, sliding_window_size = smooth_filter_window)
            logStats.systemName = globalStats.systemName
            logStats.voltageEntities = voltage_entities
            logStats.temperatureEntities = temperature_entities

            if not os.path.isdir(path + '/mra'):
                os.makedirs(path + '/mra')
            
            src_file = path + '/Data.lsf'
            src_xlsx_file = path + '/mra/' + 'Statistics' + '.xlsx'

            try:
                # Connect to the actual file
                #print("\n*** NEW LOG ***")
                sub = n.subscriber(n.file_interface(input = src_file), use_mp=True)
                #print("EXPORTING: {} to xlsx file \n".format(src_file))

                globalStats.numberOfLogs += 1
                globalStats.new_log_name(os.path.basename(path.rstrip('/')))
                logStats.sourceId = source_id
                logStats.numberOfLogs += 1
                logStats.new_log_name(os.path.basename(path.rstrip('/')))
                
                # Subscribe to specific variables and provide sub with a callback function

                # EstimatedState
                sub.subscribe_async(globalStats.update_state, msg_id = pg.messages.EstimatedState)
                sub.subscribe_async(logStats.update_state, msg_id = pg.messages.EstimatedState)

                # Announce
                if globalStats.systemName is None:
                    sub.subscribe_async(globalStats.update_name, msg_id = pg.messages.Announce)
                    sub.subscribe_async(logStats.update_name, msg_id = pg.messages.Announce)

                # Voltage
                sub.subscribe_async(globalStats.update_voltage, msg_id = pg.messages.Voltage)
                sub.subscribe_async(logStats.update_voltage, msg_id = pg.messages.Voltage)

                # Temperature
                sub.subscribe_async(globalStats.update_temperature, msg_id = pg.messages.Temperature)
                sub.subscribe_async(logStats.update_temperature, msg_id = pg.messages.Temperature)

                # Run the event loop (This is asyncio witchcraft)
                sub.run()                

                # Go through the Entity info and check for the vehicle name
                # key_with_entity_list = next((key for key, value in sub._peers.items() if 'EntityList' in value), None)

                try:
                    # look in the sub._peers.items() the value that has 'EntityList' and the src is the same as source_id
                    key_with_entity_list = next((key for key, value in sub._peers.items() if 'EntityList' in value and value['src'] == source_id), None)
                    debug("List 111: {}".format(sub._peers[key_with_entity_list]))
                    if key_with_entity_list is not None:
                        entity_list = sub._peers[key_with_entity_list]['EntityList']
                        # Populate entitiesMappings
                        for entity_name in entity_list:
                            globalStats.entitiesMappings[entity_name] = entity_list[entity_name]
                            logStats.entitiesMappings[entity_name] = entity_list[entity_name]
                except Exception as e:
                    pass

            except Exception as e:
                print("Error while processing file {}: {}".format(src_file, e))
                #rejected_files.append(src_file)
                continue
            finally:
                # Finalize the statistics
                # globalStats.finalize()
                logStats.finalize()
                logStats.map_unnamed_variables_to_named()
                globalStats.map_unnamed_variables_to_named()
                globalStats.entitiesMappings.clear()
            
            # Print the statistics
            print("\n\n*** STATISTICS *** {}".format(path))
            print(logStats)

            if logStats.systemName:
                filename_suffix = '_{}.xlsx'.format(logStats.systemName)
            else:
                filename_suffix = '_0x{:04X}.xlsx'.format(logStats.sourceId)
            src_xlsx_file_final = src_xlsx_file.replace('.xlsx', filename_suffix)
            with pd.ExcelWriter(src_xlsx_file_final, engine='xlsxwriter') as writer:
                logStats.write_to_file(writer, sheet_name="Global Statistics")
            
            if len(logStats.logNames) > 0:
                logStats.write_to_file(writer_global, sheet_name=logStats.logNames[0])
            
            log_processing_end_time = datetime.now()
            processing_end_time = log_processing_end_time
            print("\nLog processing time: {}".format(format_timedelta_human(log_processing_end_time - log_processing_start_time)))
            
        
        globalStats.finalize()
        # Print the global statistics
        print("\n\n*** GLOBAL STATISTICS ***")
        print(globalStats)

        globalStats.write_to_file(writer_global, sheet_name="Global Statistics", reuse_sheet=True)

        processing_end_time = datetime.now()
        print("\nTotal processing time: {}".format(format_timedelta_human(processing_end_time - processing_start_time)))
