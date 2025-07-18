from example.netCDF.utils import *
import geopandas.geodataframe
from datetime import datetime
import pandas as pd
import xarray as xr
import numpy as np

class VoltAmpGatherer():
    
    def __init__(self, f : str) -> None:
        
        '''f is file name'''

        self.file_name = f
        
        ## This will have to be seperated 
        self.datatable = []
        self.voltage = []
        self.current = []
        self.batteries_id = None

        ## Usefull for parsing 
        self.sensor_ent = -1
        
        # Updated state last_time
        self.us_last_time = 0 
        # Will skip updated state message if difference between timestamps isn't big enough (milliseconds)
        self.msg_diff_time = 99.5

        ## Usefull for parsing 
        self.sensor_ent = -1
        
        # Will skip updated state message if difference between timestamps isn't big enough (milliseconds)
        self.msg_diff_time = 99.5

        self.name = "Caravel"

    def update_voltage(self, msg, callback):

        time =  msg._header.timestamp
        src_ent = msg._header.src_ent
        voltage = [time, src_ent, msg.value]
        self.voltage.append(voltage)

    def update_current(self, msg, callback):
        
        time =  msg._header.timestamp
        src_ent = msg._header.src_ent
        current = [time, src_ent, msg.value]
        self.current.append(current)
        
    # Save the variables in a dataframe for easier parsing
    def create_dataframes(self):
        
        print("Creating Dataframes")
        
        if self.voltage:

            self.df_voltage = pd.DataFrame(self.voltage, columns=['TIME','SRC_ENT', 'VOLT'])
            self.df_voltage = self.df_voltage.sort_values(by='TIME')

        else: 
            print("No Values found for Voltage")
            Exception("No Voltage values found. SKIP file")
        
        if self.current:

            self.df_current = pd.DataFrame(self.current, columns=['TIME','SRC_ENT', 'CURR'])
            self.df_current = self.df_current.sort_values(by='TIME')

        else: 
            print("No Values found for Current")
            Exception("No Current values found. SKIP file")
      
    # Merge all data into a single dataframe for later filtering
    def merge_data(self):
        
        print("Merging Data")

        self.cols = ['TIME', "VOLT", "CURR"]

        if self.df_voltage.isnull().all().all():
            raise Exception("No Voltage Values found")
        
        else:
            
            self.df_voltage = self.df_voltage[self.df_voltage['SRC_ENT'] == self.batteries_id]

            if self.df_voltage.isnull().all().all():
                raise Exception("All Voltage values were filtered out")
            
                      
        if self.df_current.isnull().all().all():
            
            raise Exception("No Current Values found")

        else:
            
            self.df_current = self.df_current[(self.df_current['SRC_ENT']) == (self.batteries_id)]

            if self.df_current.isnull().all().all():
                raise Exception("All Current values were filtered out")

            self.df_all_data = pd.merge_asof(self.df_voltage, self.df_current, on='TIME',
                                             direction='nearest', suffixes=('_df1', '_df2'))
            
          
        # Rearrange positions dataframe for better visibility
        self.df_all_data = self.df_all_data[self.cols]
        
        # Turn the normal dataframe into geopandas dataframe for easier filtering 
        self.df_all_data = geopandas.GeoDataFrame(self.df_all_data)
        
    def filter_data(self, polygon = False, duration_limit=-1):

        self.df_all_data.sort_values(by='TIME')
        initial_rows = len(self.df_all_data)

        # Check the duration of the current data gathered
        if duration_limit != -1:

            duration = self.df_all_data['TIME'].max() - self.df_all_data['TIME'].min()
            
            # Duration of log is just too short so csv will not be created 
            if duration < duration_limit*60:

                raise Exception("Log has a duration of {} minutes which is lower than the required {} minutes"
                                .format(duration/60, duration_limit))
        
        if self.df_all_data.isnull().all().all():

            raise Exception("Dataframe is empty. Log was filtered out")
        
    def write_to_file(self):

        if not self.df_all_data.isnull().all().all():  
            
            # Before writing to file let's add some general metadata
            metadata = {
            'system' : self.name,
            'data_created' : datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'time_coverage_start' : datetime.fromtimestamp(self.df_all_data['TIME'].min()),
            'time_coverage_end' : datetime.fromtimestamp(self.df_all_data['TIME'].max()),
            }

            with pd.ExcelWriter(self.file_name, engine='xlsxwriter') as writer:

                print("Writing to {}".format(self.file_name))
                self.df_all_data.to_excel(writer, sheet_name='DATA', index=False)

                workbook = writer.book 
                metadata_sheet = workbook.add_worksheet('METADATA')

                for i, (key, value) in enumerate(metadata.items()):
                    metadata_sheet.write(0, i, key)
                    metadata_sheet.write(1, i, str(value))

        else:

            print("Dataframe {} was empty. NOT WRITING".format(self.file_name))

def export_large_df_to_csv(df, base_filename, max_rows_per_file=500_000):
    
    total_rows = len(df)

    for i in range(0, total_rows, max_rows_per_file):
        chunk = df.iloc
        

        
                
