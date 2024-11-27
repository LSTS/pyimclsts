from example.netCDF.utils import *
import geopandas.geodataframe
import matplotlib.pyplot as plt
import seawater as sea
import gsw 
from datetime import datetime
import pandas as pd
import xarray as xr
import numpy as np
import json
import sys
import copy


class locationType():

    def __init__(self):

        self.lat = 0
        self.lon = 0
        self.depth = 0
        self.alt = 0

        self.roll = 0 
        self.pitch = 0
        self.yaw = 0
    
        self.north_offset = 0 
        self.east_offset = 0 
        self.down_offset = 0 

        self.time = 0

    def fill_it (self, msg):
        
        self.time = msg._header.timestamp

        self.depth = msg.depth
        self.alt = msg.alt

        self.roll = msg.phi
        self.pitch = msg.theta 
        self.yaw = msg.psi

        self.u = msg.u 
        self.v = msg.v 
        self.w = msg.w

        self.vx = msg.vx
        self.vy = msg.vy
        self.vz = msg.vz

    def set_position(self, pos : 'locationType'):

        self.lat = pos.lat
        self.lon = pos.lon
        self.depth = pos.depth

    
    # Add offsets to (x,y,z)
    def translate_positions(self, x, y, z):

        self.north_offset = self.north_offset + x
        self.east_offset = self.east_offset + y
        self.down_offset = self.down_offset + z

    def add_offsets(self):

        # Only apply offset if there is actually any
        if(self.north_offset != 0 or self.east_offset != 0):

            # Translate WGSM coordinates to ECEF so we can add x,y displacement
            x_ecef, y_ecef, z_ecef = toECEF(self.lat,self.lon,self.depth)

            p = np.sqrt(x_ecef**2 + y_ecef**2)

            phi = np.arctan2(z_ecef, p)

            slon = np.sin(self.lon)
            clon = np.cos(self.lon)
            sphi = np.sin(phi)
            cphi = np.cos(phi)
            
            # Add the displacement
            x_ecef = x_ecef + (-slon*self.east_offset - clon*sphi*self.north_offset - clon*cphi*self.down_offset)
            y_ecef = y_ecef + (clon*self.east_offset - slon*sphi*self.north_offset -slon*cphi*self.down_offset)
            z_ecef = z_ecef + (cphi*self.north_offset - sphi*self.down_offset)
            
            lld = []
            lld.extend(fromECEF(x_ecef, y_ecef, z_ecef))

            if self.down_offset != 0:

                lld[2] = self.depth + self.down_offset

            else:

                lld[2] = self.depth        

            self.lat = lld[0]
            self.lon = lld[1]
            self.depth = lld[2] 

            self.north_offset = 0
            self.east_offset = 0
            self.down_offset = 0       


    # Using the coordinates and the offset we check the actual distance between two locations        
    def getHorizontalDistanceInMeters(self, otherLocation : 'locationType'):
        
        displacements = []
        displacements.extend(WGS84displacement(otherLocation.lat, otherLocation.lon, otherLocation.depth, self.lat, self.lon, self.depth))
        
        return np.sqrt(displacements[0]**2 + displacements[1]**2) 
    
    # Get the (x,y,z) displacements
    def getWGS84displacement(self, otherLocation : 'locationType'):

        displacements = []
        displacements.extend(WGS84displacement(otherLocation.lat, otherLocation.lon, otherLocation.depth, self.lat, self.lon, self.depth))
        
        return displacements
    

class logDataGatherer():
    
    def __init__(self, f : str) -> None:
        '''f is file name'''

        self.file_name = f
        
        ## This will have to be seperated 
        self.datatable = []
        self.temperature = []
        self.estimated_states = []
        self.sound_speed = []
        self.conductivity = []
        self.medium = []
        self.salinity = []
        self.turbidity = []
        self.chloro = []
        self.pressure = []
        self.name = 'NoName'
        self.cols = []

        ## Usefull for parsing 
        self.sensor_ent = -1

        ## Usefull for correcting positions 
        self.currentLoc = locationType()
        self.lastLoc = locationType()
        self.lastTime = -1
        self.nonAdjusted = []
        self.nonAdjustedLoc = []
        self.positions = []
        
        # Updated state last_time
        self.us_last_time = 0 
        # Will skip updated state message if difference between timestamps isn't big enough (milliseconds)
        self.msg_diff_time = 99.5


    """
    # After added offsets we check if the value needs to be corrected or not ## 
    # To do this we compare the expected difference of positons based on speed with the actual differance
    # If the actual difference is below the expected diff we save that position for correction 
    # That correction is made when that difference is above the expected one. Essentially, when this happens, we are assuming 
    # that the vehicle has resurfaced and a correction to its position was made.
    # We then propagate that correction to all previous uncorrected measurements
    """

    def correct_positions(self, msg):

        # This code is strictly for correcting positions
        self.currentLoc.lat = msg.lat
        self.currentLoc.lon = msg.lon 
        self.currentLoc.height =  msg.height
        self.currentLoc.time = msg._header.timestamp

        if msg.depth > 0:
            self.currentLoc.depth = msg.depth
        
        if msg.alt > 0:
            self.currentLoc.depth = - msg.alt

        self.currentLoc.translate_positions(msg.x, msg.y, 0)
        speed = np.sqrt(msg.u**2 + msg.v**2 + msg.w**2)
        
        # Add the offsets to the lat lon depth values
        self.currentLoc.add_offsets()

        # Check if past position is not empty 
        if self.lastLoc.lat != 0: 
            
            # The expected diff is calculated using speed values of current location and current and previous timestamps
            expectedDiff = speed * (msg._header.timestamp - self.lastTime)
            self.lastTime = msg._header.timestamp

            # Again, the difference here is the one based displacements between the current and previous positions
            diff = self.lastLoc.getHorizontalDistanceInMeters(self.currentLoc)

            # If that difference isn't much different from the expected diff we save it for further correction
            if (diff < expectedDiff * 3):

                self.nonAdjusted.append(msg)
                self.nonAdjustedLoc.append(copy.deepcopy(self.currentLoc))

            # If that difference is big enough we correct it
            else:
                
                # Check if the non ajdusted list is not empty
                if (self.nonAdjusted):
                    
                    adjustement = []
                    adjustement.extend(self.currentLoc.getWGS84displacement(self.lastLoc))
                    
                    # Let's get a new average velocity of based on the time of the first adjusted location and the current, updated one
                    firstNonAdjusted = self.nonAdjusted[0]
                    timeOfAdjustement = msg._header.timestamp - firstNonAdjusted._header.timestamp
                    
                    # The velocities in x and y axis. 
                    xIncPerSec = adjustement[0] / timeOfAdjustement
                    yIncPerSec = adjustement[1] / timeOfAdjustement

                    for index, item in enumerate(self.nonAdjusted):
                        
                        adj = item
                        loc = self.nonAdjustedLoc[index]
                        loc.translate_positions(xIncPerSec * (adj._header.timestamp - firstNonAdjusted._header.timestamp),
                                              yIncPerSec * (adj._header.timestamp - firstNonAdjusted._header.timestamp), 0)
                        
                        loc.add_offsets()

                        # Now we get the corrected position with the rest of information (roll, pitch, yaw, etc)
                        corrected_loc = copy.deepcopy(loc)
                        corrected_loc.fill_it(self.nonAdjusted[index])
                        self.positions.append(corrected_loc)          

                    self.nonAdjusted.clear()
                    self.nonAdjustedLoc.clear()    
                    self.nonAdjusted.append(msg)
                    self.nonAdjustedLoc.append(copy.deepcopy(self.currentLoc))
                    
        else:
            
            corrected_loc = locationType()
            corrected_loc = copy.deepcopy(self.currentLoc)
            corrected_loc.fill_it(msg)
            self.positions.append(copy.deepcopy(corrected_loc))

            
            
        # Update previous location and timestamp
        self.lastTime = msg._header.timestamp 
        self.lastLoc = copy.deepcopy(self.currentLoc)

    def finish_positions(self):
        
        # The last values of the log will probably not need corretion (Assuming it finishes in the surface)
        for index, item in enumerate(self.nonAdjusted):

            adj = item 
            loc = copy.deepcopy(self.nonAdjustedLoc[index])
            loc.add_offsets()
            loc.fill_it(adj)
            self.positions.append(loc)

        for item in self.positions:

            # Turn the roll, pitch, yaw into readable degrees
            roll = np.rad2deg(np.arctan2(np.sin(item.roll), np.cos(item.roll)))
            pitch = np.rad2deg(np.arctan2(np.sin(item.pitch), np.cos(item.pitch)))
            yaw = np.rad2deg(np.arctan2(np.sin(item.yaw), np.cos(item.yaw)))

            # Calculate the velocity over ground magniute (dont take into account z axis)
            ground_speed =  [item.vx, item.vy]
            ground_speed = np.linalg.norm(ground_speed)

            # Calculate course over ground
            course_ground = np.rad2deg(np.arctan2(item.vy, item.vx))

            lat = item.lat 
            lon =  item.lon 

            point = [item.time, np.rad2deg(lat), np.rad2deg(lon), item.depth, roll, pitch, yaw, ground_speed, course_ground]
            self.estimated_states.append(point)
    
    def update_temperature(self, msg, callback):

        time = msg._header.timestamp
        src_ent = msg._header.src_ent
        temp = [time, src_ent, msg.value]
        self.temperature.append(temp)
        
    def update_state(self, msg, callback):
        
        time = msg._header.timestamp

        if (time*1000 - self.us_last_time*1000) < self.msg_diff_time:
            
            return

        self.correct_positions(msg)
        
        # Clear current location and update previous time
        self.currentLoc.__init__()
        self.us_last_time = time
    
    def update_pressure(self, msg, callback):

        time = msg._header.timestamp
        src_ent = msg._header.src_ent 
        pressure = [time, src_ent, msg.value]
        self.pressure.append(pressure)

    def update_sound_speed(self, msg, callback):

        time = msg._header.timestamp
        sspeed = [time, msg.value]
        self.sound_speed.append(sspeed)

    def update_conductivity(self, msg, callback):

        time = msg._header.timestamp
        src_ent = msg._header.src_ent
        conductivity  = [time, src_ent, msg.value]
        self.conductivity.append(conductivity)

    # Turbidity may or may not exist depending on the vehicle
    def update_turbidity(self, msg, callback):

        time = msg._header.timestamp
        turbidity  = [time, msg.value]
        self.turbidity.append(turbidity)
    
    # Chlorophyll may also exist or not depending on the vehicle
    def update_chloro(self, msg, callback):

        time = msg._header.timestamp
        chloro = [time, msg.value]
        self.chloro.append(chloro)
    
    def update_salinity(self, msg, callback):
        
        time = msg._header.timestamp
        salinity = [time, msg.value]
        self.salinity.append(salinity)

    def update_vehicle_medium(self, msg, callback):

        time = msg._header.timestamp
        medium = [time, msg.medium]
        self.medium.append(medium)

    # Use to compute Density from CTD values
    def computeDensity(self):

        self.cols = ['TIME','LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PTCH', 'HDNG', 'APSA', 'APDA', 'TEMP','PSAL', 'PRES', 'MEDIUM']
        
        if not self.estimated_states:
            raise Exception("Log has no ESTIMATED State")
        
        else:

            self.df_positions = pd.DataFrame(self.estimated_states, columns=['TIME', 'LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PTCH', 'HDNG', 'APSA', 'APDA'])
            self.df_positions = self.df_positions.sort_values(by='TIME')

        if not self.medium:
            raise Exception("Log has no VEHICLE MEDIUM")
        
        else:
            self.df_vehicle_medium = pd.DataFrame(self.medium, columns=['TIME', 'MEDIUM'])
            self.df_vehicle_medium = self.df_vehicle_medium.sort_values(by='TIME')
        
        self.df_salinity = pd.DataFrame(self.salinity, columns=['TIME', 'PSAL'])
        self.df_salinity = self.df_salinity.sort_values(by='TIME')
 
        self.df_temperatures = pd.DataFrame(self.temperature, columns=['TIME','SRC_ENT', 'TEMP'])
        self.df_temperatures = self.df_temperatures.sort_values(by='TIME')

        self.df_pressure = pd.DataFrame(self.pressure, columns=['TIME','SRC_ENT','PRES'])
        self.df_pressure = self.df_pressure.sort_values(by='TIME')

        # Get the proper entity for CTD sensor
        self.sensor_ent = self.df_pressure.loc[1, 'SRC_ENT']
        self.df_pressure.drop('SRC_ENT', axis=1)

        # Pascal to dBar
        self.df_pressure['PRES'] = self.df_pressure['PRES'] / 1000

        # Get the correct values from temperature
        self.df_temperatures = self.df_temperatures[self.df_temperatures['SRC_ENT'] == self.sensor_ent]
        self.df_temperatures = self.df_temperatures.drop('SRC_ENT', axis=1)

        # Now merge all data
        self.df_all_data = pd.merge_asof(self.df_temperatures, self.df_pressure, on='TIME', 
                                        direction='nearest', suffixes=('_df1', '_df2'))
        self.df_all_data = pd.merge_asof(self.df_all_data, self.df_salinity, on='TIME',
                                        direction='nearest', suffixes=('_df1', '_df2'))
        self.df_all_data = pd.merge_asof(self.df_all_data, self.df_positions, on='TIME', 
                                        direction='nearest', suffixes=('_df1', '_df2'))
        self.df_all_data = pd.merge_asof(self.df_all_data, self.df_vehicle_medium, on='TIME',
                                         direction='nearest', suffixes=('_df1', '_df2'))
                
        # Rearrange positions dataframe for better visibility
        self.df_all_data = self.df_all_data[self.cols]

        print(self.df_all_data)

        # Now that we have all the data we can calculate the density in each point
        salinity = self.df_all_data['PSAL'].to_list()
        temperature = self.df_all_data['TEMP'].to_list()
        pressure = self.df_all_data['PRES'].to_list()
        depth = self.df_all_data['DEPH'].to_list()
        lat = self.df_all_data['LATITUDE'].to_list()
        lon = self.df_all_data['LONGITUDE'].to_list()

        # Density calculated with 80s equation 
        density = sea.eos80.dens(salinity, temperature, pressure)
        
        # To calculate with updated module we need absolute salinity values and Conservative temperature

        # According to gsw.conversions.SA_from_SP remove this value from here 
        pressure = [x -5 for x in pressure]
        # Absolute salinity
        abs_salinity = gsw.conversions.SA_from_SP(salinity, pressure, lon, lat)
        # Convervative temperature
        cons_temperature = gsw.conversions.CT_from_t(abs_salinity, temperature, pressure)
        # Density according to TEOS-10 
        teos_density = gsw.density.rho(abs_salinity, cons_temperature, pressure)

        print(density)
        print(teos_density)

        """ 
        
        fig, axs = plt.subplots(2,2, figsize=(10,8), sharex=True)

        # Plot Temperature vs Time
        axs[0,0].plot(self.df_all_data['TIME'], self.df_all_data['TEMP'], color='red', marker='o', label='Temperature (°C)')
        axs[0,0].set_title('Temperature vs Time')
        axs[0,0].set_ylabel('Temperature (°C)')
        axs[0,0].legend(loc='upper right')
        axs[0,0].grid(True)

        # Plot Humidity vs Time
        axs[0,1].plot(self.df_all_data['TIME'], self.df_all_data['PSAL'], color='blue', label='Salinity')
        axs[0,1].set_title('Salinity vs Time')
        axs[0,1].set_ylabel('Salinity')
        axs[0,1].legend(loc='upper right')
        axs[0,1].grid(True)

        # Plot Pressure vs Time
        axs[1,0].plot(self.df_all_data['TIME'], self.df_all_data['PRES'], color='green', label='Pressure (dBar)')
        axs[1,0].set_title('Pressure vs Time')
        axs[1,0].set_ylabel('Pressure (dBar)')
        axs[1,0].set_xlabel('Time')
        axs[1,0].legend(loc='upper right')
        axs[1,0].grid(True)

        # Plot Density vs Time
        axs[1, 1].plot(self.df_all_data['TIME'], density, color='purple', marker='x', linestyle='--', label='Density')
        axs[1, 1].set_title('Density vs Time')
        axs[1, 1].set_ylabel('Density')
        axs[1, 1].legend()
        axs[1, 1].grid(True)

        # Add shared X-axis label and adjust layout
        for ax in axs[1, :]:  # Add X-axis label only to the bottom row
            ax.set_xlabel('Time (hours)')
        plt.tight_layout()
        plt.show()

        """


    # Save the variables in a dataframe for easier parsing
    def create_dataframes(self):
        
        # If the estimated states 
        if not self.estimated_states:
            raise Exception("Log has no ESTIMATED STATE")
        
        else:
            self.df_positions = pd.DataFrame(self.estimated_states, columns=['TIME', 'LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PTCH', 'HDNG', 'APSA', 'APDA'])
            self.df_positions = self.df_positions.sort_values(by='TIME')

        if not self.medium:
            raise Exception("Log has no VEHICLE MEDIUM")

        self.df_vehicle_medium = pd.DataFrame(self.medium, columns=['TIME', 'MEDIUM'])
        self.df_vehicle_medium = self.df_vehicle_medium.sort_values(by='TIME')
 
        self.df_temperatures = pd.DataFrame(self.temperature, columns=['TIME','SRC_ENT', 'TEMP'])
        self.df_temperatures = self.df_temperatures.sort_values(by='TIME')

        self.df_conductivity = pd.DataFrame(self.conductivity, columns=['TIME','SRC_ENT', 'CNDC'])
        self.df_conductivity = self.df_conductivity.sort_values(by='TIME')

        self.df_sound_speed = pd.DataFrame(self.sound_speed, columns=['TIME', 'SVEL'])
        self.df_sound_speed = self.df_sound_speed.sort_values(by='TIME')

        self.df_salinity = pd.DataFrame(self.salinity, columns=['TIME', 'PSAL'])
        self.df_salinity = self.df_salinity.sort_values(by='TIME')
        
        if self.name == 'lauv-xplore-2': 
            self.df_turbidity = pd.DataFrame(self.turbidity, columns=['TIME', 'TSED'])
            self.df_turbidity = self.df_turbidity.sort_values(by='TIME')

            self.df_chloro =  pd.DataFrame(self.chloro, columns=['TIME', 'CPWC'])
            self.df_chloro = self.df_chloro.sort_values(by='TIME')

    # Merge all data into a single dataframe for later filtering
    def merge_data(self):

        self.cols = ['TIME','LATITUDE', 'LONGITUDE', 'DEPH', 'ROLL', 'PTCH', 'HDNG', 'APSA', 'APDA', 'TEMP', 'CNDC', 'SVEL', 'PSAL', 'MEDIUM']
        
        # Do a sanity check and look for the sensor gathering oceanographic data
        # Also merge data by lowest frequency data which seems to always be the sound speed variable
        if self.df_sound_speed.isnull().all().all():
            print("NO SOUND SPEED FOUND")

        if self.df_conductivity.isnull().all().all():
            print("NO CONDUCTIVITY FOUND")

        else:
            # Save the correct entity to later filter temperature values
            self.sensor_ent = self.df_conductivity.loc[1, 'SRC_ENT']
            self.df_all_data = pd.merge_asof(self.df_sound_speed, self.df_conductivity, on='TIME', 
                                            direction='nearest', suffixes=('_df1', '_df2'))
        if self.sensor_ent != -1:

            if self.df_temperatures.isnull().all().all():

                print("NO TEMPERATURE FOUND")

            else:

                self.df_temperatures = self.df_temperatures[self.df_temperatures['SRC_ENT'] == self.sensor_ent]
                self.df_temperatures = self.df_temperatures.drop('SRC_ENT', axis=1)
                self.df_all_data = self.df_all_data.drop('SRC_ENT', axis=1)

                self.df_all_data = pd.merge_asof(self.df_all_data, self.df_temperatures, on='TIME', 
                                                direction='nearest', suffixes=('_df1', '_df2') )
        
        else:
            print("NO USABLE ENTITY FOR TEMPERATURE FOUND")

        if self.df_salinity.isnull().all().all():
            print("NO SALINITY FOUND")
        
        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_salinity, on='TIME',
                                            direction='nearest', suffixes=('_df1', '_df2'))
            
        if self.name == 'lauv-xplore-2':
        
            if self.df_chloro.isnull().all().all():
                print("NO CHLOROPHYLL FOUND")

            else:
                self.df_all_data = pd.merge_asof(self.df_all_data, self.df_chloro, on='TIME',
                                                direction='nearest', suffixes=('_df1', '_df2'))
                self.cols.append('CPWC')
                
            
            if self.df_turbidity.isnull().all().all():
                print("NO TURBIDITY FOUND")

            else:
                self.df_all_data = pd.merge_asof(self.df_all_data, self.df_turbidity, on='TIME',
                                                direction='nearest', suffixes=('_df1', '_df2'))
                self.cols.append('TSED')
        
        if self.df_positions.isnull().all().all():
            print("NO POSITIONS FOUND")

        else:
            self.df_all_data = pd.merge_asof(self.df_all_data, self.df_positions, on='TIME',
                                             direction='nearest', suffixes=('_df1', '_df2'))
        
        if self.df_vehicle_medium.isnull().all().all():
            print("NO VEHICLE MEDIUM FOUND")
        
        else: 
            self.df_all_data =  pd.merge_asof(self.df_all_data, self.df_vehicle_medium, on='TIME',
                                              direction='nearest', suffixes=('_df1','_df2'))
        
        # Rearrange positions dataframe for better visibility
        self.df_all_data = self.df_all_data[self.cols]

        # Turn the normal dataframe into geopandas dataframe for easier filtering 
        self.df_all_data = geopandas.GeoDataFrame(self.df_all_data,
                                                geometry = geopandas.points_from_xy(self.df_all_data.LONGITUDE, self.df_all_data.LATITUDE))
        
    def filter_data(self, polygon = False, duration_limit=-1, filter_underwater=False):

        self.df_all_data.sort_values(by='TIME')
        initial_rows = len(self.df_all_data)

        # Check the duration of the current data gathered
        if duration_limit != -1:

            duration = self.df_all_data['TIME'].max() - self.df_all_data['TIME'].min()
            
            # Duration of log is just too short so csv will not be created 
            if duration < duration_limit*60:

                raise Exception("Log has a duration of {} minutes which is lower than the required {} minutes"
                                .format(duration/60, duration_limit))
        
        if filter_underwater:

            self.df_all_data = self.df_all_data[(self.df_all_data['MEDIUM'] != 0) & (self.df_all_data['PSAL'] != 0)]
            underwater_rows = len(self.df_all_data)

            if (initial_rows - underwater_rows) > 0:

                print("{} points were removed due to Underwater Filter".format(initial_rows - underwater_rows))

        else: 
            print("No underwater filter was specified")

        # If an area (polygon) a filtering should occur      
        if polygon:
            
            print("Checking if trajectory belongs within polygon {}".format(polygon))
            # GeoDataframe now comes in handy to remove points not belonging to the polygon provided
            self.df_all_data = self.df_all_data[self.df_all_data.geometry.within(polygon)]

            inside_polygon_rows = len(self.df_all_data)

            if underwater_rows - inside_polygon_rows > 0:

                print("{} points were removed due to geographical limits".format(underwater_rows - inside_polygon_rows))

        else:

            print("No polygon was specified as a filter")

        if self.df_all_data.isnull().all().all():

            raise Exception("log was filtered out")
        
        # Medium data is really not necessary after filtering
        self.df_all_data = self.df_all_data.drop('MEDIUM', axis=1)

        
    def write_to_file(self):

        if not self.df_all_data.isnull().all().all():  
            
            # Before writing to file let's add some general metadata
            metadata = {
            'system' : self.name,
            'data_created' : datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'time_coverage_start' : datetime.fromtimestamp(self.df_all_data['TIME'].min()),
            'time_coverage_end' : datetime.fromtimestamp(self.df_all_data['TIME'].max()),
            'geospatial_lat_min' : self.df_all_data['LATITUDE'].min(),
            'geospatial_lat_max' : self.df_all_data['LATITUDE'].max(),
            'geospatial_lon_min' : self.df_all_data['LONGITUDE'].min(),
            'geospatial_lon_max' : self.df_all_data['LONGITUDE'].max(),
            'geospatial_vertical_min' : self.df_all_data['DEPH'].min(),
            'geospatial_vertical_max' : self.df_all_data['DEPH'].max()
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

class netCDFExporter():

    def __init__(self, f:str) -> None:

        '''f is file name'''
        self.file_name = f
        self.data_attrs = {}
        self.coor_attrs = {}
        self.data_attrs = {}

        # Check for the metadata in the excel file
        with pd.ExcelFile("{}.xlsx".format(self.file_name), engine='openpyxl') as writer:
            
            # If it exists replace needed specific metadata
            if 'METADATA' in writer.sheet_names:
                
                self.data_df = pd.read_excel(writer, sheet_name='DATA')
                self.metadata_df = pd.read_excel(writer, sheet_name='METADATA')

            else:     
                print("Excel file has no Metadata")
                sys.exit()
        
        # Create a base xarray dataset
        self.xrds = xr.Dataset()
        # Load global attributes from json
        with open('metadata/global_attrs.json', 'r') as f:
            self.global_attrs = json.load(f)
        # Load coordinates attributes from json
        with open('metadata/coor_attrs.json', 'r') as f:
            self.coor_attrs = json.load(f)
        # Load data attributes from json
        with open('metadata/var_attrs.json', 'r') as f:
            self.data_attrs = json.load(f)
        # Save it in the dataset attrbutes
        #self.xrds.attrs = global_attrs
    
                
    def replace_json_metadata(self):
        
        if not 'date_created' in self.global_attrs:

            print("Something is wrong with the format of the global_attrs.json file")
        
        # Fill up Metadata of netCDF file
        else: 
            
            ## Global parameters 
            self.system_name = self.metadata_df['system']
            self.global_attrs['date_created'] = self.metadata_df['date_created'].iloc[0]
            self.global_attrs['time_coverage_start'] = self.metadata_df['time_coverage_start'].iloc[0]
            self.global_attrs['time_coverage_end'] = self.metadata_df['time_coverage_end'].iloc[0]
            self.global_attrs['geospatial_lat_min'] = self.metadata_df['geospatial_lat_min'].iloc[0]
            self.global_attrs['geospatial_lat_max'] = self.metadata_df['geospatial_lat_max'].iloc[0]
            self.global_attrs['geospatial_lon_min'] = self.metadata_df['geospatial_lon_min'].iloc[0]
            self.global_attrs['geospatial_lon_max'] = self.metadata_df['geospatial_lon_max'].iloc[0]
            self.global_attrs['geospatial_vertical_min'] = self.metadata_df['geospatial_vertical_min'].iloc[0]
            self.global_attrs['geospatial_vertical_max'] = self.metadata_df['geospatial_vertical_max'].iloc[0]
 
            self.xrds.attrs = self.global_attrs
        
        if not 'LATITUDE' in self.coor_attrs:

            print("Something is wrong with the format of the coor_attrs.json")
        
        else:

            for data in self.coor_attrs:
                
                self.xrds[str(data)].attrs = self.coor_attrs[str(data)]

        if not 'TEMP' in self.data_attrs:

            print("Something is wrong with the format of data_attrs.json")

        else: 

            for data in self.data_attrs:
                
                if str(data) in self.xrds.keys():
                    self.xrds[str(data)].attrs = self.data_attrs[str(data)]
            
        # If its lauv xplore-1 change a couple of the names of the sensors
        if 'lauv-xplore-1' in self.system_name:

            self.xrds['CNDC'].attrs['sdn_instrument_name'] = 'C-Metrec-X'
            self.xrds['TEMP'].attrs['sdn_instrument_name'] = 'T-Metrec-X'
            self.xrds['PSAL'].attrs['sdn_instrument_name'] = 'D-Metrec-X'
                                
    def build_netCDF(self):

        self.xrds = self.xrds.assign_coords(
            {
                'TIME' : ('TIME', pd.to_datetime(self.data_df['TIME'].unique(), unit='s') ),
                'LATITUDE' : ('TIME', self.data_df['LATITUDE'].astype(np.float32)),
                'LONGITUDE' : ('TIME', self.data_df['LONGITUDE'].astype(np.float32) )
            }
        )

        for data in self.data_df.columns:
            
            if str(data) != 'LATITUDE' and str(data) != 'LONGITUDE' and str(data) != 'geometry':

                self.xrds[str(data)] = ('TIME', self.data_df[str(data)])

    def to_netCDF(self):
        
        # Mission name
        mission_name = self.global_attrs['project']
        
        self.xrds.to_netcdf("{}.nc".format(self.file_name))
        print("netCDF {} sucessfully created".format(self.file_name))

    def print_netCDF(self):

        print(self.xrds.global_attrs)

   