import math
from typing import Final, Optional
import pandas as pd
import math
from datetime import datetime

from example.netCDF.core import locationType
from example.util.math import CircularMean, MeanStats, SlidingWindow, haversine, normalize_angle_mpi_pi
from example.util.units import *

import pyimc_generated as pg


class LogStats:

    def __init__(self, source_id: int = 0x0000, jump_time_millis: int = 10000, smooth_filter: bool = False, sliding_window_size: int = 10):
        self.BATTERIES_STR: Final[str] = 'Batteries'

        self.DISPLACEMENT_Z_STR: Final[str] = 'Displacement Z'
        self.VOLTAGE_STR: Final[str] = 'Voltage'
        self.CURRENT_STR: Final[str] = 'Current'
        self.TEMPERATURE_STR: Final[str] = 'Temperature'

        self._var_list_names = [self.DISPLACEMENT_Z_STR, self.VOLTAGE_STR, self.CURRENT_STR, self.TEMPERATURE_STR]

        self.units_mappings: dict = {}
        self.units_mappings[self.VOLTAGE_STR] = 'V'
        self.units_mappings[self.CURRENT_STR] = 'A'
        self.units_mappings[self.TEMPERATURE_STR] = '°C'
        self.units_mappings[self.DISPLACEMENT_Z_STR] = 'm'

        self._jump_time_millis = jump_time_millis

        self._smooth_filter = smooth_filter
        self._sliding_window_size = sliding_window_size

        self.log_names = []
        self._new_log_name = None
        self.log_days = []

        self.system_name = None
        self.source_id = source_id

        self.start_millis = -1
        self.end_millis = -1
        self.last_millis = -1
        self.cur_time_millis = -1

        self.min_lat_rads = float('NaN')
        self.max_lat_rads = float('NaN')
        self.avg_lat_rads = CircularMean(degrees=False)
        self.min_lon_rads = float('NaN')
        self.max_lon_rads = float('NaN')
        self.avg_lon_rads = CircularMean(degrees=False)

        self.min_height = float('NaN')
        self.max_height = float('NaN')
        self.avg_height = MeanStats()

        self.max_depth = float('NaN')
        self.avg_depth = MeanStats()

        self.max_alt = float('NaN')
        self.avg_alt = MeanStats()

        self.min_roll_rads = float('NaN')
        self.max_roll_rads = float('NaN')
        self.amp_roll_rads = float('NaN')
        self.avg_roll_rads = CircularMean(degrees=False)

        self.min_pitch_rads = float('NaN')
        self.max_pitch_rads = float('NaN')
        self.amp_pitch_rads = float('NaN')
        self.avg_pitch_rads = CircularMean(degrees=False)

        # calculate sideslip angle, min, max and avg from heading and speed
        self.min_sideslip_rads = float('NaN')
        self.max_sideslip_rads = float('NaN')
        self.avg_sideslip_rads = CircularMean(degrees=False)

        self.num_states = 0
        self.number_of_logs = 0

        self.distance = 0.0  # Total distance traveled in meters
        # smoothed
        self.distance_smoothed = 0.0  # Total distance traveled in meters

        self.last_location = None
        self._last_lat_rads_sliding_window = SlidingWindow(window_size=self._sliding_window_size)
        self._last_lon_rads_sliding_window = SlidingWindow(window_size=self._sliding_window_size)

        self.avg_speed_mps_calc = float('NaN')
        # smoothed
        self.avg_speed_smoothed_mps_calc = float('NaN')

        self.min_speed_mps = float('NaN')
        self.max_speed_mps = float('NaN')
        self.avg_speed_mps = MeanStats()
        # smoothed
        self.min_speed_smoothed_mps = float('NaN')
        self.max_speed_smoothed_mps = float('NaN')
        self.avg_speed_smoothed_mps = MeanStats()
        self._speed_sliding_window = SlidingWindow(window_size=self._sliding_window_size)

        self.min_vspeed_mps = float('NaN')
        self.max_vspeed_mps = float('NaN')
        self.avg_vspeed_mps = MeanStats()
        # smoothed
        self.min_vspeed_smoothed_mps = float('NaN')
        self.max_vspeed_smoothed_mps = float('NaN')
        self.avg_vspeed_smoothed_mps = MeanStats()
        self._vspeed_sliding_window = SlidingWindow(window_size=self._sliding_window_size)

        # Dictonary for other variables (min, max, avg)
        self.variables = {}
        self._variables_numeric_tmp = {}

        self.voltage_entities = []
        self.current_entities = []
        self.temperature_entities = []
        self.displacement_z_entities = []

        self.entities_mappings = {}

        ## Final stats need final wrap up calculations

        self.duration_traveled_millis = 0.0
        self._duration_traveled_millis_tmp = float('NaN')

        # Duration calculations
        self.duration_in_seconds = float('NaN')
        # Separate parts of the duration for better readability
        self.duration_days_part = float('NaN')
        self.duration_hours_part = float('NaN')
        self.duration_minutes_part = float('NaN')
        self.duration_seconds_part = float('NaN')


    def _initialize_variable_list(self):
        if self.variables is None:
            self.variables = {}
        if self._variables_numeric_tmp is None:
            self._variables_numeric_tmp = {}
        
        for var_name in self._var_list_names:
            if self.variables.get(var_name, None) is None:
                self.variables[var_name] = {}
            if self._variables_numeric_tmp.get(var_name, None) is None:
                self._variables_numeric_tmp[var_name] = {}


    def new_log_name(self, new_log_name):
        """
        Set a new log name for the statistics.
        This is used to differentiate between multiple logs.
        """
        if new_log_name is not None:
            self._new_log_name = new_log_name
        else:
            self._new_log_name = "?"


    def update_name(self, msg, callback=None):
        """
        Update the statistics with a new message.
        This method is called by the subscriber when a new message is received.
        """
        if not isinstance(msg, pg.messages.Announce):
            return
        if self.source_id == 0x0000 or msg._header.src != self.source_id or self.system_name is not None:
            return
        if (msg.sys_name is None or msg.sys_name == ""):
            return
        
        self.system_name = msg.sys_name
        
    def update_state(self, msg, callback=None):
        if not isinstance(msg, pg.messages.EstimatedState):
            return
        
        #debug(1)
        #debug("Source ID: {}, Message Source: {}".format(self.sourceId, msg._header.src))
        if msg._header.src != self.source_id:
            return

        if self.last_location is not None:
            if self.cur_time_millis - self.last_millis > self._jump_time_millis:
                self._last_lat_rads_sliding_window.clear()
                self._last_lon_rads_sliding_window.clear()
                self._speed_sliding_window.clear()
                self._vspeed_sliding_window.clear()

        if self.last_location is None:
            self._last_lat_rads_sliding_window.clear()
            self._last_lon_rads_sliding_window.clear()
            self._speed_sliding_window.clear()
            self._vspeed_sliding_window.clear()


        height = msg.height
        self.min_height = min(self.min_height, height) if not math.isnan(self.min_height) else height
        self.max_height = max(self.max_height, height) if not math.isnan(self.max_height) else height
        self.avg_height.update(height)

        if msg.depth > 0:
            depth = msg.depth
            self.max_depth = max(self.max_depth, depth) if not math.isnan(self.max_depth) else depth
            self.avg_depth.update(depth)

        if msg.alt > 0:
            alt = msg.alt
            self.max_alt = max(self.max_alt, alt) if not math.isnan(self.max_alt) else alt
            self.avg_alt.update(alt)

        phi = msg.phi
        theta = msg.theta
        
        self.max_roll_rads = max(self.max_roll_rads, phi) if not math.isnan(self.max_roll_rads) else phi
        self.min_roll_rads = min(self.min_roll_rads, phi) if not math.isnan(self.min_roll_rads) else phi
        self.avg_roll_rads.update(phi)
        
        self.max_pitch_rads = max(self.max_pitch_rads, theta) if not math.isnan(self.max_pitch_rads) else theta
        self.min_pitch_rads = min(self.min_pitch_rads, theta) if not math.isnan(self.min_pitch_rads) else theta
        self.avg_pitch_rads.update(theta)

        vspeed = msg.vz
        self.max_vspeed_mps = max(self.max_vspeed_mps, vspeed) if not math.isnan(self.max_vspeed_mps) else vspeed
        self.min_vspeed_mps = min(self.min_vspeed_mps, vspeed) if not math.isnan(self.min_vspeed_mps) else vspeed
        self.avg_vspeed_mps.update(vspeed)
        self._vspeed_sliding_window.update(vspeed)
        vspeed_smoothed = self._vspeed_sliding_window.mean()
        self.max_vspeed_smoothed_mps = max(self.max_vspeed_smoothed_mps, vspeed_smoothed) if not math.isnan(self.max_vspeed_smoothed_mps) else vspeed_smoothed
        self.min_vspeed_smoothed_mps = min(self.min_vspeed_smoothed_mps, vspeed) if not math.isnan(self.min_vspeed_smoothed_mps) else vspeed_smoothed
        self.avg_vspeed_smoothed_mps.update(vspeed_smoothed)

        hspeed = math.sqrt(msg.vx** 2 + msg.vy ** 2)
        self.max_speed_mps = max(self.max_speed_mps, hspeed) if not math.isnan(self.max_speed_mps) else hspeed
        self.min_speed_mps = min(self.min_speed_mps, hspeed) if not math.isnan(self.min_speed_mps) else hspeed
        self.avg_speed_mps.update(hspeed)
        self._speed_sliding_window.update(hspeed)
        hspeed_smoothed = self._speed_sliding_window.mean() if self._smooth_filter else hspeed
        self.max_speed_smoothed_mps = max(self.max_speed_smoothed_mps, hspeed_smoothed) if not math.isnan(self.max_speed_smoothed_mps) else hspeed_smoothed
        self.min_speed_smoothed_mps = min(self.min_speed_smoothed_mps, hspeed) if not math.isnan(self.min_speed_smoothed_mps) else hspeed_smoothed
        self.avg_speed_smoothed_mps.update(hspeed_smoothed)
        
        # calculate sideslip angle, min, max and avg from heading and speed
        sideslip_rads = math.atan2(msg.vy, msg.vx)
        sideslip_rads -= msg.psi
        sideslip_rads = normalize_angle_mpi_pi(sideslip_rads)
        self.min_sideslip_rads = min(self.min_sideslip_rads, sideslip_rads) if not math.isnan(self.min_sideslip_rads) else sideslip_rads
        self.max_sideslip_rads = max(self.max_sideslip_rads, sideslip_rads) if not math.isnan(self.max_sideslip_rads) else sideslip_rads
        self.avg_sideslip_rads.update(sideslip_rads)

        self.cur_time_millis = msg._header.timestamp * 1000.0  # Convert to milliseconds

        log_day = pd.to_datetime(self.cur_time_millis, unit='ms', utc=True).strftime('%Y%m%d')
        # append to logDays if not already present
        if log_day not in self.log_days:
            self.log_days.append(log_day)
        if self._new_log_name is not None:
            self.log_names.append("{}/{}".format(log_day, self._new_log_name))
            self._new_log_name = None

        if self.last_location is not None:
            if self.cur_time_millis - self.last_millis > self._jump_time_millis:
                self.last_location = None
                if not math.isnan(self._duration_traveled_millis_tmp):
                    self.duration_traveled_millis += self._duration_traveled_millis_tmp
                    self._duration_traveled_millis_tmp = float('NaN')
            else:
                self._duration_traveled_millis_tmp += self.cur_time_millis - self.last_millis

        if self.last_location is None:
            self.last_location = locationType()
            self.last_location.__init__()
            self.last_location.fill_it(msg)
            self.last_location.lat = msg.lat
            self.last_location.lon = msg.lon
            self.last_location.translate_positions(msg.x, msg.y, msg.z)
            self.last_location.add_offsets()

            self.min_lat_rads = msg.lat if math.isnan(self.min_lat_rads) else min(self.min_lat_rads, self.last_location.lat)
            self.max_lat_rads = msg.lat if math.isnan(self.max_lat_rads) else max(self.max_lat_rads, self.last_location.lat)
            self.avg_lat_rads.update(self.last_location.lat)
            self.min_lon_rads = msg.lon if math.isnan(self.min_lon_rads) else min(self.min_lon_rads, self.last_location.lon)
            self.max_lon_rads = msg.lon if math.isnan(self.max_lon_rads) else max(self.max_lon_rads, self.last_location.lon)
            self.avg_lon_rads.update(self.last_location.lon)
            
            self.last_millis = msg._header.timestamp * 1000.0  # Convert to milliseconds
            self.start_millis = msg._header.timestamp * 1000.0 if self.start_millis < 0 else self.start_millis  # Convert to milliseconds
            self.end_millis = msg._header.timestamp * 1000.0  # Convert to milliseconds
            
            if not math.isnan(self._duration_traveled_millis_tmp):
                self.duration_traveled_millis += self._duration_traveled_millis_tmp
                self._duration_traveled_millis_tmp = float('NaN')
            self._duration_traveled_millis_tmp = 0
            
            self._last_lat_rads_sliding_window.update(self.last_location.lat)
            self._last_lon_rads_sliding_window.update(self.last_location.lon)

            self.num_states += 1
            return

        if self.start_millis < 0:
            self.start_millis = self.cur_time_millis
        if self.end_millis < 0:
            self.end_millis = self.cur_time_millis

        self.cur_location = locationType()
        self.cur_location.__init__()
        self.cur_location.fill_it(msg)
        self.cur_location.lat = msg.lat
        self.cur_location.lon = msg.lon
        self.cur_location.translate_positions(msg.x, msg.y, msg.z)
        self.cur_location.add_offsets()

        self.min_lat_rads = msg.lat if math.isnan(self.min_lat_rads) else min(self.min_lat_rads, self.cur_location.lat)
        self.max_lat_rads = msg.lat if math.isnan(self.max_lat_rads) else max(self.max_lat_rads, self.cur_location.lat)
        self.avg_lat_rads.update(self.cur_location.lat)
        self.min_lon_rads = msg.lon if math.isnan(self.min_lon_rads) else min(self.min_lon_rads, self.cur_location.lon)
        self.max_lon_rads = msg.lon if math.isnan(self.max_lon_rads) else max(self.max_lon_rads, self.cur_location.lon)
        self.avg_lon_rads.update(self.cur_location.lon)

        #distH = self.curLocation.getHorizontalDistanceInMeters(self.lastLocation)
        dist = haversine(self.last_location.lat, self.last_location.lon, self.cur_location.lat, self.cur_location.lon, degrees=False)
        #print("Distance between {}° {}° and {}° {}°: {}m vs {}m".format(math.degrees(self.lastLocation.lat), math.degrees(self.lastLocation.lon), math.degrees(self.curLocation.lat), math.degrees(self.curLocation.lon), dist, distH))
        self.distance += dist

        if self._smooth_filter:
            smoothed_last_loc = locationType()
            smoothed_last_loc.__init__()
            smoothed_last_loc.lat = self._last_lat_rads_sliding_window.mean()
            smoothed_last_loc.lon = self._last_lon_rads_sliding_window.mean()
            self._last_lat_rads_sliding_window.update(self.cur_location.lat)
            self._last_lon_rads_sliding_window.update(self.cur_location.lon)
            smoothed_cur_loc = locationType()
            smoothed_cur_loc.__init__()
            smoothed_cur_loc.lat = self._last_lat_rads_sliding_window.mean()
            smoothed_cur_loc.lon = self._last_lon_rads_sliding_window.mean()
            #distH = smoothed_cur_loc.getHorizontalDistanceInMeters(self.smoothed_last_loc)
            dist_smoothed = haversine(smoothed_last_loc.lat, smoothed_last_loc.lon, smoothed_cur_loc.lat, smoothed_cur_loc.lon, degrees=False)
            self.distance_smoothed += dist_smoothed

        self.last_location = self.cur_location
        self.end_millis = self.cur_time_millis
        self.last_millis = self.cur_time_millis
        self.num_states += 1

    # add lambda to extract value from message to the arguments of this function, defaul to msg.value
    def _update_variable_value(self, msg, type_match, variable_entities, var_str: str, value_extractor=lambda msg: msg.value):
        if not isinstance(msg, type_match):
            return
        if msg._header.src != self.source_id:
            return
        if variable_entities is None or len(variable_entities) == 0:
            return

        self._initialize_variable_list()
        variable_dic = self.variables[var_str]
        variable_tmp_dic = self._variables_numeric_tmp[var_str]

        source_ent = msg._header.src_ent
        source_ent_name: Optional[str] = None
        
        # search entitiesMappings by the name for source_ent (the dicionary is name vs source_ent)
        for name, entity_id in self.entities_mappings.items():
            if entity_id == source_ent:
                source_ent_name = name
                break

        entry_elem = None
        if source_ent_name is not None and source_ent_name != '':
            if source_ent_name not in variable_dic:
                variable_dic[source_ent_name] = {}
            entry_elem = variable_dic[source_ent_name]
        else:
            if source_ent not in variable_tmp_dic:
                variable_tmp_dic[source_ent] = {}
            entry_elem = variable_tmp_dic[source_ent]
        if entry_elem is None:
            return
        if len(entry_elem) == 0:
            # initialize
            entry_elem['min'] = float('NaN')
            entry_elem['max'] = float('NaN')
            entry_elem['avg'] = MeanStats()

        variable_value = value_extractor(msg) # msg.value
        entry_elem['min'] = min(entry_elem['min'], variable_value) if not math.isnan(entry_elem['min']) else variable_value
        entry_elem['max'] = max(entry_elem['max'], variable_value) if not math.isnan(entry_elem['max']) else variable_value
        entry_elem['avg'].update(variable_value)

    def update_voltage(self, msg, callback=None):
        self._update_variable_value(msg, pg.messages.Voltage, self.voltage_entities, self.VOLTAGE_STR)

    def update_current(self, msg, callback=None):
        self._update_variable_value(msg, pg.messages.Current, self.current_entities, self.CURRENT_STR)
    
    def update_temperature(self, msg, callback=None):
        self._update_variable_value(msg, pg.messages.Temperature, self.temperature_entities, self.TEMPERATURE_STR)

    def update_displacement_z(self, msg, callback=None):
        self._update_variable_value(msg, pg.messages.Displacement, self.displacement_z_entities, self.DISPLACEMENT_Z_STR, lambda msg: msg.z)        

    def map_unnamed_variables_to_named(self):
        self._initialize_variable_list()

        def _merge(list_entities, dic_entities_mappings, dic_variables, dic_variables_tmp):
            if list_entities is None or len(list_entities) == 0:
                return
            
            def _update_map(entity_name, entity_id):
                if entity_id is None:
                    return
                var_tmp = dic_variables_tmp.get(entity_id, None)
                if var_tmp is None:
                    return
                var = dic_variables.get(entity_name, None)
                if var is None:
                    dic_variables[entity_name] = var_tmp
                    dic_variables_tmp.pop(entity_id, None)
                else:
                    var['min'] = min(var['min'], var_tmp['min']) if not math.isnan(var['min']) else var_tmp['min']
                    var['max'] = max(var['max'], var_tmp['max']) if not math.isnan(var['max']) else var_tmp['max']
                    var['avg'].merge_with(var_tmp['avg'])
                    
            if dic_entities_mappings is not None and len(dic_entities_mappings) > 0:
                if len(list_entities) == 1 and list_entities[0] == '*':
                    # Accept all entities
                    for entity_name, entity_id in dic_entities_mappings.items():
                        _update_map(entity_name, entity_id)
                else:
                    for entity_name in list_entities:
                        entity_id = dic_entities_mappings.get(entity_name, None)
                        _update_map(entity_name, entity_id)

        # Voltage
        _merge(self.voltage_entities, self.entities_mappings,
               self.variables[self.VOLTAGE_STR], self._variables_numeric_tmp[self.VOLTAGE_STR])
        # Current
        _merge(self.current_entities, self.entities_mappings,
               self.variables[self.CURRENT_STR], self._variables_numeric_tmp[self.CURRENT_STR])
        # Temperature
        _merge(self.temperature_entities, self.entities_mappings,
               self.variables[self.TEMPERATURE_STR], self._variables_numeric_tmp[self.TEMPERATURE_STR])
        # Displacement Z
        _merge(self.displacement_z_entities, self.entities_mappings,
               self.variables[self.DISPLACEMENT_Z_STR], self._variables_numeric_tmp[self.DISPLACEMENT_Z_STR])
        
        self._variables_numeric_tmp.clear()
        # self._initialize_variable_list()


    def finalize(self):
        """Final calculations for the statistics."""
        if self.start_millis < 0 or self.end_millis < 0:
            print("No data found in the logs.")
            return

        if not math.isnan(self._duration_traveled_millis_tmp):
            self.duration_traveled_millis += self._duration_traveled_millis_tmp
            self._duration_traveled_millis_tmp = float('NaN')
        
        # set logDay as yyyyMMdd
        #logDay = pd.to_datetime(self.startMillis, unit='ms', utc=True).strftime('%Y%m%d')
        #if self.logName != "":
        #    self.logName = "{}/{}".format(logDay, self.logName)
        # self.durationInSeconds = (self.endMillis - self.startMillis) / 1000.0  # Convert to seconds
        self.duration_in_seconds = self.duration_traveled_millis / 1000.0  # Convert to seconds
        duration_seconds_part_tmp = self.duration_in_seconds
        self.duration_days_part = int(duration_seconds_part_tmp // (24 * 3600))
        duration_seconds_part_tmp %= (24 * 3600)
        self.duration_hours_part = int(duration_seconds_part_tmp // 3600)
        duration_seconds_part_tmp %= 3600
        self.duration_minutes_part = int(duration_seconds_part_tmp // 60)
        self.duration_seconds_part = duration_seconds_part_tmp % 60

        if not math.isnan(self.max_roll_rads) and not math.isnan(self.min_roll_rads):
            self.amp_roll_rads = self.max_roll_rads - self.min_roll_rads

        if not math.isnan(self.max_pitch_rads) and not math.isnan(self.min_pitch_rads):
            self.amp_pitch_rads = self.max_pitch_rads - self.min_pitch_rads

        if not math.isnan(self.max_sideslip_rads) and not math.isnan(self.min_sideslip_rads):
            self.amp_sideslip_rads = self.max_sideslip_rads - self.min_sideslip_rads

        #if (self.endMillis - self.startMillis) > 0:
        #    self.avgSpeedMpsCalc = self.distance / ((self.endMillis - self.startMillis) / 1000.0)
        if (self.duration_traveled_millis) > 0:
            self.avg_speed_mps_calc = self.distance / (self.duration_traveled_millis / 1000.0)
            self.avg_speed_smoothed_mps_calc = self.distance_smoothed / (self.duration_traveled_millis / 1000.0)
        else:
            self.avg_speed_mps_calc = 0
            self.avg_speed_smoothed_mps_calc = 0
        
        # order in ascending order the self.variables acording to the key
        # self.variables = dict(sorted(self.variables.items()))
        # sort the values acording to thir keys
        for key, value in self.variables.items():
            value = dict(sorted(value.items()))


    def __str__(self):
        """String representation of the statistics."""
        output = []
        justify = 37
        output.append("{}: {}".format("Log Name".ljust(justify), ", ".join(self.log_names)))
        output.append("{}: {}".format("Log Days".ljust(justify), ", ".join(self.log_days)))
        
        if self.system_name is not None:
            output.append("{}: {} | Source ID: {} (0x{:04X})".format("System Name".ljust(justify), self.system_name, self.source_id, self.source_id))
        else:
            output.append("{}: | Source ID: {} (0x{:04X})".format("System Name".ljust(justify), self.source_id, self.source_id))

        if self.start_millis < 0 or self.end_millis < 0:
            output.append("No data found in the logs.")
        else:
            output.append("{}: {}".format("Start time".ljust(justify), pd.to_datetime(self.start_millis, unit='ms', utc=True)))
            output.append("{}: {}".format("End time".ljust(justify), pd.to_datetime(self.end_millis, unit='ms', utc=True)))
            output.append("{}: {} days, {} hours, {} minutes, {:.3f} seconds".format(
                "Duration".ljust(justify), self.duration_days_part, self.duration_hours_part, self.duration_minutes_part, self.duration_seconds_part))

            output.append("{}: {:.1f} m | {:.2f} NM".format("Distance".ljust(justify), self.distance, self.distance * METERS_TO_NM))
            if self._smooth_filter:
                output.append("{}: {:.1f} m | {:.2f} NM".format("Distance Smoothed".ljust(justify), self.distance_smoothed, self.distance_smoothed * METERS_TO_NM))
            
            if self._smooth_filter:
                output.append("{}: {} elements".format("Smooth Window".ljust(justify), self._sliding_window_size))

            avg_speed_knots_calc = self.avg_speed_mps_calc * MPS_TO_KNOTS
            output.append("{}: {:.2f} m/s : {:.2f} kn".format("Average Speed Calc by Time".ljust(justify), self.avg_speed_mps_calc, avg_speed_knots_calc))

            if self._smooth_filter:
                avg_speed_Knots_calc_smoothed = self.avg_speed_smoothed_mps_calc * MPS_TO_KNOTS
                output.append("{}: {:.2f} m/s : {:.2f} kn".format("Average Speed Calc by Time Smoothed".ljust(justify), self.avg_speed_smoothed_mps_calc, avg_speed_Knots_calc_smoothed))

            output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                "Speed".ljust(justify), self.min_speed_mps, self.min_speed_mps * MPS_TO_KNOTS, 
                self.max_speed_mps, self.max_speed_mps * MPS_TO_KNOTS, 
                self.avg_speed_mps.mean(), self.avg_speed_mps.mean() * MPS_TO_KNOTS, 
                self.avg_speed_mps.std_dev(), self.avg_speed_mps.std_dev() * MPS_TO_KNOTS))
            if self._smooth_filter:
                output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                    "Speed Smoothed".ljust(justify), self.min_speed_smoothed_mps, self.min_speed_smoothed_mps * MPS_TO_KNOTS, 
                    self.max_speed_smoothed_mps, self.max_speed_smoothed_mps * MPS_TO_KNOTS, 
                    self.avg_speed_smoothed_mps.mean(), self.avg_speed_smoothed_mps.mean() * MPS_TO_KNOTS, 
                    self.avg_speed_smoothed_mps.std_dev(), self.avg_speed_smoothed_mps.std_dev() * MPS_TO_KNOTS))


            if not math.isnan(self.min_vspeed_mps) and not math.isnan(self.max_vspeed_mps) \
                    and self.min_vspeed_mps == self.max_vspeed_mps and self.max_vspeed_mps > 0:
                output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                    "Vertical Speed".ljust(justify), self.min_vspeed_mps, self.min_vspeed_mps * MPS_TO_KNOTS, 
                    self.max_vspeed_mps, self.max_vspeed_mps * MPS_TO_KNOTS, 
                    self.avg_vspeed_mps.mean(), self.avg_vspeed_mps.mean() * MPS_TO_KNOTS, 
                    self.avg_vspeed_mps.std_dev(), self.avg_vspeed_mps.std_dev() * MPS_TO_KNOTS))
                if self._smooth_filter:
                    output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                        "Vertical Speed Smoothed".ljust(justify), self.min_vspeed_smoothed_mps, self.min_vspeed_smoothed_mps * MPS_TO_KNOTS, 
                        self.max_vspeed_smoothed_mps, self.max_vspeed_smoothed_mps * MPS_TO_KNOTS, 
                        self.avg_vspeed_smoothed_mps.mean(), self.avg_vspeed_smoothed_mps.mean() * MPS_TO_KNOTS, 
                        self.avg_vspeed_smoothed_mps.std_dev(), self.avg_vspeed_smoothed_mps.std_dev() * MPS_TO_KNOTS))

            if not math.isnan(self.min_lat_rads) and not math.isnan(self.max_lat_rads):
                output.append("{}: Min: {:.7f}° | Max: {:.7f}° | Avg: {:.7f}° | Std Dev: {:.7f}°".format(
                    "Latitude".ljust(justify), math.degrees(self.min_lat_rads), math.degrees(self.max_lat_rads), 
                    math.degrees(self.avg_lat_rads.mean()), math.degrees(self.avg_lat_rads.std_dev())))
            if not math.isnan(self.min_lon_rads) and not math.isnan(self.max_lon_rads):
                output.append("{}: Min: {:.7f}° | Max: {:.7f}° | Avg: {:.7f}° | Std Dev: {:.7f}°".format(
                    "Longitude".ljust(justify), math.degrees(self.min_lon_rads), math.degrees(self.max_lon_rads), 
                    math.degrees(self.avg_lon_rads.mean()), math.degrees(self.avg_lon_rads.std_dev())))

            if not math.isnan(self.max_height):
                output.append("{}: Min: {:.1f} m | Max: {:.1f} m | Avg: {:.1f} m | Std Dev: {:.1f} m".format(
                    "Height".ljust(justify), self.min_height, self.max_height, self.avg_height.mean(), self.avg_height.std_dev()))
                
            if not math.isnan(self.max_depth) and self.max_depth > 0:
                output.append("{}: Max: {:.1f} m | Avg: {:.1f} m | Std Dev: {:.1f} m".format(
                    "Depth".ljust(justify), self.max_depth, math.degrees(self.avg_depth.mean()), math.degrees(self.avg_depth.std_dev())))
            
            if not math.isnan(self.max_alt) and self.max_alt > 0:
                output.append("{}: Max: {:.1f} m | Avg: {:.1f} m | Std Dev: {:.1f} m".format(
                    "Altitude".ljust(justify), self.max_alt, self.avg_alt.mean(), self.avg_alt.std_dev()))

            if not math.isnan(self.max_roll_rads):
                output.append("{}: Min: {:.2f}° | Max: {:.2f}° | Amp: {:.2f}° | Avg: {:.2f}° | Std Dev: {:.2f}°".format(
                    "Roll".ljust(justify), math.degrees(self.min_roll_rads), math.degrees(self.max_roll_rads), 
                    math.degrees(self.amp_roll_rads), math.degrees(self.avg_roll_rads.mean()), 
                    math.degrees(self.avg_roll_rads.std_dev())))

            if not math.isnan(self.max_pitch_rads):
                output.append("{}: Min: {:.2f}° | Max: {:.2f}° | Amp: {:.2f}° | Avg: {:.2f}° | Std Dev: {:.2f}°".format(
                    "Pitch".ljust(justify), math.degrees(self.min_pitch_rads), math.degrees(self.max_pitch_rads), 
                    math.degrees(self.amp_pitch_rads), math.degrees(self.avg_pitch_rads.mean()), 
                    math.degrees(self.avg_pitch_rads.std_dev())))

            if not math.isnan(self.max_sideslip_rads):
                output.append("{}: Min: {:.2f}° | Max: {:.2f}° | Amp: {:.2f}° | Avg: {:.2f}° | Std Dev: {:.2f}°".format(
                    "Sideslip".ljust(justify), math.degrees(self.min_sideslip_rads), math.degrees(self.max_sideslip_rads), 
                    math.degrees(self.amp_sideslip_rads), math.degrees(self.avg_sideslip_rads.mean()), 
                    math.degrees(self.avg_sideslip_rads.std_dev())))
                
            # output.append("{}: {:.3f} s".format("Average Time Between States".ljust(justify), (self.endMillis - self.startMillis) / self.numStates / 1000))
            output.append("{}: {:.3f} s".format("Average Time Between States".ljust(justify), self.duration_traveled_millis / self.num_states / 1000))
            output.append("{}: {}".format("Number of States".ljust(justify), self.num_states))
            output.append("{}: {}".format("Number of Log Files".ljust(justify), self.number_of_logs))

            if self.variables is not None and len(self.variables) > 0:
                section_variables_written = False
                for section_name in self.variables:
                    section_name_written = False
                    variables = self.variables[section_name]
                    if variables is None or len(variables) == 0:
                        continue
                    for entity in variables:
                        var = variables[entity]
                        if var is None:
                            continue
                        unit_str = self.units_mappings.get(section_name, '')
                        unit_str = " {}".format(unit_str) if unit_str != "" else ""

                        if not section_variables_written:
                            output.append("{}:".format("Variables".ljust(justify)))
                            section_variables_written = True
                        if not section_name_written:
                            output.append("  {}:".format("{}".format(section_name).ljust(justify)))
                            section_name_written = True

                        output.append("    {}: Min: {:.2f}{} | Max: {:.2f}{} | Amp: {:.2f}{} | Avg: {:.2f}{} | Std Dev: {:.2f}{}".format(
                            "{}".format(entity).ljust(justify),
                            var['min'], unit_str,
                            var['max'], unit_str,
                            var['max'] - var['min'], unit_str,
                            var['avg'].mean(), unit_str,
                            var['avg'].std_dev(), unit_str))

        return "\n".join(output)
    

    def write_to_file(self, writer: pd.ExcelWriter, sheet_name: str = 'Statistics', reuse_sheet: bool = False):
        """
        Write the statistics to a file in a human-readable format.
        
        :param file_path: The path to the file where the statistics will be written.
        """

        sheet_name = sheet_name.replace("/", "-")
        sheet_name_31_chars = sheet_name[:31]

        #debug("Writing to file")
        workbook = writer.book 

        # Add cell formats for styling
        title_format = workbook.add_format({'bold': True, 'font_size': 16})
        header_format = workbook.add_format({'bold': True, 'font_size': 13})
        header_value_format = workbook.add_format({'font_size': 13})
        name_format = workbook.add_format({'bold': True, 'font_size': 11})
        value_format = workbook.add_format({'font_size': 11})
        unit_format = workbook.add_format({'italic': True, 'font_size': 10})

        sheet_line = 0
        sheet = workbook.get_worksheet_by_name(sheet_name_31_chars) if reuse_sheet else None
        if sheet is None:
            sheet = workbook.add_worksheet(sheet_name_31_chars)

        # Set column widths to make the report more readable.
        # The width is an approximation of the number of characters.
        sheet.set_column('A:A', 37)
        sheet.set_column('B:B', 37)
        sheet.set_column('C:C', 37)
        sheet.set_column('D:D', 37)
        sheet.set_column('E:Z', 37)

        sheet.write(sheet_line, 0, "Log Statistics", title_format)
        sheet_line += 1

        sheet.write(sheet_line, 0, "System", header_format)
        sheet.write(sheet_line, 1, self.system_name if self.system_name is not None else "Unknown", header_format)
        sheet.write(sheet_line, 3, "System ID", header_format)
        sheet.write(sheet_line, 4, "0x{:04X}".format(self.source_id), header_format)
        sheet.write(sheet_line, 5, "{}".format(self.source_id), header_format)
        sheet_line += 2

        sheet.write(sheet_line, 0, "Data Created", header_format)
        sheet.write(sheet_line, 1, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), header_value_format)
        sheet_line += 2

        sheet.write(sheet_line, 0, "Log Names", name_format)
        #sheet.write(sheetLine, 1, ", ".join(self.logNames) if self.logNames else "No logs found", value_format)
        idx = 0
        for v in self.log_names:
            sheet.write(sheet_line, 1 + idx, v, value_format)
            idx += 1
        sheet_line += 1
        sheet.write(sheet_line, 0, "Log Days", name_format)
        # sheet.write(sheetLine, 1, ", ".join(self.logDays) if self.logDays else "No log days found", value_format)
        idx = 0
        for v in self.log_days:
            sheet.write(sheet_line, 1 + idx, v, value_format)
            idx += 1
        sheet_line += 1
        sheet.write(sheet_line, 0, "Number of Log Files", name_format)
        sheet.write(sheet_line, 1, len(self.log_names), value_format)
        sheet_line += 2

        if self.start_millis < 0 or self.end_millis < 0:
            sheet.write(sheet_line, 0, "No data found in the logs.", title_format)
            sheet_line += 1
            return

        sheet.write(sheet_line, 0, "Start Time", header_format)
        sheet.write(sheet_line, 1, pd.to_datetime(self.start_millis, unit='ms', utc=True).strftime('%Y-%m-%dT%H:%M:%SZ'), header_value_format)
        sheet.write(sheet_line, 2, pd.to_datetime(self.start_millis, unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S'), header_value_format)
        sheet.write(sheet_line, 3, self.start_millis, header_value_format)
        sheet.write(sheet_line, 4, "ms", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 0, "End Time", header_format)
        sheet.write(sheet_line, 1, pd.to_datetime(self.end_millis, unit='ms', utc=True).strftime('%Y-%m-%dT%H:%M:%SZ'), header_value_format)
        sheet.write(sheet_line, 2, pd.to_datetime(self.end_millis, unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S'), header_value_format)
        sheet.write(sheet_line, 3, self.end_millis, header_value_format)
        sheet.write(sheet_line, 4, "ms", unit_format)
        sheet_line += 1
        
        sheet.write(sheet_line, 0, "Duration", header_format)
        sheet.write(sheet_line, 1, self.duration_days_part, header_value_format)
        sheet.write(sheet_line, 2, "days", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 1, self.duration_hours_part, header_value_format)
        sheet.write(sheet_line, 2, "hours", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 1, self.duration_minutes_part, header_value_format)
        sheet.write(sheet_line, 2, "minutes", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 1, self.duration_seconds_part, header_value_format)
        sheet.write(sheet_line, 2, "seconds", unit_format)
        sheet_line += 2

        sheet.write(sheet_line, 0, "distance_traveled", header_format)
        sheet.write(sheet_line, 1, self.distance, header_value_format)
        sheet.write(sheet_line, 2, "m", unit_format)
        sheet.write(sheet_line, 3, "distance_traveled_nm", header_format)
        sheet.write(sheet_line, 4, self.distance * METERS_TO_NM, header_value_format)
        sheet.write(sheet_line, 5, "NM", unit_format)
        sheet_line += 2
        if self._smooth_filter:
            sheet.write(sheet_line, 0, "distance_traveled_smoothed", header_format)
            sheet.write(sheet_line, 1, self.distance_smoothed, header_value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet.write(sheet_line, 3, "distance_traveled_smoothed_nm", header_format)
            sheet.write(sheet_line, 4, self.distance_smoothed * METERS_TO_NM, header_value_format)
            sheet.write(sheet_line, 5, "NM", unit_format)
            sheet_line += 2

        if self._smooth_filter:
            sheet.write(sheet_line, 0, "smooth_window", name_format)
            sheet.write(sheet_line, 1, self._sliding_window_size, value_format)
            sheet_line += 2

        sheet.write(sheet_line, 0, "speed_calc_avg", header_format)
        sheet.write(sheet_line, 1, self.avg_speed_mps_calc, header_value_format)
        sheet.write(sheet_line, 2, "m/s", unit_format)
        sheet.write(sheet_line, 3, "speed_calc_avg_kn", header_format)
        sheet.write(sheet_line, 4, self.avg_speed_mps_calc * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheet_line, 5, "kn", unit_format)
        sheet_line += 2
        if self._smooth_filter:
            sheet.write(sheet_line, 0, "speed_calc_smoothed_avg", header_format)
            sheet.write(sheet_line, 1, self.avg_speed_smoothed_mps_calc, header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "speed_calc_smoothed_avg_kn", header_format)
            sheet.write(sheet_line, 4, self.avg_speed_smoothed_mps_calc * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 2

        sheet.write(sheet_line, 0, "speed_min", header_format)
        sheet.write(sheet_line, 1, self.min_speed_mps, header_value_format)
        sheet.write(sheet_line, 2, "m/s", unit_format)
        sheet.write(sheet_line, 3, "speed_min_kn", header_format)
        sheet.write(sheet_line, 4, self.min_speed_mps * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheet_line, 5, "kn", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 0, "speed_max", header_format)
        sheet.write(sheet_line, 1, self.max_speed_mps, header_value_format)
        sheet.write(sheet_line, 2, "m/s", unit_format)
        sheet.write(sheet_line, 3, "speed_max_kn", header_format)
        sheet.write(sheet_line, 4, self.max_speed_mps * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheet_line, 5, "kn", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 0, "speed_avg", header_format)
        sheet.write(sheet_line, 1, self.avg_speed_mps.mean(), header_value_format)
        sheet.write(sheet_line, 2, "m/s", unit_format)
        sheet.write(sheet_line, 3, "speed_avg_kn", header_format)
        sheet.write(sheet_line, 4, self.avg_speed_mps.mean() * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheet_line, 5, "kn", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 0, "speed_std_dev", header_format)
        sheet.write(sheet_line, 1, self.avg_speed_mps.std_dev(), header_value_format)
        sheet.write(sheet_line, 2, "m/s", unit_format)
        sheet.write(sheet_line, 3, "speed_std_dev_kn", header_format)
        sheet.write(sheet_line, 4, self.avg_speed_mps.std_dev() * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheet_line, 5, "kn", unit_format)
        sheet_line += 2
        if self._smooth_filter:
            sheet.write(sheet_line, 0, "speed_smoothed_min", header_format)
            sheet.write(sheet_line, 1, self.min_speed_smoothed_mps, header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "speed_smoothed_min_kn", header_format)
            sheet.write(sheet_line, 4, self.min_speed_smoothed_mps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "speed_smoothed_max", header_format)
            sheet.write(sheet_line, 1, self.max_speed_smoothed_mps, header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "speed_smoothed_max_kn", header_format)
            sheet.write(sheet_line, 4, self.max_speed_smoothed_mps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "speed_smoothed_avg", header_format)
            sheet.write(sheet_line, 1, self.avg_speed_smoothed_mps.mean(), header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "speed_smoothed_avg_kn", header_format)
            sheet.write(sheet_line, 4, self.avg_speed_smoothed_mps.mean() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "speed_smoothed_std_dev", header_format)
            sheet.write(sheet_line, 1, self.avg_speed_smoothed_mps.std_dev(), header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "speed_smoothed_std_dev_kn", header_format)
            sheet.write(sheet_line, 4, self.avg_speed_smoothed_mps.std_dev() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 2

        if not math.isnan(self.min_vspeed_mps) and not math.isnan(self.max_vspeed_mps) \
                    and self.min_vspeed_mps == self.max_vspeed_mps and self.max_vspeed_mps > 0:
            sheet.write(sheet_line, 0, "vertical_speed_min", header_format)
            sheet.write(sheet_line, 1, self.min_vspeed_mps, header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "vertical_speed_min_kn", header_format)
            sheet.write(sheet_line, 4, self.min_vspeed_mps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "vertical_speed_max", header_format)
            sheet.write(sheet_line, 1, self.max_vspeed_mps, header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "vertical_speed_max_kn", header_format)
            sheet.write(sheet_line, 4, self.max_vspeed_mps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "vertical_speed_avg", header_format)
            sheet.write(sheet_line, 1, self.avg_vspeed_mps.mean(), header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "vertical_speed__avg_kn", header_format)
            sheet.write(sheet_line, 4, self.avgVSpeedSps.mean() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "vertical_speed_std_dev", header_format)
            sheet.write(sheet_line, 1, self.avg_vspeed_mps.std_dev(), header_value_format)
            sheet.write(sheet_line, 2, "m/s", unit_format)
            sheet.write(sheet_line, 3, "vertical_speed_std_dev_kn", header_format)
            sheet.write(sheet_line, 4, self.avg_vspeed_mps.std_dev() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheet_line, 5, "kn", unit_format)
            sheet_line += 2
            if self._smooth_filter:
                sheet.write(sheet_line, 0, "vertical_speed_smoothed_min", header_format)
                sheet.write(sheet_line, 1, self.min_vspeed_smoothed_mps, header_value_format)
                sheet.write(sheet_line, 2, "m/s", unit_format)
                sheet.write(sheet_line, 3, "vertical_speed_smoothed_min_kn", header_format)
                sheet.write(sheet_line, 4, self.min_vspeed_smoothed_mps * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheet_line, 5, "kn", unit_format)
                sheet_line += 1
                sheet.write(sheet_line, 0, "vertical_speed_smoothed_max", header_format)
                sheet.write(sheet_line, 1, self.max_vspeed_smoothed_mps, header_value_format)
                sheet.write(sheet_line, 2, "m/s", unit_format)
                sheet.write(sheet_line, 3, "vertical_speed_smoothed_max_kn", header_format)
                sheet.write(sheet_line, 4, self.max_vspeed_smoothed_mps * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheet_line, 5, "kn", unit_format)
                sheet_line += 1
                sheet.write(sheet_line, 0, "vertical_speed_smoothed_avg", header_format)
                sheet.write(sheet_line, 1, self.avg_vspeed_smoothed_mps.mean(), header_value_format)
                sheet.write(sheet_line, 2, "m/s", unit_format)
                sheet.write(sheet_line, 3, "vertical_speed_smoothed_avg_kn", header_format)
                sheet.write(sheet_line, 4, self.avg_vspeed_smoothed_mps.mean() * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheet_line, 5, "kn", unit_format)
                sheet_line += 1
                sheet.write(sheet_line, 0, "vertical_speed_smoothed_std_dev", header_format)
                sheet.write(sheet_line, 1, self.avg_vspeed_smoothed_mps.std_dev(), header_value_format)
                sheet.write(sheet_line, 2, "m/s", unit_format)
                sheet.write(sheet_line, 3, "vertical_speed_smoothed_std_dev_kn", header_format)
                sheet.write(sheet_line, 4, self.avg_vspeed_smoothed_mps.std_dev() * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheet_line, 5, "kn", unit_format)
                sheet_line += 2

        if not math.isnan(self.min_lat_rads) and not math.isnan(self.max_lat_rads) \
                and not math.isnan(self.min_lon_rads) and not math.isnan(self.max_lon_rads):
            sheet.write(sheet_line, 0, "geospatial_lat_min", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.min_lat_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_lat_max", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_lat_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_lon_min", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.min_lon_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_lon_max", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_lon_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 2
            sheet.write(sheet_line, 0, "geospatial_lat_avg", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_lat_rads.mean()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_lat_std_dev", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_lat_rads.std_dev()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_lon_avg", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_lon_rads.mean()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_lon_std_dev", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_lon_rads.std_dev()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 2
        
        if not math.isnan(self.max_height):
            sheet.write(sheet_line, 0, "geospatial_height_min", name_format)
            sheet.write(sheet_line, 1, self.min_height, value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_height_max", name_format)
            sheet.write(sheet_line, 1, self.max_height, value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_height_avg", name_format)
            sheet.write(sheet_line, 1, self.avg_height.mean(), value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_height_std_dev", name_format)
            sheet.write(sheet_line, 1, self.avg_height.std_dev(), value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 2

        section_vertical = False
        if not math.isnan(self.max_depth) and self.max_depth > 0:
            sheet.write(sheet_line, 0, "geospatial_depth_max", name_format)
            sheet.write(sheet_line, 1, self.max_depth, value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_depth_avg", name_format)
            sheet.write(sheet_line, 1, self.avg_depth.mean(), value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_depth_std_dev", name_format)
            sheet.write(sheet_line, 1, self.avg_depth.std_dev(), value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            section_vertical = True
        if not math.isnan(self.max_alt) and self.max_alt > 0:
            sheet.write(sheet_line, 0, "geospatial_alt_max", name_format)
            sheet.write(sheet_line, 1, self.max_alt, value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            section_vertical = True
            sheet.write(sheet_line, 0, "geospatial_alt_avg", name_format)
            sheet.write(sheet_line, 1, self.avg_alt.mean(), value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "geospatial_alt_std_dev", name_format)
            sheet.write(sheet_line, 1, self.avg_alt.std_dev(), value_format)
            sheet.write(sheet_line, 2, "m", unit_format)
            sheet_line += 1
            section_vertical = True
        if section_vertical:
            sheet_line += 1

        if not math.isnan(self.max_roll_rads):
            sheet.write(sheet_line, 0, "attitude_roll_min", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.min_roll_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_roll_max", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_roll_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_roll_amp", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_roll_rads - self.min_roll_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_roll_avg", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_roll_rads.mean()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_roll_std_dev", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_roll_rads.std_dev()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 2

        if not math.isnan(self.max_pitch_rads):
            sheet.write(sheet_line, 0, "attitude_pitch_min", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.min_pitch_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_pitch_max", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_pitch_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_pitch_amp", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_pitch_rads - self.min_pitch_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_pitch_avg", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_pitch_rads.mean()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_pitch_std_dev", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_pitch_rads.std_dev()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 2
        
        if not math.isnan(self.max_sideslip_rads):
            sheet.write(sheet_line, 0, "attitude_sideslip_min", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.min_sideslip_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_sideslip_max", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_sideslip_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_sideslip_amp", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.max_sideslip_rads - self.min_sideslip_rads), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_sideslip_avg", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_sideslip_rads.mean()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 1
            sheet.write(sheet_line, 0, "attitude_sideslip_std_dev", name_format)
            sheet.write(sheet_line, 1, math.degrees(self.avg_sideslip_rads.std_dev()), value_format)
            sheet.write(sheet_line, 2, "°", unit_format)
            sheet_line += 2
    
        sheet.write(sheet_line, 0, "time_between_states_avg", name_format)
        # sheet.write(sheetLine, 1, (self.endMillis - self.startMillis) / self.numStates / 1000, value_format)
        sheet.write(sheet_line, 1, self.duration_traveled_millis / self.num_states / 1000, value_format)
        sheet.write(sheet_line, 2, "s", unit_format)
        sheet_line += 1
        sheet.write(sheet_line, 0, "number_of_states", name_format)
        sheet.write(sheet_line, 1, self.num_states, value_format)
        sheet_line += 1
        sheet.write(sheet_line, 0, "number_of_log_files", name_format)
        sheet.write(sheet_line, 1, len(self.log_names), value_format)
        sheet_line += 2

        sheet_line += 2
        if self.variables is not None and len(self.variables) > 0:
            section_variables_written = False
            for section_name in self.variables:
                section_name_written = False
                variables = self.variables[section_name]
                if variables is None or len(variables) == 0:
                    continue
                for entity in variables:
                    var = variables[entity]
                    if var is None:
                        continue

                    unit_str = self.units_mappings.get(section_name, '')

                    if not section_variables_written:
                        sheet.write(sheet_line, 0, "Variables", header_format)
                        sheet_line += 1
                        section_variables_written = True
                    if not section_name_written:
                        sheet.write(sheet_line, 1, section_name, name_format)
                        sheet_line += 1
                        section_name_written = True

                    sheet.write(sheet_line, 2, entity, name_format)

                    sheet.write(sheet_line, 3, "min", name_format)
                    sheet.write(sheet_line, 4, var['min'], value_format)
                    if unit_str is not None and unit_str != '':
                        sheet.write(sheet_line, 5, unit_str, unit_format)
                    sheet_line += 1
                    sheet.write(sheet_line, 3, "max", name_format)
                    sheet.write(sheet_line, 4, var['max'], value_format)
                    if unit_str is not None and unit_str != '':
                        sheet.write(sheet_line, 5, unit_str, unit_format)
                    sheet_line += 1
                    sheet.write(sheet_line, 3, "amp", name_format)
                    sheet.write(sheet_line, 4, var['max'] - var['min'], value_format)
                    if unit_str is not None and unit_str != '':
                        sheet.write(sheet_line, 5, unit_str, unit_format)
                    sheet_line += 1
                    sheet.write(sheet_line, 3, "avg", name_format)
                    sheet.write(sheet_line, 4, var['avg'].mean(), value_format)
                    if unit_str is not None and unit_str != '':
                        sheet.write(sheet_line, 5, unit_str, unit_format)
                    sheet_line += 1
                    sheet.write(sheet_line, 3, "std_dev", name_format)
                    sheet.write(sheet_line, 4, var['avg'].std_dev(), value_format)
                    if unit_str is not None and unit_str != '':
                        sheet.write(sheet_line, 5, unit_str, unit_format)
                    sheet_line += 2
                    