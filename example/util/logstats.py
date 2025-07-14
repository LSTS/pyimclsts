import math
from typing import Final
import pandas as pd
import math
from datetime import datetime

from example.netCDF.core import locationType
from example.util.math import CircularMean, MeanStats, SlidingWindow, haversine
from example.util.units import *

import pyimc_generated as pg


class LogStats:

    def __init__(self, source_id: int = 0x0000, jump_time_millis: int = 10000, smooth_filter: bool = False, sliding_window_size: int = 10):
        self.BATTERIES_STR: Final[str] = 'Batteries'
        self.VOLTAGE_STR: Final[str] = 'Voltage'
        self.TEMPERATURE_STR: Final[str] = 'Temperature'

        self.units_mappings: dict = {}
        self.units_mappings[self.VOLTAGE_STR] = 'V'
        self.units_mappings[self.TEMPERATURE_STR] = '°C'

        self._jump_time_millis = jump_time_millis

        self._smooth_filter = smooth_filter
        self._sliding_window_size = sliding_window_size

        self.logNames = []
        self._newLogName = None
        self.logDays = []

        self.systemName = None
        self.sourceId = source_id

        self.startMillis = -1
        self.endMillis = -1
        self.lastMillis = -1

        self.minLatRads = float('NaN')
        self.maxLatRads = float('NaN')
        self.avgLatRads = CircularMean(degrees=False)
        self.minLonRads = float('NaN')
        self.maxLonRads = float('NaN')
        self.avgLonRads = CircularMean(degrees=False)

        self.minHeight = float('NaN')
        self.maxHeight = float('NaN')
        self.avgHeight = MeanStats()

        self.maxDepth = float('NaN')
        self.avgDepth = MeanStats()

        self.maxAlt = float('NaN')
        self.avgAlt = MeanStats()

        self.minRollRads = float('NaN')
        self.maxRollRads = float('NaN')
        self.ampRollRads = float('NaN')
        self.avgRollRads = CircularMean(degrees=False)

        self.minPitchRads = float('NaN')
        self.maxPitchRads = float('NaN')
        self.ampPitchRads = float('NaN')
        self.avgPitchRads = CircularMean(degrees=False)

        self.numStates = 0
        self.numberOfLogs = 0

        self.distance = 0.0  # Total distance traveled in meters
        # smoothed
        self.distance_smoothed = 0.0  # Total distance traveled in meters

        self.lastLocation = None
        self._last_lat_rads_sliding_window = SlidingWindow(window_size=self._sliding_window_size)
        self._last_lon_rads_sliding_window = SlidingWindow(window_size=self._sliding_window_size)

        self.avgSpeedMpsCalc = float('NaN')
        # smoothed
        self.avgSpeedSmoothedMpsCalc = float('NaN')

        self.minSpeedMps = float('NaN')
        self.maxSpeedMps = float('NaN')
        self.avgSpeedMps = MeanStats()
        # smoothed
        self.minSpeedSmoothedMps = float('NaN')
        self.maxSpeedSmoothedMps = float('NaN')
        self.avgSpeedSmoothedMps = MeanStats()
        self._speed_sliding_window = SlidingWindow(window_size=self._sliding_window_size)

        self.minVSpeedMps = float('NaN')
        self.maxVSpeedMps = float('NaN')
        self.avgVSpeedMps = MeanStats()
        # smoothed
        self.minVSpeedSmoothedMps = float('NaN')
        self.maxVSpeedSmoothedMps = float('NaN')
        self.avgVSpeedSmoothedMps = MeanStats()
        self._vspeed_sliding_window = SlidingWindow(window_size=self._sliding_window_size)

        # Dictonary for other variables (min, max, avg)
        self.variables = {}
        self._variablesNumericTmp = {}

        self.voltageEntities = []
        self.temperatureEntities = []

        self.entitiesMappings = {}

        ## Final stats need final wrap up calculations

        self.durationTravelledMillis = 0.0
        self._durationTravelledMillisTmp = float('NaN')

        # Duration calculations
        self.durationInSeconds = float('NaN')
        # Separate parts of the duration for better readability
        self.durationDaysPart = float('NaN')
        self.durationHoursPart = float('NaN')
        self.durationMinutesPart = float('NaN')
        self.durationSecondsPart = float('NaN')

    def _initialize_variable_list(self):
        if self.variables is None:
            self.variables = {}
        if self._variablesNumericTmp is None:
            self._variablesNumericTmp = {}
        
        if self.variables.get(self.VOLTAGE_STR, None) is None:
            self.variables[self.VOLTAGE_STR] = {}
        if self._variablesNumericTmp.get(self.VOLTAGE_STR, None) is None:
            self._variablesNumericTmp[self.VOLTAGE_STR] = {}
        
        if self.variables.get(self.TEMPERATURE_STR, None) is None:
            self.variables[self.TEMPERATURE_STR] = {}
        if self._variablesNumericTmp.get(self.TEMPERATURE_STR, None) is None:
            self._variablesNumericTmp[self.TEMPERATURE_STR] = {}


    def new_log_name(self, newLogName):
        """
        Set a new log name for the statistics.
        This is used to differentiate between multiple logs.
        """
        if newLogName is not None:
            self._newLogName = newLogName
        else:
            self._newLogName = "?"


    def update_name(self, msg, callback=None):
        """
        Update the statistics with a new message.
        This method is called by the subscriber when a new message is received.
        """
        if not isinstance(msg, pg.messages.Announce):
            return
        if self.sourceId == 0x0000 or msg._header.src != self.sourceId or not (self.systemName is None):
            return
        if (msg.sys_name is None or msg.sys_name == ""):
            return
        
        self.systemName = msg.sys_name
        
    def update_state(self, msg, callback=None):
        if not isinstance(msg, pg.messages.EstimatedState):
            return
        
        #debug(1)
        #debug("Source ID: {}, Message Source: {}".format(self.sourceId, msg._header.src))
        if msg._header.src != self.sourceId:
            return

        if self.lastLocation is not None:
            if self.curTimeMillis - self.lastMillis > self._jump_time_millis:
                self._last_lat_rads_sliding_window.clear()
                self._last_lon_rads_sliding_window.clear()
                self._speed_sliding_window.clear()
                self._vspeed_sliding_window.clear()

        if self.lastLocation is None:
            self._last_lat_rads_sliding_window.clear()
            self._last_lon_rads_sliding_window.clear()
            self._speed_sliding_window.clear()
            self._vspeed_sliding_window.clear()


        height = msg.height
        self.minHeight = min(self.minHeight, height) if not math.isnan(self.minHeight) else height
        self.maxHeight = max(self.maxHeight, height) if not math.isnan(self.maxHeight) else height
        self.avgHeight.update(height)

        if msg.depth > 0:
            depth = msg.depth
            self.maxDepth = max(self.maxDepth, depth) if not math.isnan(self.maxDepth) else depth
            self.avgDepth.update(depth)

        if msg.alt > 0:
            alt = msg.alt
            self.maxAlt = max(self.maxAlt, alt) if not math.isnan(self.maxAlt) else alt
            self.avgAlt.update(alt)

        phi = msg.phi
        theta = msg.theta
        
        self.maxRollRads = max(self.maxRollRads, phi) if not math.isnan(self.maxRollRads) else phi
        self.minRollRads = min(self.minRollRads, phi) if not math.isnan(self.minRollRads) else phi
        self.avgRollRads.update(phi)
        
        self.maxPitchRads = max(self.maxPitchRads, theta) if not math.isnan(self.maxPitchRads) else theta
        self.minPitchRads = min(self.minPitchRads, theta) if not math.isnan(self.minPitchRads) else theta
        self.avgPitchRads.update(theta)

        vspeed = msg.vz
        self.maxVSpeedMps = max(self.maxVSpeedMps, vspeed) if not math.isnan(self.maxVSpeedMps) else vspeed
        self.minVSpeedMps = min(self.minVSpeedMps, vspeed) if not math.isnan(self.minVSpeedMps) else vspeed
        self.avgVSpeedMps.update(vspeed)
        self._vspeed_sliding_window.update(vspeed)
        vspeed_smoothed = self._vspeed_sliding_window.mean()
        self.maxVSpeedSmoothedMps = max(self.maxVSpeedSmoothedMps, vspeed_smoothed) if not math.isnan(self.maxVSpeedSmoothedMps) else vspeed_smoothed
        self.minVSpeedSmoothedMps = min(self.minVSpeedSmoothedMps, vspeed) if not math.isnan(self.minVSpeedSmoothedMps) else vspeed_smoothed
        self.avgVSpeedSmoothedMps.update(vspeed_smoothed)

        hspeed = math.sqrt(msg.vx** 2 + msg.vy ** 2)
        self.maxSpeedMps = max(self.maxSpeedMps, hspeed) if not math.isnan(self.maxSpeedMps) else hspeed
        self.minSpeedMps = min(self.minSpeedMps, hspeed) if not math.isnan(self.minSpeedMps) else hspeed
        self.avgSpeedMps.update(hspeed)
        self._speed_sliding_window.update(hspeed)
        hspeed_smoothed = self._speed_sliding_window.mean() if self._smooth_filter else hspeed
        self.maxSpeedSmoothedMps = max(self.maxSpeedSmoothedMps, hspeed_smoothed) if not math.isnan(self.maxSpeedSmoothedMps) else hspeed_smoothed
        self.minSpeedSmoothedMps = min(self.minSpeedSmoothedMps, hspeed) if not math.isnan(self.minSpeedSmoothedMps) else hspeed_smoothed
        self.avgSpeedSmoothedMps.update(hspeed_smoothed)
        

        self.curTimeMillis = msg._header.timestamp * 1000.0  # Convert to milliseconds

        logDay = pd.to_datetime(self.curTimeMillis, unit='ms', utc=True).strftime('%Y%m%d')
        # append to logDays if not already present
        if logDay not in self.logDays:
            self.logDays.append(logDay)
        if self._newLogName is not None:
            self.logNames.append("{}/{}".format(logDay, self._newLogName))
            self._newLogName = None

        if self.lastLocation is not None:
            if self.curTimeMillis - self.lastMillis > self._jump_time_millis:
                self.lastLocation = None
                if not math.isnan(self._durationTravelledMillisTmp):
                    self.durationTravelledMillis += self._durationTravelledMillisTmp
                    self._durationTravelledMillisTmp = float('NaN')
            else:
                self._durationTravelledMillisTmp += self.curTimeMillis - self.lastMillis

        if self.lastLocation is None:
            self.lastLocation = locationType()
            self.lastLocation.__init__()
            self.lastLocation.fill_it(msg)
            self.lastLocation.lat = msg.lat
            self.lastLocation.lon = msg.lon
            self.lastLocation.translate_positions(msg.x, msg.y, msg.z)
            self.lastLocation.add_offsets()

            self.minLatRads = msg.lat if math.isnan(self.minLatRads) else min(self.minLatRads, self.lastLocation.lat)
            self.maxLatRads = msg.lat if math.isnan(self.maxLatRads) else max(self.maxLatRads, self.lastLocation.lat)
            self.avgLatRads.update(self.lastLocation.lat)
            self.minLonRads = msg.lon if math.isnan(self.minLonRads) else min(self.minLonRads, self.lastLocation.lon)
            self.maxLonRads = msg.lon if math.isnan(self.maxLonRads) else max(self.maxLonRads, self.lastLocation.lon)
            self.avgLonRads.update(self.lastLocation.lon)
            
            self.lastMillis = msg._header.timestamp * 1000.0  # Convert to milliseconds
            self.startMillis = msg._header.timestamp * 1000.0 if self.startMillis < 0 else self.startMillis  # Convert to milliseconds
            self.endMillis = msg._header.timestamp * 1000.0  # Convert to milliseconds
            
            if not math.isnan(self._durationTravelledMillisTmp):
                self.durationTravelledMillis += self._durationTravelledMillisTmp
                self._durationTravelledMillisTmp = float('NaN')
            self._durationTravelledMillisTmp = 0
            
            self._last_lat_rads_sliding_window.update(self.lastLocation.lat)
            self._last_lon_rads_sliding_window.update(self.lastLocation.lon)

            self.numStates += 1
            return

        if self.startMillis < 0:
            self.startMillis = self.curTimeMillis
        if self.endMillis < 0:
            self.endMillis = self.curTimeMillis

        self.curLocation = locationType()
        self.curLocation.__init__()
        self.curLocation.fill_it(msg)
        self.curLocation.lat = msg.lat
        self.curLocation.lon = msg.lon
        self.curLocation.translate_positions(msg.x, msg.y, msg.z)
        self.curLocation.add_offsets()

        self.minLatRads = msg.lat if math.isnan(self.minLatRads) else min(self.minLatRads, self.curLocation.lat)
        self.maxLatRads = msg.lat if math.isnan(self.maxLatRads) else max(self.maxLatRads, self.curLocation.lat)
        self.avgLatRads.update(self.curLocation.lat)
        self.minLonRads = msg.lon if math.isnan(self.minLonRads) else min(self.minLonRads, self.curLocation.lon)
        self.maxLonRads = msg.lon if math.isnan(self.maxLonRads) else max(self.maxLonRads, self.curLocation.lon)
        self.avgLonRads.update(self.curLocation.lon)

        #distH = self.curLocation.getHorizontalDistanceInMeters(self.lastLocation)
        dist = haversine(self.lastLocation.lat, self.lastLocation.lon, self.curLocation.lat, self.curLocation.lon, degrees=False)
        #print("Distance between {}° {}° and {}° {}°: {}m vs {}m".format(math.degrees(self.lastLocation.lat), math.degrees(self.lastLocation.lon), math.degrees(self.curLocation.lat), math.degrees(self.curLocation.lon), dist, distH))
        self.distance += dist

        if self._smooth_filter:
            smoothed_last_loc = locationType()
            smoothed_last_loc.__init__()
            smoothed_last_loc.lat = self._last_lat_rads_sliding_window.mean()
            smoothed_last_loc.lon = self._last_lon_rads_sliding_window.mean()
            self._last_lat_rads_sliding_window.update(self.curLocation.lat)
            self._last_lon_rads_sliding_window.update(self.curLocation.lon)
            smoothed_cur_loc = locationType()
            smoothed_cur_loc.__init__()
            smoothed_cur_loc.lat = self._last_lat_rads_sliding_window.mean()
            smoothed_cur_loc.lon = self._last_lon_rads_sliding_window.mean()
            #distH = smoothed_cur_loc.getHorizontalDistanceInMeters(self.smoothed_last_loc)
            dist_smoothed = haversine(smoothed_last_loc.lat, smoothed_last_loc.lon, smoothed_cur_loc.lat, smoothed_cur_loc.lon, degrees=False)
            self.distance_smoothed += dist_smoothed

        self.lastLocation = self.curLocation
        self.endMillis = self.curTimeMillis
        self.lastMillis = self.curTimeMillis
        self.numStates += 1

    def update_voltage(self, msg, callback=None):
        if not isinstance(msg, pg.messages.Voltage):
            return
        if msg._header.src != self.sourceId:
            return
        if self.voltageEntities is None or len(self.voltageEntities) == 0:
            return

        self._initialize_variable_list()
        voltage_dic = self.variables[self.VOLTAGE_STR]
        voltage_tmp_dic = self._variablesNumericTmp[self.VOLTAGE_STR]

        source_ent = msg._header.src_ent
        source_ent_name: str = None
        
        # search entitiesMappings by the name for source_ent (the discionary is name vs source_ent)
        for name, entity_id in self.entitiesMappings.items():
            if entity_id == source_ent:
                source_ent_name = name
                break

        entry_elem = None
        if source_ent_name is not None and source_ent_name != '':
            if source_ent_name not in voltage_dic:
                voltage_dic[source_ent_name] = {}
            entry_elem = voltage_dic[source_ent_name]
        else:
            if source_ent not in voltage_tmp_dic:
                voltage_tmp_dic[source_ent] = {}
            entry_elem = voltage_tmp_dic[source_ent]
        if entry_elem is None:
            return
        if len(entry_elem) == 0:
            # initialize
            entry_elem['min'] = float('NaN')
            entry_elem['max'] = float('NaN')
            entry_elem['avg'] = MeanStats()
        
        voltage = msg.value
        entry_elem['min'] = min(entry_elem['min'], voltage) if not math.isnan(entry_elem['min']) else voltage
        entry_elem['max'] = max(entry_elem['max'], voltage) if not math.isnan(entry_elem['max']) else voltage
        entry_elem['avg'].update(voltage)

    
    def update_temperature(self, msg, callback=None):
        if not isinstance(msg, pg.messages.Temperature):
            return
        if msg._header.src != self.sourceId:
            return
        if self.temperatureEntities is None or len(self.temperatureEntities) == 0:
            return
        
        self._initialize_variable_list()
        temperature_dic = self.variables[self.TEMPERATURE_STR]
        temperature_tmp_dic = self._variablesNumericTmp[self.TEMPERATURE_STR]

        source_ent = msg._header.src_ent
        source_ent_name: str = None

        for name, entity_id in self.entitiesMappings.items():
            if entity_id == source_ent:
                source_ent_name = name
                break

        entry_elem = None
        if source_ent_name is not None and source_ent_name != '':
            if source_ent_name not in temperature_dic:
                temperature_dic[source_ent_name] = {}
            entry_elem = temperature_dic[source_ent_name]
        else:
            if source_ent not in temperature_tmp_dic:
                temperature_tmp_dic[source_ent] = {}
            entry_elem = temperature_tmp_dic[source_ent]
        if entry_elem is None:
            return
        if len(entry_elem) == 0:
            # initialize
            entry_elem['min'] = float('NaN')
            entry_elem['max'] = float('NaN')
            entry_elem['avg'] = MeanStats()
        
        temperature = msg.value
        entry_elem['min'] = min(entry_elem['min'], temperature) if not math.isnan(entry_elem['min']) else temperature
        entry_elem['max'] = max(entry_elem['max'], temperature) if not math.isnan(entry_elem['max']) else temperature
        entry_elem['avg'].update(temperature)
        

    def map_unnamed_variables_to_named(self):
        self._initialize_variable_list()

        def _merge(list_entities, dic_entities_mappings, dic_variables, dic_variables_tmp):
            if list_entities is None or len(list_entities) == 0:
                return
            if dic_entities_mappings is not None and len(dic_entities_mappings) > 0:
                for entity_name in list_entities:
                    entity_id = dic_entities_mappings.get(entity_name, None)
                    if entity_id is None:
                        continue
                    var_tmp = dic_variables_tmp.get(entity_id, None)
                    if var_tmp is None:
                        continue
                    var = dic_variables.get(entity_name, None)
                    if var is None:
                        dic_variables[entity_name] = var_tmp
                        dic_variables_tmp.pop(entity_id, None)
                    else:
                        var['min'] = min(var['min'], var_tmp['min']) if not math.isnan(var['min']) else var_tmp['min']
                        var['max'] = max(var['max'], var_tmp['max']) if not math.isnan(var['max']) else var_tmp['max']
                        var['avg'].merge_with(var_tmp['avg'])
            

        # Voltage
        _merge(self.voltageEntities, self.entitiesMappings,
               self.variables[self.VOLTAGE_STR], self._variablesNumericTmp[self.VOLTAGE_STR])
        # Temperature
        _merge(self.temperatureEntities, self.entitiesMappings,
               self.variables[self.TEMPERATURE_STR], self._variablesNumericTmp[self.TEMPERATURE_STR])
        
        self._variablesNumericTmp.clear()
        # self._initialize_variable_list()


    def finalize(self):
        """Final calculations for the statistics."""
        if self.startMillis < 0 or self.endMillis < 0:
            print("No data found in the logs.")
            return

        if not math.isnan(self._durationTravelledMillisTmp):
            self.durationTravelledMillis += self._durationTravelledMillisTmp
            self._durationTravelledMillisTmp = float('NaN')
        
        # set logDay as yyyyMMdd
        #logDay = pd.to_datetime(self.startMillis, unit='ms', utc=True).strftime('%Y%m%d')
        #if self.logName != "":
        #    self.logName = "{}/{}".format(logDay, self.logName)
        # self.durationInSeconds = (self.endMillis - self.startMillis) / 1000.0  # Convert to seconds
        self.durationInSeconds = self.durationTravelledMillis / 1000.0  # Convert to seconds
        durationSecondsPartTmp = self.durationInSeconds
        self.durationDaysPart = int(durationSecondsPartTmp // (24 * 3600))
        durationSecondsPartTmp %= (24 * 3600)
        self.durationHoursPart = int(durationSecondsPartTmp // 3600)
        durationSecondsPartTmp %= 3600
        self.durationMinutesPart = int(durationSecondsPartTmp // 60)
        self.durationSecondsPart = durationSecondsPartTmp % 60

        if not math.isnan(self.maxRollRads) and not math.isnan(self.minRollRads):
            self.ampRollRads = self.maxRollRads - self.minRollRads

        if not math.isnan(self.maxPitchRads) and not math.isnan(self.minPitchRads):
            self.ampPitchRads = self.maxPitchRads - self.minPitchRads

        #if (self.endMillis - self.startMillis) > 0:
        #    self.avgSpeedMpsCalc = self.distance / ((self.endMillis - self.startMillis) / 1000.0)
        if (self.durationTravelledMillis) > 0:
            self.avgSpeedMpsCalc = self.distance / (self.durationTravelledMillis / 1000.0)
            self.avgSpeedSmoothedMpsCalc = self.distance_smoothed / (self.durationTravelledMillis / 1000.0)
        else:
            self.avgSpeedMpsCalc = 0
            self.avgSpeedSmoothedMpsCalc = 0
    

    def __str__(self):
        """String representation of the statistics."""
        output = []
        justify = 37
        output.append("{}: {}".format("Log Name".ljust(justify), ", ".join(self.logNames)))
        output.append("{}: {}".format("Log Days".ljust(justify), ", ".join(self.logDays)))
        
        if self.systemName is not None:
            output.append("{}: {} | Source ID: {} (0x{:04X})".format("System Name".ljust(justify), self.systemName, self.sourceId, self.sourceId))
        else:
            output.append("{}: | Source ID: {} (0x{:04X})".format("System Name".ljust(justify), self.sourceId, self.sourceId))

        if self.startMillis < 0 or self.endMillis < 0:
            output.append("No data found in the logs.")
        else:
            output.append("{}: {}".format("Start time".ljust(justify), pd.to_datetime(self.startMillis, unit='ms', utc=True)))
            output.append("{}: {}".format("End time".ljust(justify), pd.to_datetime(self.endMillis, unit='ms', utc=True)))
            output.append("{}: {} days, {} hours, {} minutes, {:.3f} seconds".format(
                "Duration".ljust(justify), self.durationDaysPart, self.durationHoursPart, self.durationMinutesPart, self.durationSecondsPart))

            output.append("{}: {:.1f} m | {:.2f} NM".format("Distance".ljust(justify), self.distance, self.distance * METERS_TO_NM))
            if self._smooth_filter:
                output.append("{}: {:.1f} m | {:.2f} NM".format("Distance Smoothed".ljust(justify), self.distance_smoothed, self.distance_smoothed * METERS_TO_NM))
            
            if self._smooth_filter:
                output.append("{}: {} elements".format("Smooth Window".ljust(justify), self._sliding_window_size))

            avgSpeedKnotsCalc = self.avgSpeedMpsCalc * MPS_TO_KNOTS
            output.append("{}: {:.2f} m/s : {:.2f} kn".format("Average Speed Calc by Time".ljust(justify), self.avgSpeedMpsCalc, avgSpeedKnotsCalc))

            if self._smooth_filter:
                avgSpeedKnotsCalcSmoothed = self.avgSpeedSmoothedMpsCalc * MPS_TO_KNOTS
                output.append("{}: {:.2f} m/s : {:.2f} kn".format("Average Speed Calc by Time Smoothed".ljust(justify), self.avgSpeedSmoothedMpsCalc, avgSpeedKnotsCalcSmoothed))

            output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                "Speed".ljust(justify), self.minSpeedMps, self.minSpeedMps * MPS_TO_KNOTS, 
                self.maxSpeedMps, self.maxSpeedMps * MPS_TO_KNOTS, 
                self.avgSpeedMps.mean(), self.avgSpeedMps.mean() * MPS_TO_KNOTS, 
                self.avgSpeedMps.std_dev(), self.avgSpeedMps.std_dev() * MPS_TO_KNOTS))
            if self._smooth_filter:
                output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                    "Speed Smoothed".ljust(justify), self.minSpeedSmoothedMps, self.minSpeedSmoothedMps * MPS_TO_KNOTS, 
                    self.maxSpeedSmoothedMps, self.maxSpeedSmoothedMps * MPS_TO_KNOTS, 
                    self.avgSpeedSmoothedMps.mean(), self.avgSpeedSmoothedMps.mean() * MPS_TO_KNOTS, 
                    self.avgSpeedSmoothedMps.std_dev(), self.avgSpeedSmoothedMps.std_dev() * MPS_TO_KNOTS))


            if not math.isnan(self.minVSpeedMps) and not math.isnan(self.maxVSpeedMps) \
                    and self.minVSpeedMps == self.maxVSpeedMps and self.maxVSpeedMps > 0:
                output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                    "Vertical Speed".ljust(justify), self.minVSpeedMps, self.minVSpeedMps * MPS_TO_KNOTS, 
                    self.maxVSpeedMps, self.maxVSpeedMps * MPS_TO_KNOTS, 
                    self.avgVSpeedMps.mean(), self.avgVSpeedMps.mean() * MPS_TO_KNOTS, 
                    self.avgVSpeedMps.std_dev(), self.avgVSpeedMps.std_dev() * MPS_TO_KNOTS))
                if self._smooth_filter:
                    output.append("{}: Min: {:.2f} m/s : {:.2f} kn | Max: {:.2f} m/s : {:.2f} kn | Avg: {:.2f} m/s : {:.2f} kn | Std Dev: {:.2f} m/s : {:.2f} kn".format(
                        "Vertical Speed Smoothed".ljust(justify), self.minVSpeedSmoothedMps, self.minVSpeedSmoothedMps * MPS_TO_KNOTS, 
                        self.maxVSpeedSmoothedMps, self.maxVSpeedSmoothedMps * MPS_TO_KNOTS, 
                        self.avgVSpeedSmoothedMps.mean(), self.avgVSpeedSmoothedMps.mean() * MPS_TO_KNOTS, 
                        self.avgVSpeedSmoothedMps.std_dev(), self.avgVSpeedSmoothedMps.std_dev() * MPS_TO_KNOTS))

            if not math.isnan(self.minLatRads) and not math.isnan(self.maxLatRads):
                output.append("{}: Min: {:.7f}° | Max: {:.7f}° | Avg: {:.7f}° | Std Dev: {:.7f}°".format(
                    "Latitude".ljust(justify), math.degrees(self.minLatRads), math.degrees(self.maxLatRads), 
                    math.degrees(self.avgLatRads.mean()), math.degrees(self.avgLatRads.std_dev())))
            if not math.isnan(self.minLonRads) and not math.isnan(self.maxLonRads):
                output.append("{}: Min: {:.7f}° | Max: {:.7f}° | Avg: {:.7f}° | Std Dev: {:.7f}°".format(
                    "Longitude".ljust(justify), math.degrees(self.minLonRads), math.degrees(self.maxLonRads), 
                    math.degrees(self.avgLonRads.mean()), math.degrees(self.avgLonRads.std_dev())))

            if not math.isnan(self.maxHeight):
                output.append("{}: Min: {:.1f} m | Max: {:.1f} m | Avg: {:.1f} m | Std Dev: {:.1f} m".format(
                    "Height".ljust(justify), self.minHeight, self.maxHeight, self.avgHeight.mean(), self.avgHeight.std_dev()))
                
            if not math.isnan(self.maxDepth) and self.maxDepth > 0:
                output.append("{}: Max: {:.1f} m | Avg: {:.1f} m | Std Dev: {:.1f} m".format(
                    "Depth".ljust(justify), self.maxDepth, math.degrees(self.avgDepth.mean()), math.degrees(self.avgDepth.std_dev())))
            
            if not math.isnan(self.maxAlt) and self.maxAlt > 0:
                output.append("{}: Max: {:.1f} m | Avg: {:.1f} m | Std Dev: {:.1f} m".format(
                    "Altitude".ljust(justify), self.maxAlt, self.avgAlt.mean(), self.avgAlt.std_dev()))

            if not math.isnan(self.maxRollRads):
                output.append("{}: Min: {:.2f}° | Max: {:.2f}° | Amp: {:.2f}° | Avg: {:.2f}° | Std Dev: {:.2f}°".format(
                    "Roll".ljust(justify), math.degrees(self.minRollRads), math.degrees(self.maxRollRads), 
                    math.degrees(self.ampRollRads), math.degrees(self.avgRollRads.mean()), 
                    math.degrees(self.avgRollRads.std_dev())))

            if not math.isnan(self.maxPitchRads):
                output.append("{}: Min: {:.2f}° | Max: {:.2f}° | Amp: {:.2f}° | Avg: {:.2f}° | Std Dev: {:.2f}°".format(
                    "Pitch".ljust(justify), math.degrees(self.minPitchRads), math.degrees(self.maxPitchRads), 
                    math.degrees(self.ampPitchRads), math.degrees(self.avgPitchRads.mean()), 
                    math.degrees(self.avgPitchRads.std_dev())))

            # output.append("{}: {:.3f} s".format("Average Time Between States".ljust(justify), (self.endMillis - self.startMillis) / self.numStates / 1000))
            output.append("{}: {:.3f} s".format("Average Time Between States".ljust(justify), self.durationTravelledMillis / self.numStates / 1000))
            output.append("{}: {}".format("Number of States".ljust(justify), self.numStates))
            output.append("{}: {}".format("Number of Log Files".ljust(justify), self.numberOfLogs))

            if self.variables is not None and len(self.variables) > 0:
                section_variables_written = False
                for sectionName in self.variables:
                    section_name_written = False
                    variables = self.variables[sectionName]
                    if variables is None or len(variables) == 0:
                        continue
                    for entity in variables:
                        var = variables[entity]
                        if var is None:
                            continue
                        unitStr = self.units_mappings.get(sectionName, '')
                        unitStr = " {}".format(unitStr) if unitStr != "" else ""

                        if not section_variables_written:
                            output.append("{}:".format("Variables".ljust(justify)))
                            section_variables_written = True
                        if not section_name_written:
                            output.append("  {}:".format("{}".format(sectionName).ljust(justify)))
                            section_name_written = True

                        output.append("    {}: Min: {:.2f}{} | Max: {:.2f}{} | Amp: {:.2f}{} | Avg: {:.2f}{} | Std Dev: {:.2f}{}".format(
                            "{}".format(entity).ljust(justify),
                            var['min'], unitStr,
                            var['max'], unitStr,
                            var['max'] - var['min'], unitStr,
                            var['avg'].mean(), unitStr,
                            var['avg'].std_dev(), unitStr))

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

        sheetLine = 0
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

        sheet.write(sheetLine, 0, "Log Statistics", title_format)
        sheetLine += 1

        sheet.write(sheetLine, 0, "System", header_format)
        sheet.write(sheetLine, 1, self.systemName if self.systemName is not None else "Unknown", header_format)
        sheet.write(sheetLine, 3, "System ID", header_format)
        sheet.write(sheetLine, 4, "0x{:04X}".format(self.sourceId), header_format)
        sheet.write(sheetLine, 5, "{}".format(self.sourceId), header_format)
        sheetLine += 2

        sheet.write(sheetLine, 0, "Data Created", header_format)
        sheet.write(sheetLine, 1, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), header_value_format)
        sheetLine += 2

        sheet.write(sheetLine, 0, "Log Names", name_format)
        #sheet.write(sheetLine, 1, ", ".join(self.logNames) if self.logNames else "No logs found", value_format)
        idx = 0
        for v in self.logNames:
            sheet.write(sheetLine, 1 + idx, v, value_format)
            idx += 1
        sheetLine += 1
        sheet.write(sheetLine, 0, "Log Days", name_format)
        # sheet.write(sheetLine, 1, ", ".join(self.logDays) if self.logDays else "No log days found", value_format)
        idx = 0
        for v in self.logDays:
            sheet.write(sheetLine, 1 + idx, v, value_format)
            idx += 1
        sheetLine += 1
        sheet.write(sheetLine, 0, "Number of Log Files", name_format)
        sheet.write(sheetLine, 1, len(self.logNames), value_format)
        sheetLine += 2

        if self.startMillis < 0 or self.endMillis < 0:
            sheet.write(sheetLine, 0, "No data found in the logs.", title_format)
            sheetLine += 1
            return

        sheet.write(sheetLine, 0, "Start Time", header_format)
        sheet.write(sheetLine, 1, pd.to_datetime(self.startMillis, unit='ms', utc=True).strftime('%Y-%m-%dT%H:%M:%SZ'), header_value_format)
        sheet.write(sheetLine, 2, pd.to_datetime(self.startMillis, unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S'), header_value_format)
        sheet.write(sheetLine, 3, self.startMillis, header_value_format)
        sheet.write(sheetLine, 4, "ms", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 0, "End Time", header_format)
        sheet.write(sheetLine, 1, pd.to_datetime(self.endMillis, unit='ms', utc=True).strftime('%Y-%m-%dT%H:%M:%SZ'), header_value_format)
        sheet.write(sheetLine, 2, pd.to_datetime(self.endMillis, unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S'), header_value_format)
        sheet.write(sheetLine, 3, self.endMillis, header_value_format)
        sheet.write(sheetLine, 4, "ms", unit_format)
        sheetLine += 1
        
        sheet.write(sheetLine, 0, "Duration", header_format)
        sheet.write(sheetLine, 1, self.durationDaysPart, header_value_format)
        sheet.write(sheetLine, 2, "days", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 1, self.durationHoursPart, header_value_format)
        sheet.write(sheetLine, 2, "hours", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 1, self.durationMinutesPart, header_value_format)
        sheet.write(sheetLine, 2, "minutes", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 1, self.durationSecondsPart, header_value_format)
        sheet.write(sheetLine, 2, "seconds", unit_format)
        sheetLine += 2

        sheet.write(sheetLine, 0, "distance_travelled", header_format)
        sheet.write(sheetLine, 1, self.distance, header_value_format)
        sheet.write(sheetLine, 2, "m", unit_format)
        sheet.write(sheetLine, 3, "distance_travelled_nm", header_format)
        sheet.write(sheetLine, 4, self.distance * METERS_TO_NM, header_value_format)
        sheet.write(sheetLine, 5, "NM", unit_format)
        sheetLine += 2
        if self._smooth_filter:
            sheet.write(sheetLine, 0, "distance_travelled_smoothed", header_format)
            sheet.write(sheetLine, 1, self.distance_smoothed, header_value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheet.write(sheetLine, 3, "distance_travelled_smoothed_nm", header_format)
            sheet.write(sheetLine, 4, self.distance_smoothed * METERS_TO_NM, header_value_format)
            sheet.write(sheetLine, 5, "NM", unit_format)
            sheetLine += 2

        if self._smooth_filter:
            sheet.write(sheetLine, 0, "smooth_window", name_format)
            sheet.write(sheetLine, 1, self._sliding_window_size, value_format)
            sheetLine += 2

        sheet.write(sheetLine, 0, "speed_calc_avg", header_format)
        sheet.write(sheetLine, 1, self.avgSpeedMpsCalc, header_value_format)
        sheet.write(sheetLine, 2, "m/s", unit_format)
        sheet.write(sheetLine, 3, "speed_calc_avg_kn", header_format)
        sheet.write(sheetLine, 4, self.avgSpeedMpsCalc * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheetLine, 5, "kn", unit_format)
        sheetLine += 2
        if self._smooth_filter:
            sheet.write(sheetLine, 0, "speed_calc_smoothed_avg", header_format)
            sheet.write(sheetLine, 1, self.avgSpeedSmoothedMpsCalc, header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "speed_calc_smoothed_avg_kn", header_format)
            sheet.write(sheetLine, 4, self.avgSpeedSmoothedMpsCalc * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 2

        sheet.write(sheetLine, 0, "speed_min", header_format)
        sheet.write(sheetLine, 1, self.minSpeedMps, header_value_format)
        sheet.write(sheetLine, 2, "m/s", unit_format)
        sheet.write(sheetLine, 3, "speed_min_kn", header_format)
        sheet.write(sheetLine, 4, self.minSpeedMps * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheetLine, 5, "kn", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 0, "speed_max", header_format)
        sheet.write(sheetLine, 1, self.maxSpeedMps, header_value_format)
        sheet.write(sheetLine, 2, "m/s", unit_format)
        sheet.write(sheetLine, 3, "speed_max_kn", header_format)
        sheet.write(sheetLine, 4, self.maxSpeedMps * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheetLine, 5, "kn", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 0, "speed_avg", header_format)
        sheet.write(sheetLine, 1, self.avgSpeedMps.mean(), header_value_format)
        sheet.write(sheetLine, 2, "m/s", unit_format)
        sheet.write(sheetLine, 3, "speed_avg_kn", header_format)
        sheet.write(sheetLine, 4, self.avgSpeedMps.mean() * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheetLine, 5, "kn", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 0, "speed_std_dev", header_format)
        sheet.write(sheetLine, 1, self.avgSpeedMps.std_dev(), header_value_format)
        sheet.write(sheetLine, 2, "m/s", unit_format)
        sheet.write(sheetLine, 3, "speed_std_dev_kn", header_format)
        sheet.write(sheetLine, 4, self.avgSpeedMps.std_dev() * MPS_TO_KNOTS, header_value_format)
        sheet.write(sheetLine, 5, "kn", unit_format)
        sheetLine += 2
        if self._smooth_filter:
            sheet.write(sheetLine, 0, "speed_smoothed_min", header_format)
            sheet.write(sheetLine, 1, self.minSpeedSmoothedMps, header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "speed_smoothed_min_kn", header_format)
            sheet.write(sheetLine, 4, self.minSpeedSmoothedMps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "speed_smoothed_max", header_format)
            sheet.write(sheetLine, 1, self.maxSpeedSmoothedMps, header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "speed_smoothed_max_kn", header_format)
            sheet.write(sheetLine, 4, self.maxSpeedSmoothedMps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "speed_smoothed_avg", header_format)
            sheet.write(sheetLine, 1, self.avgSpeedSmoothedMps.mean(), header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "speed_smoothed_avg_kn", header_format)
            sheet.write(sheetLine, 4, self.avgSpeedSmoothedMps.mean() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "speed_smoothed_std_dev", header_format)
            sheet.write(sheetLine, 1, self.avgSpeedSmoothedMps.std_dev(), header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "speed_smoothed_std_dev_kn", header_format)
            sheet.write(sheetLine, 4, self.avgSpeedSmoothedMps.std_dev() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 2

        if not math.isnan(self.minVSpeedMps) and not math.isnan(self.maxVSpeedMps) \
                    and self.minVSpeedMps == self.maxVSpeedMps and self.maxVSpeedMps > 0:
            sheet.write(sheetLine, 0, "vertical_speed_min", header_format)
            sheet.write(sheetLine, 1, self.minVSpeedMps, header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "vertical_speed_min_kn", header_format)
            sheet.write(sheetLine, 4, self.minVSpeedMps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "vertical_speed_max", header_format)
            sheet.write(sheetLine, 1, self.maxVSpeedMps, header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "vertical_speed_max_kn", header_format)
            sheet.write(sheetLine, 4, self.maxVSpeedMps * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "vertical_speed_avg", header_format)
            sheet.write(sheetLine, 1, self.avgVSpeedMps.mean(), header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "vertical_speed__avg_kn", header_format)
            sheet.write(sheetLine, 4, self.avgVSpeedSps.mean() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "vertical_speed_std_dev", header_format)
            sheet.write(sheetLine, 1, self.avgVSpeedMps.std_dev(), header_value_format)
            sheet.write(sheetLine, 2, "m/s", unit_format)
            sheet.write(sheetLine, 3, "vertical_speed_std_dev_kn", header_format)
            sheet.write(sheetLine, 4, self.avgVSpeedMps.std_dev() * MPS_TO_KNOTS, header_value_format)
            sheet.write(sheetLine, 5, "kn", unit_format)
            sheetLine += 2
            if self._smooth_filter:
                sheet.write(sheetLine, 0, "vertical_speed_smoothed_min", header_format)
                sheet.write(sheetLine, 1, self.minVSpeedSmoothedMps, header_value_format)
                sheet.write(sheetLine, 2, "m/s", unit_format)
                sheet.write(sheetLine, 3, "vertical_speed_smoothed_min_kn", header_format)
                sheet.write(sheetLine, 4, self.minVSpeedSmoothedMps * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheetLine, 5, "kn", unit_format)
                sheetLine += 1
                sheet.write(sheetLine, 0, "vertical_speed_smoothed_max", header_format)
                sheet.write(sheetLine, 1, self.maxVSpeedSmoothedMps, header_value_format)
                sheet.write(sheetLine, 2, "m/s", unit_format)
                sheet.write(sheetLine, 3, "vertical_speed_smoothed_max_kn", header_format)
                sheet.write(sheetLine, 4, self.maxVSpeedSmoothedMps * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheetLine, 5, "kn", unit_format)
                sheetLine += 1
                sheet.write(sheetLine, 0, "vertical_speed_smoothed_avg", header_format)
                sheet.write(sheetLine, 1, self.avgVSpeedSmoothedMps.mean(), header_value_format)
                sheet.write(sheetLine, 2, "m/s", unit_format)
                sheet.write(sheetLine, 3, "vertical_speed_smoothed_avg_kn", header_format)
                sheet.write(sheetLine, 4, self.avgVSpeedSmoothedMps.mean() * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheetLine, 5, "kn", unit_format)
                sheetLine += 1
                sheet.write(sheetLine, 0, "vertical_speed_smoothed_std_dev", header_format)
                sheet.write(sheetLine, 1, self.avgVSpeedSmoothedMps.std_dev(), header_value_format)
                sheet.write(sheetLine, 2, "m/s", unit_format)
                sheet.write(sheetLine, 3, "vertical_speed_smoothed_std_dev_kn", header_format)
                sheet.write(sheetLine, 4, self.avgVSpeedSmoothedMps.std_dev() * MPS_TO_KNOTS, header_value_format)
                sheet.write(sheetLine, 5, "kn", unit_format)
                sheetLine += 2

        if not math.isnan(self.minLatRads) and not math.isnan(self.maxLatRads) \
                and not math.isnan(self.minLonRads) and not math.isnan(self.maxLonRads):
            sheet.write(sheetLine, 0, "geospatial_lat_min", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.minLatRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_lat_max", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.maxLatRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_lon_min", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.minLonRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_lon_max", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.maxLonRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 2
            sheet.write(sheetLine, 0, "geospatial_lat_avg", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgLatRads.mean()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_lat_std_dev", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgLatRads.std_dev()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_lon_avg", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgLonRads.mean()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_lon_std_dev", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgLonRads.std_dev()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 2
        
        if not math.isnan(self.maxHeight):
            sheet.write(sheetLine, 0, "geospatial_height_min", name_format)
            sheet.write(sheetLine, 1, self.minHeight, value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_height_max", name_format)
            sheet.write(sheetLine, 1, self.maxHeight, value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_height_avg", name_format)
            sheet.write(sheetLine, 1, self.avgHeight.mean(), value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_height_std_dev", name_format)
            sheet.write(sheetLine, 1, self.avgHeight.std_dev(), value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 2

        sectionVertical = False
        if not math.isnan(self.maxDepth) and self.maxDepth > 0:
            sheet.write(sheetLine, 0, "geospatial_depth_max", name_format)
            sheet.write(sheetLine, 1, self.maxDepth, value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_depth_avg", name_format)
            sheet.write(sheetLine, 1, self.avgDepth.mean(), value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_depth_std_dev", name_format)
            sheet.write(sheetLine, 1, self.avgDepth.std_dev(), value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sectionVertical = True
        if not math.isnan(self.maxAlt) and self.maxAlt > 0:
            sheet.write(sheetLine, 0, "geospatial_alt_max", name_format)
            sheet.write(sheetLine, 1, self.maxAlt, value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sectionVertical = True
            sheet.write(sheetLine, 0, "geospatial_alt_avg", name_format)
            sheet.write(sheetLine, 1, self.avgAlt.mean(), value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "geospatial_alt_std_dev", name_format)
            sheet.write(sheetLine, 1, self.avgAlt.std_dev(), value_format)
            sheet.write(sheetLine, 2, "m", unit_format)
            sheetLine += 1
            sectionVertical = True
        if sectionVertical:
            sheetLine += 1

        if not math.isnan(self.maxRollRads):
            sheet.write(sheetLine, 0, "attitude_roll_min", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.minRollRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_roll_max", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.maxRollRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_roll_amp", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.maxRollRads - self.minRollRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_roll_avg", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgRollRads.mean()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_roll_std_dev", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgRollRads.std_dev()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 2

        if not math.isnan(self.maxPitchRads):
            sheet.write(sheetLine, 0, "attitude_pitch_min", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.minPitchRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_pitch_max", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.maxPitchRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_pitch_amp", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.maxPitchRads - self.minPitchRads), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_pitch_avg", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgPitchRads.mean()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 1
            sheet.write(sheetLine, 0, "attitude_pitch_std_dev", name_format)
            sheet.write(sheetLine, 1, math.degrees(self.avgPitchRads.std_dev()), value_format)
            sheet.write(sheetLine, 2, "°", unit_format)
            sheetLine += 2
        
        sheet.write(sheetLine, 0, "time_between_states_avg", name_format)
        # sheet.write(sheetLine, 1, (self.endMillis - self.startMillis) / self.numStates / 1000, value_format)
        sheet.write(sheetLine, 1, self.durationTravelledMillis / self.numStates / 1000, value_format)
        sheet.write(sheetLine, 2, "s", unit_format)
        sheetLine += 1
        sheet.write(sheetLine, 0, "number_of_states", name_format)
        sheet.write(sheetLine, 1, self.numStates, value_format)
        sheetLine += 1
        sheet.write(sheetLine, 0, "number_of_log_files", name_format)
        sheet.write(sheetLine, 1, len(self.logNames), value_format)
        sheetLine += 2

        sheetLine += 2
        if self.variables is not None and len(self.variables) > 0:
            section_variables_written = False
            for sectionName in self.variables:
                section_name_written = False
                variables = self.variables[sectionName]
                if variables is None or len(variables) == 0:
                    continue
                for entity in variables:
                    var = variables[entity]
                    if var is None:
                        continue

                    unitStr = self.units_mappings.get(sectionName, '')

                    if not section_variables_written:
                        sheet.write(sheetLine, 0, "Variables", header_format)
                        sheetLine += 1
                        section_variables_written = True
                    if not section_name_written:
                        sheet.write(sheetLine, 1, sectionName, name_format)
                        sheetLine += 1
                        section_name_written = True

                    sheet.write(sheetLine, 2, entity, name_format)

                    sheet.write(sheetLine, 3, "min", name_format)
                    sheet.write(sheetLine, 4, var['min'], value_format)
                    if unitStr is not None and unitStr != '':
                        sheet.write(sheetLine, 5, unitStr, unit_format)
                    sheetLine += 1
                    sheet.write(sheetLine, 3, "max", name_format)
                    sheet.write(sheetLine, 4, var['max'], value_format)
                    if unitStr is not None and unitStr != '':
                        sheet.write(sheetLine, 5, unitStr, unit_format)
                    sheetLine += 1
                    sheet.write(sheetLine, 3, "amp", name_format)
                    sheet.write(sheetLine, 4, var['max'] - var['min'], value_format)
                    if unitStr is not None and unitStr != '':
                        sheet.write(sheetLine, 5, unitStr, unit_format)
                    sheetLine += 1
                    sheet.write(sheetLine, 3, "avg", name_format)
                    sheet.write(sheetLine, 4, var['avg'].mean(), value_format)
                    if unitStr is not None and unitStr != '':
                        sheet.write(sheetLine, 5, unitStr, unit_format)
                    sheetLine += 1
                    sheet.write(sheetLine, 3, "std_dev", name_format)
                    sheet.write(sheetLine, 4, var['avg'].std_dev(), value_format)
                    if unitStr is not None and unitStr != '':
                        sheet.write(sheetLine, 5, unitStr, unit_format)
                    sheetLine += 2
                    