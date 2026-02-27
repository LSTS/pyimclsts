import pandas as pd
import argparse
import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D

from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel, WhiteKernel, Matern, RationalQuadratic
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

if __name__ == '__main__':

    # Parser for command line
    parser = argparse.ArgumentParser(description="Process arguments for Caravel Data Analysis \n" \
                                                "WARNING: If you have all your data in a csv, simply USE --csv_path.")

    # Path to the mission argument
    parser.add_argument('-e', '--sea_export', type=bool, default=False,
                        help="Set to True to compile sea data and caravel data in single csv. Change paths on the file itself")
    
    parser.add_argument('-dp', '--data_path', type=str, default=os.getcwd(),
                        help="Absolute Path to excel file with caravel AND sea data")
    
    args = parser.parse_args()
    export = args.sea_export
    fullData = args.data_path

    caravelPaths = ['/home/ruben/Workspace/pyimclsts/outdata/caravel_2025_07_30.xlsx', '/home/ruben/Workspace/pyimclsts/outdata/caravel_2025_08_04.xlsx']
    seaPaths = ['/home/ruben/Workspace/pyimclsts/outdata/sea_state.nc', '/home/ruben/Workspace/pyimclsts/outdata/currents.nc']

    # Open Data sets and turn them into pandas dataframes 
    # Variables to look up on Sea Dataset 
    # Wave Signficant Height, Spectral Density Maximum, Wave From direction, Sea Surface Wave Wind Direction, "current" speed x,y  
    waveVarNames = ['VHM0', 'VTPK', 'VMDR', 'VMDR_WW', 'VHM0_WW', 'VTM01_WW', 'VSDX', 'VSDY']
    currentVarNames = ['uo', 'vo']
    
    if export:     
    
        print("### ------------------ ###")
        print("Combining Sea Data and Caravel Data into a Single File")

        ## First the Sea Data
        seaData = xr.open_dataset(seaPaths[0])
        seaData['time'] = xr.DataArray(
            pd.to_datetime(seaData['time'].values, unit='s', utc=True),
            dims='time'
        )

        currentsData = xr.open_dataset(seaPaths[1])
        currentsData['time'] = xr.DataArray(
            pd.to_datetime(currentsData['time'].values, unit='s', utc=True), 
            dims='time'
        )

        ## Second Caravel's Auxiliary Data
        caravelDataDf = pd.read_excel(caravelPaths[0])
        caravelDataDf['TIME'] = pd.to_datetime(caravelDataDf['TIME'], unit='s', utc=True)
        caravelDataDf = caravelDataDf.set_index('TIME').sort_index()
        caravelDataDf = caravelDataDf[caravelDataDf['THRUSTER'] == 0]
        caravelDataDf = caravelDataDf.drop(columns='THRUSTER')

        ## Second Caravel's Auxiliary Data
        caravelDataAuxDf = pd.read_excel(caravelPaths[1])
        caravelDataAuxDf['TIME'] = pd.to_datetime(caravelDataAuxDf['TIME'], unit='s', utc=True)
        caravelDataAuxDf = caravelDataAuxDf.set_index('TIME').sort_index()
        caravelDataAuxDf = caravelDataAuxDf[caravelDataAuxDf['THRUSTER'] == 0]
        caravelDataAuxDf = caravelDataAuxDf.drop(columns='THRUSTER')

        combined = pd.concat([caravelDataDf, caravelDataAuxDf])
        combined = combined[~combined.index.duplicated(keep='first')]
        combined = combined.sort_index()        

        caravelDataDf = combined

        ## Added the columns coming from sea data
        for var in waveVarNames: 
            caravelDataDf[var]  = np.nan

        for var in currentVarNames:
            caravelDataDf[var] = np.nan
        
        # Slice data based on time 
        cut_time = pd.to_datetime("2025-08-04 15:00:00.197859049", utc=True)
        caravelDataDf = caravelDataDf.loc[cut_time:]    

        ## Slice data based on velocity
        caravelDataDf['VMAG'] = np.sqrt(
        caravelDataDf['VX']**2 + caravelDataDf['VY']**2)
        
        # Filter out values of velocity that are inside the 0 to 0.1 m/s range 
        caravelDataDf = caravelDataDf[
             (caravelDataDf["VMAG"] > 0.1)
        ]

        # Convert seaData.time to naive UTC
        seaData['time'] = xr.DataArray(
            pd.to_datetime(seaData['time'].values).tz_localize(None),  
            dims='time'
        )

        currentsData['time'] = xr.DataArray(
            pd.to_datetime(currentsData['time'].values).tz_localize(None),
            dims='time'
        )

        # Convert Caravel index to naive UTC
        caravelDataDf.index = caravelDataDf.index.tz_convert(None)

        ## Nearest neighbour data first for 1d dimensions array
        seaDataNearest = seaData.sel(
            latitude=xr.DataArray(caravelDataDf['LATITUDE'].reindex(seaData.time, method='nearest'), dims='time'),
            longitude=xr.DataArray(caravelDataDf['LONGITUDE'].reindex(seaData.time, method='nearest'), dims='time'),
            method='nearest'
        )

        currentsDataNearest = currentsData.sel(
            latitude=xr.DataArray(caravelDataDf['LATITUDE'].reindex(currentsData.time, method='nearest'), dims='time'),
            longitude=xr.DataArray(caravelDataDf['LONGITUDE'].reindex(currentsData.time, method='nearest'), dims='time'),
            method='nearest'
        )

        # Linear regression based on that time
        every10MinTime = pd.date_range(start=caravelDataDf.index[0], end=caravelDataDf.index[-1], freq='10min')

        seaDataLinear = seaDataNearest.interp(
            time=caravelDataDf.index,
            method ='linear'
            )
        
        currentsDataLinear = currentsDataNearest.interp(
            time=caravelDataDf.index, 
            method='linear'
        )

        ## Visually confirm that linear interpolation worked

        WaveHeightName = 'VHM0'
        currentNameX = 'uo'
        currentNameY = 'vo'

        plt.figure(figsize=(12,6))

        plt.plot(seaDataNearest.time, seaDataNearest[WaveHeightName], 'o-', label='3-hourly (nearest)', markersize=4)
        plt.plot(seaDataLinear.time, seaDataLinear[WaveHeightName], '-', label='10-min linear interpolation', alpha=0.7)

        plt.xlabel('Time')
        plt.ylabel(WaveHeightName)
        plt.title("{} Along Caravel Trajectory")
        plt.legend()
        plt.grid(True)

        plt.figure()

        plt.plot(currentsDataNearest.time, currentsDataNearest[currentNameX], 'o-', label='3-hour', markersize=4)
        plt.plot(currentsDataLinear.time, currentsDataLinear[currentNameX], '-', label='10-min linear interpolation', alpha=0.7)

        plt.xlabel('Time')
        plt.ylabel(currentNameX)
        plt.title("{} Along Caravel Current")
        plt.legend()
        plt.grid(True)

        plt.show()

        ## Fill dataset
        for var in waveVarNames:

            caravelDataDf[var] = seaDataLinear[var].values

        for var in currentVarNames:
            caravelDataDf[var] = currentsDataLinear[var].values


        caravelDataDf.to_csv("caravel_07_30.csv")


    else: 

        caravelDataDf = pd.read_csv(fullData)

        # Convert epoch -> datetime (UTC)
        caravelDataDf["TIME"] = pd.to_datetime(
            caravelDataDf["TIME"],
            utc=True
        )

        #### ------- TIME FILTERING HERE ------- ####
        caravelDataDf = caravelDataDf.set_index("TIME").sort_index()
        
        cut_time1 = pd.to_datetime("2025-08-04 08:00:00.197859049", utc=True)
        cut_time2 = pd.to_datetime("2025-08-07 15:00:00.197859049", utc=True)

        #caravelDataDf = caravelDataDf.loc[cut_time2:]
        
        #### ------- VELOCITY FILTERING HERE ------- ####
        # Vx and Vy are in the global frame
        caravelDataDf['VMAG'] = np.sqrt(
            caravelDataDf['VX']**2 + caravelDataDf['VY']**2)
        
        caravelDataDf['HDNG'] = np.deg2rad(caravelDataDf['HDNG']) 
        caravelDataDf['HDNG'] = np.mod(caravelDataDf['HDNG'], 2*np.pi) 


                
        # Get V_X and V_Y in the frame of the vehicle
        # To do so, apply rotational matrix#
        caravelDataDf['V_X'] = np.cos(caravelDataDf['HDNG'])*caravelDataDf['VX'] + np.sin(caravelDataDf['HDNG'])*caravelDataDf['VY']
        caravelDataDf['V_Y'] = -np.sin(caravelDataDf['HDNG'])*caravelDataDf['VX'] + np.cos(caravelDataDf['HDNG'])*caravelDataDf['VY']

        # Filter out values of velocity that are inside the 0 to 0.1 m/s range 
        caravelDataDf = caravelDataDf[
             (caravelDataDf["VMAG"] > 0.1)
        ]

        ## Slice it just for the 1st day
        start = pd.Timestamp(caravelDataDf.index[0]) 
        one_day = start + pd.Timedelta(days=1)
        #caravelDataDf = caravelDataDf.loc['2025-06-30']
        
         ## ------- WIND DATA ------- ## 
        # Calculate Angle of attack of the wind
        # Rotate Wind to WHERE it is going
        caravelDataDf['WIND_DIR'] = caravelDataDf['WIND_DIR'] + np.pi
        # Center it around 2pi
        caravelDataDf['WIND_DIR'] = np.mod(caravelDataDf['WIND_DIR'], 2*np.pi)
        caravelDataDf['WIND_AA'] =  caravelDataDf['HDNG'] - caravelDataDf['WIND_DIR']

        # Wind magnitude and proportion
        caravelDataDf['WIND_AX'] = np.cos(caravelDataDf['HDNG'] - caravelDataDf['WIND_DIR'])
        caravelDataDf['WIND_AY'] = np.sin(caravelDataDf['HDNG'] - caravelDataDf['WIND_DIR'])

        # Wind Magnitude times 
        caravelDataDf['WIND_X'] = caravelDataDf['WIND_AX']*caravelDataDf['WIND_VAL']
        caravelDataDf['WIND_Y'] = caravelDataDf['WIND_AY']*caravelDataDf['WIND_VAL']

        ## ----------- CURRENT DATA ----------- ##
        caravelDataDf['CANG'] = np.arctan(caravelDataDf['vo'], caravelDataDf['uo'])
        caravelDataDf['CMAG'] = np.sqrt(
            caravelDataDf['uo']**2 + caravelDataDf['vo']**2
        )
        caravelDataDf['CATK_X'] = np.cos(caravelDataDf['HDNG'] - caravelDataDf['CANG'])
        caravelDataDf['CATK_Y'] = np.sin(caravelDataDf['HDNG'] - caravelDataDf['CANG'])
        caravelDataDf['CONX'] = caravelDataDf['CMAG']*caravelDataDf['CATK_X']
        caravelDataDf['CONY'] = caravelDataDf['CMAG']*caravelDataDf['CATK_Y']


        ## ----------- WAVE DIRECTION DATA ----------- ##
        caravelDataDf['VMDR'] = np.deg2rad(caravelDataDf['VMDR'])

        # Wave data is FROM. rotate it 180º 
        caravelDataDf['VMDR'] = caravelDataDf['VMDR'] + np.pi
        caravelDataDf['VMDR'] = np.mod(caravelDataDf['VMDR'], 2*np.pi)

        caravelDataDf['VMDR_WW'] = np.deg2rad(caravelDataDf['VMDR_WW'])
        caravelDataDf['WATK'] = caravelDataDf['HDNG'] - caravelDataDf['VMDR']
        caravelDataDf['WATK_X'] = np.cos(caravelDataDf['WATK'])
        caravelDataDf['WATK_Y'] = np.sin(caravelDataDf['WATK'])
        caravelDataDf['W_F'] = (1 / caravelDataDf['VTPK'])

        print("## Calculating rolling Window!!")

        # Get all columns as a list
        all_columns = caravelDataDf.columns.tolist()

        # Remove LATITUDE and LONGITUDE if they exist
        spatial_cols = ['LATITUDE', 'LONGITUDE']
        data_cols = [col for col in all_columns if col not in spatial_cols]
        
        caravelDataDfRolling = caravelDataDf[data_cols]
        print(caravelDataDfRolling.head(100))

        caravelDataDfRolling = (
            caravelDataDfRolling
                .rolling('60min', center=True)
                .mean()
        )

        caravelDataDfRolling[spatial_cols] = caravelDataDf[spatial_cols] 

        caravelDataDfRolling = caravelDataDfRolling.resample('30min').first()

        Vx = caravelDataDfRolling['VX']
        Vy = caravelDataDfRolling['VY']
        Vmag = np.sqrt(Vx**2 + Vy**2)

        ## Filtering 

        CMAG = np.sqrt(caravelDataDfRolling['uo']**2 + caravelDataDfRolling['vo']**2)
        CANG = np.arctan2(caravelDataDfRolling['uo'], caravelDataDfRolling['vo'])

        ## Filtering Asked velocity and direction difference 
        delta_heading = np.abs(caravelDataDfRolling['HDNG'] - CANG)

        # Wrap differences to [0, π] (so 330° ~ 30° works correctly)
        delta_heading = np.mod(delta_heading + np.pi, 2*np.pi) - np.pi
        delta_heading = np.abs(delta_heading)

        angle_threshold = np.deg2rad(60)  # 30 degrees in radians
        current_threshold = 0.05           # m/s

        mask_filter = ((delta_heading <= angle_threshold) & (CMAG >= current_threshold)) | ((CMAG <= current_threshold) & (delta_heading <= 90))
        # -------------------------------
        # 1️⃣ Choose your day
        # -------------------------------
        #dia_escolhido = '2025-08-09'
        #df_day = caravelDataDfRolling.loc[dia_escolhido]

        df_day = caravelDataDfRolling

        df_day = df_day[mask_filter].copy()

        # -------------------------------
        # 2️⃣ Extract valid data
        # -------------------------------
        lat = df_day['LATITUDE'].values
        lon = df_day['LONGITUDE'].values
        HDNG = df_day['HDNG'].values   # vehicle heading in radians
        VMAG = df_day['VMAG'].values   # vehicle speed

        WDIR = df_day['WIND_DIR'].values 
        WVAL = df_day['WIND_VAL'].values        

        Cx = df_day['uo'].values       # current x
        Cy = df_day['vo'].values       # current y

        Wx = WVAL * np.sin(WDIR)
        Wy = WVAL * np.cos(WDIR)

        Hs = df_day['VHM0'].values     # wave height
        Wf = df_day['W_F']

        # Mask NaNs
        mask = (~np.isnan(HDNG)) & (~np.isnan(VMAG)) & (~np.isnan(Cx)) & (~np.isnan(Cy))
        lat = lat[mask]
        lon = lon[mask]
        HDNG = HDNG[mask]
        VMAG = VMAG[mask]
        Cx = Cx[mask]
        Cy = Cy[mask]
        Hs = Hs[mask]


        # -------------------------------
        # 3️⃣ Compute vector components
        # -------------------------------
        scale_vehicle = 0.02   # much smaller
        scale_current = 0.008  # tiny arrows for current

        Vx = VMAG * np.sin(HDNG)
        Vy = VMAG * np.cos(HDNG)
        cols_of_interest = ['LATITUDE', 'LONGITUDE', 'VMAG']
        print(df_day[cols_of_interest].dropna().tail(10))
        #print(df_day[cols_of_interest].dropna().head(20))
        # -------------------------------
        # 4️⃣ Create plot
        # -------------------------------
        plt.figure(figsize=(12,10))

        data_offset = 0.005/2

        # Circle for wave height
        sc1 = plt.scatter(
            lon, lat - data_offset, 
            c=Hs, 
            cmap='viridis', 
            s=40, 
            edgecolors='k', linewidths=0.3, 
            alpha=0.7,
            label='Wave Height (m)', 
            zorder=3,
            marker='o'
        )   

        # Square for wave frequency
        sc2 = plt.scatter(
            lon, lat + data_offset, 
            c=Wf, 
            cmap='viridis',   # different colormap to differentiate
            s=40, 
            edgecolors='k', linewidths=0.3, 
            alpha=0.9,
            label='Wave Frequency (Hz)', 
            zorder=4,
            marker='s'
        )

        plt.colorbar(sc2, label='Wave Frequency (Hz)')
        plt.colorbar(sc1, label="Wave Height (m)")


        # Vehicle vectors
        plt.quiver(
            lon, lat,
            Vx, Vy,
            color='blue',
            angles='xy', scale_units='xy', scale=25,  # scale controls actual length
            width=0.002, headwidth=3, headlength=4, alpha=0.6
        )

        # Current vectors
        plt.quiver(
            lon, lat,
            Cx, Cy,
            color='red',
            angles='xy', scale_units='xy', scale=10,  # much smaller
            width=0.0015, headwidth=2, headlength=3, alpha=0.6
        )

         # Current vectors
        plt.quiver(
            lon, lat,
            Wx, Wy,
            color='green',
            angles='xy', scale_units='xy', scale=200,  # much smaller
            width=0.0015, headwidth=2, headlength=3, alpha=0.6
        )

        # Plot trajectory as a thin line
        plt.plot(
            lon, lat,
            color='black',       # trajectory color
            linewidth=0.2,       # make it thin
            linestyle='-',       # solid line
            zorder=2,            # behind arrows and scatter
            label='Trajectory'
        )

        # Trajectory
        #plt.plot(df_day['LONGITUDE'], df_day['LATITUDE'], color="black", lin)

        # -------------------------------
        # 5️⃣ Add magnitudes as text
        # -------------------------------
        text_offset = 0.0008
        mag_vehicle = np.sqrt(Vx**2 + Vy**2)
        mag_current = np.sqrt(Cx**2 + Cy**2)

        for i in range(len(lon)):
            plt.text(lon[i] + Vx[i]*scale_vehicle + text_offset,
                    lat[i] + Vy[i]*scale_vehicle + text_offset,
                    f"{mag_vehicle[i]:.2f}", color='blue', fontsize=7)
            
            plt.text(lon[i] + Cx[i]*scale_current + text_offset,
                    lat[i] + Cy[i]*scale_current + text_offset,
                    f"{mag_current[i]:.2f}", color='red', fontsize=7)
            
            plt.text(lon[i] + Wx[i]*scale_current + text_offset,
                    lat[i] + Wy[i]*scale_current + text_offset,
                    f"{WVAL[i]:.2f}", color='green', fontsize=7)

        # -------------------------------
        # 6️⃣ Legend
        # -------------------------------
        legend_elements = [
            Line2D([0],[0], color='blue', lw=2, label='Vehicle Heading'),
            Line2D([0],[0], color='red', lw=2, label='Current'),
            Line2D([0],[0], color='green', lw=2, label='Wind Heading'),
            plt.scatter([],[], c='gray', s=40, edgecolors='k', linewidths=0.3, label='Wave Height')
        ]

        plt.legend(handles=legend_elements, loc='upper left')

        # -------------------------------
        # 7️⃣ Final touches
        # -------------------------------
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title(f'Vehicle Heading & Current Vectors')
        plt.grid(True, alpha=0.3)
        plt.axis('equal')
        plt.show()