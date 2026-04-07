import pandas as pd
import argparse
import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from utide import solve, reconstruct
from oceans.filters import pl33tn


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

    caravelPaths = ['/home/ruben/Workspace/pyimclsts/data/caravel_2025_08_03_loiter.xlsx',
                     '/home/ruben/Workspace/pyimclsts/data/caravel_2025_08_04_loiter.xlsx']
    #caravelPaths = ['/home/ruben/Workspace/pyimclsts/data/caravel_2025_06_30.xlsx']

    seaPaths = ['/home/ruben/Workspace/pyimclsts/data/copernicus/sea_state.nc', '/home/ruben/Workspace/pyimclsts/data/copernicus/currents.nc']

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

        if len(caravelPaths) >= 2:
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
        #cut_time = pd.to_datetime("2025-08-04 15:00:00.197859049", utc=True)
        #caravelDataDf = caravelDataDf.loc[cut_time:]    

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


        caravelDataDf.to_csv("caravel_06_30.csv")


    else: 

        caravelDataDf = pd.read_csv(fullData)

        # Convert epoch -> datetime (UTC)
        caravelDataDf["TIME"] = pd.to_datetime(
            caravelDataDf["TIME"],
            utc=True
        )

        #### ------- TIME FILTERING HERE ------- ####
        caravelDataDf = caravelDataDf.set_index("TIME").sort_index()

        cut_time = pd.to_datetime("2025-08-04 16:00:00.197859049", utc=True)
        cut_time_ending = pd.to_datetime("2025-08-07 14:00:00.197859049", utc=True)
        caravelDataDf = caravelDataDf[cut_time:]

        #### ------- LOITERS Filtering -------- ####
        caravelDataDf = caravelDataDf[(caravelDataDf['LRadius'] == 0)]

        #### ------- THRUSTER FILTERING -------- ####
        #caravelDataDf = caravelDataDf[caravelDataDf['THRUSTER'] == 0.75]
        
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

        print("## Calculating rolling Window!!")

        # Get all columns as a list
        all_columns = caravelDataDf.columns.tolist()

        # Remove LATITUDE and LONGITUDE if they exist
        spatial_cols = ['LATITUDE', 'LONGITUDE']
        data_cols = [col for col in all_columns if col not in spatial_cols]
        
        caravelDataDfRolling = caravelDataDf[data_cols]
        caravelDataDfRolling = (
            caravelDataDfRolling
                .rolling('60min', center=True)
                .mean()
        )

        caravelDataDfRolling[spatial_cols] = caravelDataDf[spatial_cols] 
        caravelDataDfRolling = caravelDataDfRolling.resample('30min').first()

        series = caravelDataDfRolling.copy()

        series['VMAG'] = series['VMAG'].interpolate(method='time')

        mirror = series[::-1].copy()

        dt = series.index[1] - series.index[0]
        mirror.index = series.index[-1] + dt + np.arange(len(mirror)) * dt

        series = pd.concat([series, mirror])
    
        ## Apply pl33 ocean dynamics filter ##
        filt_vel_mag = pl33tn(series['VMAG'], dt=1.0, T=33)

        filtered_index = series.index[:len(filt_vel_mag)]
        series = series[:len(filt_vel_mag)]

        # Create figure
        fig, ax1 = plt.subplots(figsize=(12,6))

        # -------------------
        # LEFT AXIS (Speed)
        # -------------------
    
        ax1.plot(filtered_index, filt_vel_mag,
                label='Filtered Vehicle Velocity (m/s)')
        
        ax1.plot(filtered_index, series['VMAG'],
                    label="Vehicle Velocity Magnitude")
        
        ax1.set_ylabel('Vehicle Magnitudes')
        ax1.set_xlabel('Date')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # Right Axis
        ax2 = ax1.twinx()

        ax2.plot(filtered_index, series['VHM0'],
                 label="Wave Height", color='red')
        ax2.legend(loc='upper right')
        plt.title('Daily Caravel Speed and Wave Height')
        plt.show()

        # Garantir que filt_vel_mag é 1-dimensional
        filt_vel_mag_1d = np.ravel(filt_vel_mag)  # ou filt_vel_mag.flatten()

        # Criar um DataFrame temporário com os dados filtrados
        # Garantir que todos os arrays têm a mesma dimensão e são 1D
        n_points = min(len(filt_vel_mag_1d), len(series['VHM0'].values), len(filtered_index))

        ## LINEAR REGRESSION HERE
        filtered_data = pd.DataFrame({
            'VMAG_filtered': filt_vel_mag_1d[:n_points],
            'VHM0': series['VHM0'].values[:n_points], 
            'LATITUDE': series['LATITUDE'].reindex(filtered_index[:n_points], method="nearest").values[:n_points],
            'LONGITUDE' : series['LONGITUDE'].reindex(filtered_index[:n_points], method="nearest").values[:n_points]
        }, index=filtered_index[:n_points])

        filtered_data = filtered_data.dropna()

        cut_time1 = pd.to_datetime("2025-08-04 12:00:00.000000000", utc=True)
        cut_time2 = pd.to_datetime("2025-08-07 15:00:00.000000000", utc=True)

        filtered_data = filtered_data[cut_time1:cut_time2]

        ## FILTERED REGRESSION PLOT ##
        plt.figure(figsize=(12,8))

        # Extract date from index (assuming datetime index)
        days = filtered_data.index.date

        # Create a colormap based on days
        unique_days = np.unique(days)
        day_colors = {day: i for i, day in enumerate(unique_days)}

        # Map each point to a color based on its day
        colors = [day_colors[day] for day in days]

        # Create scatter plot with colormap
        scatter = plt.scatter(filtered_data['VHM0'], filtered_data['VMAG_filtered'],
                            c=colors, cmap='viridis', alpha=0.6, edgecolors='k', 
                            linewidths=0.5, s=50)

        # Add colorbar to show day mapping
        cbar = plt.colorbar(scatter)
        cbar.set_label('Day Number')
        cbar.set_ticks(range(len(unique_days)))
        cbar.set_ticklabels([day.strftime('%Y-%m-%d') for day in unique_days])

        if len(filtered_data) > 1:
            z = np.polyfit(filtered_data['VHM0'], filtered_data['VMAG_filtered'], 2)
            p = np.poly1d(z)
            
            # Extrair os parâmetros (coeficientes)
            a = z[0]  # coeficiente de x² (quadrático)
            b = z[1]  # coeficiente de x (linear)
            c = z[2]  # termo constante
            
            # Calcular correlação
            corr = np.corrcoef(filtered_data['VHM0'], filtered_data['VMAG_filtered'])[0,1]
            
            x_sorted = np.sort(filtered_data['VHM0'])
            plt.plot(x_sorted, p(x_sorted), 
                    "r--", linewidth=2, 
                    label=f'Tendência quadrática: {a:.3e}x² + {b:.3e}x + {c:.3e} (r² = {corr**2:.3f})')
            
            # Imprimir parâmetros
            print(f"Parâmetros da linha quadrática:")
            print(f"  a (x²): {a:.6f}")
            print(f"  b (x):  {b:.6f}")
            print(f"  c:      {c:.6f}")
            print(f"  r²:     {corr**2:.6f}")

        plt.xlabel('Altura Significativa da Onda - VHM0 (m)')
        plt.ylabel('Velocidade Filtrada do Veículo - VMAG (m/s)')
        plt.title('Relação entre Velocidade Filtrada (pl33tn) e Altura da Onda')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.show()


        VEL_MIN, VEL_MAX = 0.55, 0.75
        HEIGHT_MIN, HEIGHT_MAX = 1.1, 2.0
        MASK = ( (filtered_data['VMAG_filtered'] >= VEL_MIN) &
                 (filtered_data['VMAG_filtered'] <= VEL_MAX) & 
                 (filtered_data['VHM0'] >= HEIGHT_MIN)       &
                 (filtered_data['VHM0'] <= HEIGHT_MAX)
                )
        
        dados_filtrados = filtered_data[MASK].copy()

        # -------------------------------
        # Plot 1: Série temporal com duas variáveis
        # -------------------------------
        fig, ax1 = plt.subplots(figsize=(15, 6))

        # Plot velocidade filtrada no eixo esquerdo
        color = 'tab:blue'
        ax1.set_xlabel('Tempo')
        ax1.set_ylabel('Velocidade Filtrada (m/s)', color=color)
        ax1.scatter(dados_filtrados.index, dados_filtrados['VMAG_filtered'], 
                color=color, label='VMAG filtrada')
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.axhline(y=VEL_MIN, color='gray', linestyle='--', alpha=0.5)
        ax1.axhline(y=VEL_MAX, color='gray', linestyle='--', alpha=0.5)
        ax1.grid(True, alpha=0.3)

        # Criar segundo eixo para altura de onda
        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('Altura da Onda - VHM0 (m)', color=color)
        ax2.scatter(dados_filtrados.index, dados_filtrados['VHM0'], 
                color=color, label='VHM0')
        ax2.tick_params(axis='y', labelcolor=color)
        ax2.axhline(y=HEIGHT_MIN, color='gray', linestyle='--', alpha=0.5)
        ax2.axhline(y=HEIGHT_MAX, color='gray', linestyle='--', alpha=0.5)

        # Título e legendas combinadas
        plt.title(f'Velocidade Filtrada vs Altura de Onda\n'
                f'VMAG [{VEL_MIN}-{VEL_MAX}] m/s, VHM0 [{HEIGHT_MIN}-{HEIGHT_MAX}] m')
        
        # Adicionar legendas manualmente
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

        plt.tight_layout()
        plt.show()

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

        mask_filter = (
            (delta_heading <= angle_threshold) & (CMAG >= current_threshold)) | ((CMAG <= current_threshold) & (delta_heading <= 90))

        print("Depois do filtro aplicado")
        df_day = caravelDataDfRolling
        specific_day = '2025-08-05'
        df_day = df_day.loc[specific_day]
        df_day = df_day[mask_filter].copy()


        timestamps_filtrados = df_day.index
        timestamps_existentes = filtered_data.index.intersection(timestamps_filtrados)
        filtered_data = filtered_data.loc[timestamps_existentes].copy()

        ## FILTERED REGRESSION PLOT ##
        plt.figure(figsize=(12,8))

        # Extract date from index (assuming datetime index)
        days = filtered_data.index.date

        # Create a colormap based on days
        unique_days = np.unique(days)
        day_colors = {day: i for i, day in enumerate(unique_days)}

        # Map each point to a color based on its day
        colors = [day_colors[day] for day in days]

        # Create scatter plot with colormap
        scatter = plt.scatter(filtered_data['VHM0'], filtered_data['VMAG_filtered'],
                            c=colors, cmap='viridis', alpha=0.6, edgecolors='k', 
                            linewidths=0.5, s=50)

        # Add colorbar to show day mapping
        cbar = plt.colorbar(scatter)
        cbar.set_label('Day Number')
        cbar.set_ticks(range(len(unique_days)))
        cbar.set_ticklabels([day.strftime('%Y-%m-%d') for day in unique_days])

        if len(filtered_data) > 1:
            z = np.polyfit(filtered_data['VHM0'], filtered_data['VMAG_filtered'], 2)
            p = np.poly1d(z)
            
            # Extrair os parâmetros (coeficientes)
            a = z[0]  # coeficiente de x² (quadrático)
            b = z[1]  # coeficiente de x (linear)
            c = z[2]  # termo constante
            
            # Calcular correlação
            corr = np.corrcoef(filtered_data['VHM0'], filtered_data['VMAG_filtered'])[0,1]
            
            x_sorted = np.sort(filtered_data['VHM0'])
            plt.plot(x_sorted, p(x_sorted), 
                    "r--", linewidth=2, 
                    label=f'Tendência quadrática: {a:.3e}x² + {b:.3e}x + {c:.3e} (r² = {corr**2:.3f})')
            
            # Imprimir parâmetros
            print(f"Parâmetros da linha quadrática:")
            print(f"  a (x²): {a:.6f}")
            print(f"  b (x):  {b:.6f}")
            print(f"  c:      {c:.6f}")
            print(f"  r²:     {corr**2:.6f}")

        plt.xlabel('Altura Significativa da Onda - VHM0 (m)')
        plt.ylabel('Velocidade Filtrada do Veículo - VMAG (m/s)')
        plt.title('Relação entre Velocidade Filtrada (pl33tn) e Altura da Onda')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.show()
            
        df_day['VMAG'] = filtered_data['VMAG_filtered']

        # -------------------------------
        # 2️⃣ Extract valid data
        # -------------------------------
        lat = df_day['LATITUDE'].values
        lon = df_day['LONGITUDE'].values
        HDNG = df_day['HDNG'].values   # vehicle heading in radians
        VMAG = df_day['VMAG'].values   # vehicle speed
        VMDR = df_day['VMDR'].values
        
        WIND_DIR = df_day['WIND_DIR'].values 
        WIND_VAL = df_day['WIND_VAL'].values        

        Cx = df_day['uo'].values       # current x
        Cy = df_day['vo'].values       # current y

        Wx = WIND_VAL * np.sin(WIND_DIR)
        Wy = WIND_VAL * np.cos(WIND_DIR)

        Hs = df_day['VHM0'].values     # wave height
        Wp = df_day['VTPK'].values

        Wave_x = Hs * np.sin(df_day['VMDR'])
        Wave_y = Hs * np.cos(df_day['VMDR'])

        # Mask NaNs
        mask = (~np.isnan(HDNG)) & (~np.isnan(VMAG)) & (~np.isnan(Cx)) & (~np.isnan(Cy))
        lat = lat[mask]
        lon = lon[mask]
        HDNG = HDNG[mask]
        VMAG = VMAG[mask]
        VMDR = VMDR[mask]
        Cx = Cx[mask]
        Cy = Cy[mask]
        Hs = Hs[mask]
        Wp = Wp[mask]
        Wave_x = Wave_x[mask]
        Wave_y = Wave_y[mask]
        Wx = Wx[mask]
        Wy = Wy[mask]

        # 2. Verificar DEPOIS do resample
        print("\n2️⃣ DEPOIS DO RESAMPLE:")
        print("-"*40)

        WpNan = np.isnan(Wp).sum()
        latNan = np.isnan(lat).sum()
        lonNan = np.isnan(lon).sum()

        wp_len = len(Wp)
        lat_len = len(lat)
        lon_len = len(lon)

        print("WpNan: {}".format(WpNan))
        print("latNan: {}".format(latNan))
        print("lonNan: {}".format(lonNan))

        print("Wp Len: {}".format(wp_len))
        print("Lat Len: {}".format(lat_len))
        print("Lot Len: {}".format(lon_len))
    

        # -------------------------------
        # 3️⃣ Compute vector components
        # ------------------------------
        Vx = VMAG * np.sin(HDNG)
        Vy = VMAG * np.cos(HDNG)
        cols_of_interest = ['LATITUDE', 'LONGITUDE', 'VMAG']
        
        # -------------------------------
        # 4️⃣ Create plot
        # -------------------------------
        plt.figure(figsize=(12,10))

        data_offset = 0.005/2

        # Find first timestamp of each day
        day_starts = df_day.groupby(df_day.index.date).head(1)
        plt.scatter(
            day_starts['LONGITUDE'],
            day_starts['LATITUDE'],
            marker='*',
            s=150,
            color='black',
            edgecolors='yellow',
            linewidths=1.5,
            zorder=5,
            label='Start of Day'
        )

        for idx, row in day_starts.iterrows():
            plt.text(
                row['LONGITUDE'],
                row['LATITUDE'],
                idx.strftime('%m-%d'),
                fontsize=8,
                ha='left',
                va='bottom'
            )

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
            c=Wp, 
            cmap='viridis',   # different colormap to differentiate
            s=40, 
            edgecolors='k', linewidths=0.3, 
            alpha=0.9,
            label='Wave Period (s)', 
            zorder=4,
            marker='s'
        )
        
        plt.colorbar(sc1, label="Wave Height (m)", orientation='horizontal', pad=0.02, shrink=0.7)
        plt.colorbar(sc2, label="Wave Period (s)", orientation='horizontal', pad=0.1, shrink=0.7)

        # Wave direction vectors
        plt.quiver(
            lon, lat, 
            Wave_x, Wave_y,
            color='blue',
            angles='xy', scale_units='xy', scale=75,
            width=0.002, headwidth=3, headlength=4, alpha=0.6
        )

        # Vehicle vectors
        plt.quiver(
            lon, lat,
            Vx, Vy,
            color='purple',
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

        # -------------------------------
        # 5️⃣ Add magnitudes as text
        # -------------------------------
        text_offset = 0.0008
        mag_vehicle = np.sqrt(Vx**2 + Vy**2)
        mag_current = np.sqrt(Cx**2 + Cy**2)

        scale_vehicle = 0.02   # much smaller
        scale_current = 0.008  # tiny arrows for current
        scale_wind = 0.005

        for i in range(len(lon)):
            plt.text(lon[i] + Vx[i]*scale_vehicle + text_offset,
                    lat[i] + Vy[i]*scale_vehicle + text_offset,
                    f"{mag_vehicle[i]:.2f}", color='purple', fontsize=7)
            
            plt.text(lon[i] + Cx[i]*scale_current + text_offset,
                    lat[i] + Cy[i]*scale_current + text_offset,
                    f"{mag_current[i]:.2f}", color='red', fontsize=7)
            
            plt.text(lon[i] + Wx[i]*scale_wind + text_offset,
                    lat[i] + Wy[i]*scale_wind + text_offset,
                    f"{WIND_VAL[i]:.2f}", color='green', fontsize=7)
            
        # -------------------------------
        # 6️⃣ Legend
        # -------------------------------
        legend_elements = [
            Line2D([0],[0], color='purple', lw=2, label='Vehicle Heading'),
            Line2D([0],[0], color='red', lw=2, label='Current'),
            Line2D([0],[0], color='green', lw=2, label='Wind Heading'),
            Line2D([0], [0], color='blue', lw=2, label="Wave Direction"),
            plt.scatter([],[], c='gray', s=40, edgecolors='k', linewidths=0.3, label='Wave Height', marker='o'),

            # Frequency as circle
            Line2D(
                [0], [0],
                marker='s',
                color='none',
                markerfacecolor='gray',
                markeredgecolor='black',
                markersize=7,
                label='Wave Frequency (Hz)'
            )        
    ]
        
                # Definir o timestamp específico (8 de agosto de 2025, 12:00 UTC)
        target_time = pd.Timestamp("2025-08-08 12:00:00", tz='UTC')

        # Encontrar o ponto mais próximo no teu DataFrame
        # Como estás a usar df_day (dados filtrados), vamos procurar o índice mais próximo
        if target_time in df_day.index:
            # Se existir exatamente
            ponto_estrela = df_day.loc[target_time]
            lon_estrela = ponto_estrela['LONGITUDE']
            lat_estrela = ponto_estrela['LATITUDE']
            print(f"✅ Encontrado ponto exato em {target_time}")
        else:
            # Se não existir, encontrar o mais próximo
            idx_proximo = df_day.index.get_indexer([target_time], method='nearest')[0]
            tempo_proximo = df_day.index[idx_proximo]
            ponto_estrela = df_day.iloc[idx_proximo]
            lon_estrela = ponto_estrela['LONGITUDE']
            lat_estrela = ponto_estrela['LATITUDE']
            print(f"⚠️  Ponto exato não encontrado. Usando o mais próximo: {tempo_proximo}")
            print(f"   Diferença: {tempo_proximo - target_time}")

        # Adicionar a estrela no plot (coloca isto ANTES do plt.show())
        plt.plot(lon_estrela, lat_estrela, 
                marker='*', 
                markersize=20, 
                color='yellow', 
                markeredgecolor='black', 
                markeredgewidth=1.5,
                zorder=10,
                linestyle='None',
                label='8 Agosto 12:00')

        # Opcional: adicionar texto junto à estrela
        plt.text(lon_estrela + 0.002, lat_estrela + 0.002, 
                '8 Ago 12:00', 
                fontsize=10, 
                weight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))

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

        