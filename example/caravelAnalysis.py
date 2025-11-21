import pandas as pd
import argparse
import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt


if __name__ == '__main__':

    # Parser for command line
    parser = argparse.ArgumentParser(description="Process arguments for Caravel Data Analysis")

    # Path to the mission argument
    parser.add_argument('-sp', '--sea_path', type=str, default=os.getcwd(),
                        help="Absolute Path to netcdf file with sea data")
    
    parser.add_argument('-cp', '--caravel_path', type=str, default=os.getcwd(),
                        help="Absolute Path to excel file with caravel data")
    
    args = parser.parse_args()
    seaPath = args.sea_path
    caravelPath = args.caravel_path

    # Open Data sets and turn them into pandas dataframes 
    # Variables to look up on Sea Dataset 
    # Wave Signficant Height, Spectral Density Maximum, Sea Surface Wave From direction, "current" speed x,y  
    waveVarNames = ['VHM0', 'VTPK', 'VMDR_WW', 'VSDX', 'VSDY'] 

    ## First the Sea Da
    seaData = xr.open_dataset(seaPath)
    print(seaData)

    """
    seaDataDf = seaData.to_dataframe().reset_index()
    seaDataDf = seaDataDf.set_index('TIME').sort_index()
    """

    ## Second Caravel's Data
    caravelDataDf = pd.read_excel(caravelPath)
    caravelDataDf['TIME'] = pd.to_datetime(caravelDataDf['TIME'], unit='s')
    caravelDataDf = caravelDataDf.set_index('TIME').sort_index()
    caravelDataDf = caravelDataDf[caravelDataDf['THRUSTER'] == 0]
    caravelDataDf = caravelDataDf.drop(columns='THRUSTER')

    ## Added the columns coming from sea data
    for var in waveVarNames: 
        caravelDataDf[var]  = np.nan
    
    # Slice in half
    middleIndex = len(caravelDataDf) // 2
    caravelDataDfSlice1 = caravelDataDf.iloc[:middleIndex]
    caravelDataDfSlice2 = caravelDataDf.iloc[middleIndex:]
    halfTime = caravelDataDf.index[middleIndex]

    print("Half to the mission is concluded at time: {}".format(halfTime))

    ## Slice sea data so it doesn't get too confusing
    minTimeDf1 = caravelDataDfSlice1.index.min()
    maxTimeDf1 = caravelDataDfSlice2.index.max()

    seaDataSub = seaData.sel(time=slice(minTimeDf1, maxTimeDf1))
    seaDataSub = seaDataSub.sel(latitude=slice(caravelDataDfSlice1['LATITUDE'].min(), caravelDataDfSlice1['LATITUDE'].max()))
    seaDataSub = seaDataSub.sel(longitude=slice(caravelDataDfSlice1['LONGITUDE'].min(), caravelDataDfSlice1['LONGITUDE'].max()))

    ## Important to note here that the frequency of my temporal data regarding the vehicle 
    ## Is much higher than the sea state data 
    ## For that reason, data from the sea state will be interpolated over the data from the vehicle
    seaDataInterpolated = seaDataSub.sel(
        time=caravelDataDfSlice1.index,
        latitude=xr.DataArray(caravelDataDfSlice1['LATITUDE'], dims=['time']),
        longitude=xr.DataArray(caravelDataDfSlice1['LONGITUDE'],dims=['time']),
        method='nearest')
    
    ## Fill dataset

    for var in waveVarNames:
        caravelDataDfSlice1[var] = seaDataInterpolated[var].values

    print(caravelDataDfSlice1)
    caravelDataDfSlice1.to_csv("caravel_with_sea.csv")

    caravelDataDfSlice1['V_HORIZONTAL'] = np.sqrt(
        caravelDataDfSlice1['VX']**2 + caravelDataDfSlice1['VY']**2
    )

    plt.figure(figsize=(10,5))
    #plt.plot(caravelDataDfSlice1.index, caravelDataDfSlice1['VX'], label='Vehicle speed on X Axis (m/s)')
    #plt.plot(caravelDataDfSlice1.index, caravelDataDfSlice1['VY'], label='Vehicle speed on Y Axis (m/s)')
    plt.plot(caravelDataDfSlice1.index, caravelDataDfSlice1['V_HORIZONTAL'], label='Vehicle speed on horizontal axis')
    plt.plot(caravelDataDfSlice1.index, caravelDataDfSlice1['VHM0'], label='Wave height (m)')
    plt.legend()
    plt.show()



    

    



    
















