import xarray as xr
import pandas as pd
import argparse
import os


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

    """
    seaDataDf = seaData.to_dataframe().reset_index()
    seaDataDf = seaDataDf.set_index('TIME').sort_index()
    """

    ## Second Caravel's Data
    caravelDataDf = pd.read_excel(caravelPath)
    caravelDataDf['TIME'] = pd.to_datetime(caravelDataDf['TIME'])
    caravelDataDf = caravelDataDf.set_index('TIME').sort_index()
    caravelDataDf = caravelDataDf[caravelDataDf['THRUSTER'] == 0]
    caravelDataDf = caravelDataDf.drop('THRUSTER')

    for var in waveVarNames: 
        caravelDataDf[var] 


    ## Important to note here that the frequency of my temporal data regarding the vehicle 
    ## Is much higher than the sea state data 
    ## For that reason, data from the sea state will be interpolated over the data from the vehicle
    seaDataInterpolated = seaData.interp(
        time=caravelDataDf.index,
        latitude=caravelDataDf['LATITUDE'],
        longitude=caravelDataDf['LONGITUDE'])

    
















