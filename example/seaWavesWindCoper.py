from copernicusmarine import subset

dataset_id = "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
variables = [
    "VHM0",           # Significant wave height
    "VTPK",           # Wave period at spectral peak
    "VMDR",           # Mean wave direction
    "VTP02",          # Tm02 (spectral moments 0,2)
    "VTM01",          # Tm-10 (spectral moments -1,0)
    "VSDX",           # Stokes drift U
    "VSDY",           # Stokes drift V
    "VHM0_WW",        # Spectral significant wind wave height
    "VTM01_WW",       # Spectral moments (0,1) wind wave period
    "VMDR_WW",        # Mean wind wave direction
    "VHM0_SW1",       # Primary swell height
    "VTM01_SW1",      # Primary swell period
    "VMDR_SW1",       # Primary swell direction
    "VHM0_SW2",       # Secondary swell height
    "VTM01_SW2",      # Secondary swell period
    "VMDR_SW2"        # Secondary swell direction
]

subset(
    dataset_id=dataset_id,
    variables=variables,
    minimum_latitude=38.0, 
    maximum_latitude=43.0,
    minimum_longitude=-20.0,
    maximum_longitude=-8.0,
    start_datetime="2025-06-30T00:00:00",
    end_datetime="2025-07-10T23:00:00",
    output_filename="waves_subset.nc"
)
