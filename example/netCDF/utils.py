import numpy as np
import os
import gzip
import shutil

# Usefull for applying offset from meters to degrees
c_wgs84_a = 6378137.0
c_wgs84_b = 6356752.3142
c_wgs84_e2 = 0.00669437999013
c_wgs84_ep2 = 0.00673949674228
c_wgs84_f = 0.0033528106647475

def computeRN(lat):

    lat_sin = np.sin(lat)
    return (c_wgs84_a / np.sqrt(1 - c_wgs84_e2 * (lat_sin**2)))

def toECEF(lat, lon, depth):

    cos_lat = np.cos(lat)
    sin_lat = np.sin(lat)
    cos_lon = np.cos(lon)
    sin_lon = np.sin(lon)
    
    # Compute the radious of earth at this given latitude
    rn = computeRN(lat)

    x = (rn - depth) * cos_lat * cos_lon
    y = (rn - depth) * cos_lat * sin_lon
    z = ( ( (1.0 - c_wgs84_e2) * rn) - depth) * sin_lat
    return x,y,z   

def n_rad(lat):

    lat_sin = np.sin(lat)
    return c_wgs84_a / np.sqrt(1 - c_wgs84_e2*(lat_sin**2))

# comment this once given the chance
def fromECEF(x, y, z):

    p = np.sqrt(x**2 + y**2) 
    lon = np.arctan2(y, x)
    lat = np.arctan2(z / p, 0.01)

    n = n_rad(lat)
    depth = p / np.cos(lat) - n

    old_depth = -1e-9
    num = z / p

    while(np.abs(depth - old_depth) > 1e-4):

        old_depth = depth

        den =  1 - c_wgs84_e2 * n / (n + depth)
        lat = np.arctan2(num, den)
        n = n_rad(lat)
        depth = p / np.cos(lat) - n

    return lat, lon, depth 

def WGS84displacement(latDegrees1, lonDegrees1, depth1, latDegrees2, lonDegrees2, depth2):
    
    cs1 = []
    cs2 = []

    # Get the coordinates to ECEF format
    cs1.extend(toECEF(latDegrees1, lonDegrees1, depth1))
    cs2.extend(toECEF(latDegrees2, lonDegrees2, depth2))

    # Calculate the displacement between the two points 
    ox = cs2[0] - cs1[0]
    oy = cs2[1] - cs1[1]
    oz = cs2[2] - cs1[2]

    slat = np.sin(latDegrees1)
    clat = np.cos(latDegrees1)
    slon = np.sin(lonDegrees1)
    clon = np.cos(lonDegrees1)

    ret = []

    ret.append(-slat * clon * ox - slat * slon * oy + clat * oz)
    ret.append(-slon * ox + clon * oy)
    ret.append(depth1 - depth2)

    return ret 


## Based on path parameter it will search for all possible .Data.lsf.gz files
def gather_log_paths(log_path):

    lsf_files = []
    
    try:
        print("## Gathering all paths to Data files ##")
        for root, dirs, files in os.walk(log_path):

            for file in files: 
                if file.endswith('Data.lsf.gz'):

                    full_path = os.path.join(root)
                    lsf_files.append(full_path)
                

    except OSError:

        print("Error While Looking for .Data.lsf.gz. {}".format(log_path))
    
    print("List of compressed files gathered: \n {}".format(lsf_files))

    return lsf_files
## Export all log files 
def export_logs(all_logs):
   
    try:
        print("## Exporting all log files ## \n")

        for f in all_logs:
        
                # Open the compressed 
                comp_log = f + '/' + 'Data.lsf.gz'
                uncomp_log =  f + '/' + 'Data.lsf'

                # Check if given log was already decompressed 
                if os.path.isfile(uncomp_log):

                    print("File {} was already decompressed".format(f))
                    continue

                with gzip.open(comp_log, 'rb') as f_in:

                    # Decompress it
                    with open(uncomp_log, 'wb') as f_out:

                        shutil.copyfileobj(f_in, f_out)

    except OSError as e:
        
        print("Not able to read file: {} \n Error: {}".format(e, f))

        if os.path.isfile(uncomp_log):

            os.remove(uncomp_log)
## Concantenate all those given logs
def concatenate_logs(all_logs):

    print("### Concatenating all Logs ###")
    conc_log = os.curdir + '/' + 'Data.lsf'

    ## Concantenating log
    with open(conc_log, 'wb') as f_out:

        for f in all_logs: 
            data_file = f + '/' + 'Data.lsf'

            with open(data_file, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)