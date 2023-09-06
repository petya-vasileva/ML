import pandas as pd
import numpy as np
import os
import re
import utils.helpers as hp
from utils.parquet import Parquet

import urllib3
urllib3.disable_warnings()


def removeInvalid(tracedf):
    """ Removes the invalid traceroutes where IPv6 tests recorded IPv4 paths
    
      Args:
        Traceroute dataframe
    
      Returns:
        Clean dataframe
    """
    
    pattern = r'^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$'
    tracedf['for_removal'] = 0
    i, j = 0, 0
    for idx, route, pair, hops, ipv, rm in tracedf[tracedf['ipv6']==True][['route', 'pair', 'hops', 'ipv6', 'for_removal']].itertuples():
        isIPv4 = None
        if len(hops) > 2:
            if ipv == True: 
                isIPv4 = re.search(pattern, hops[-1])

                if isIPv4:
                    # print(isIPv4, ipv)
                    tracedf.iat[idx, tracedf.columns.get_loc('for_removal')] = 1
                    i+=1
        else:
            tracedf.iat[idx, tracedf.columns.get_loc('for_removal')] = 1


    nullsite = len(tracedf[(tracedf['src_site'].isnull()) | (tracedf['dest_site'].isnull())])

    print(f'{round(((i+j+nullsite)/len(tracedf))*100,0)}% invalid entries removed.')

    tracedf = tracedf[~((tracedf['src_site'].isnull()) | (tracedf['dest_site'].isnull())) | ~(tracedf['route'].isnull())]
    tracedf = tracedf[tracedf['for_removal']==0]
    
    return tracedf.drop(columns=['for_removal'])



 
def loadTraceroutes(location):
    """ Loads any dataframe if saved in a file

      Args:
        Dataframe type
    
      Returns:
        Dataframe
    """
    pq = Parquet()
    dd = pq.readSequenceOfFiles(location, 'ps_trace')
    dd = dd.reset_index(drop=True)

    dd['pair'] = dd['src']+'-'+dd['dest']
    dd['site_pair'] = dd['src_site']+' -> '+dd['dest_site']
    dd.rename(columns={'route-sha1':'route'}, inplace=True)
    dd['idx'] = dd.index
    
    trace = removeInvalid(dd)
    print('Number of tests:', len(trace))
        
    display(trace.head(2))
    return trace


def getDataframes(location, subset_type = 'ipv4'):
    """ Loads the neccesary datasets by either reading from local files, 
        or creating the files first

      Args:
        trace - the complete dataset
        location of the parquet files
        subset_type - type of protocol
    
      Returns:
        ipvdf - the dataframe for the respected protocol
        subset - the data without duplicates for the combination ttls-hops,
                 i.e. a clean set of sequences
    """
    pq = Parquet()
    
    ipv_loc, cleanpaths_loc = f'{location}trace_{subset_type}', f'{location}/trace_{subset_type}_clean_paths'

    # load the data if the files exist 
    if os.path.exists(ipv_loc) and os.path.exists(cleanpaths_loc):
        print(f'{ipv_loc} and {cleanpaths_loc} exist.')
        ipvdf = pq.readSingleFile(ipv_loc)
        subsetdf = pq.readSingleFile(cleanpaths_loc)
        print(f'Number of {subset_type} tests: {len(ipvdf)}')

    # otherwise create the parquet files
    else:
        # Assuming the traceroute data is available in parquet files locally.
        # TODO: Add the code that queries traceroutes from ES
        trace = loadTraceroutes(location)
        # Check the percentage incomplete paths
        display(trace['path_complete'].value_counts(normalize=True).round(2))
        
        print("Creating...")
        ipv6 = False if subset_type=='ipv4' else True 
        ipvdf = trace[trace['ipv6']==ipv6]
        pq.writeToFile(ipvdf, ipv_loc)
        
        # turn the array-type columns into strings so that we drop the duplicates
        ipvdf.loc[:, 'hops_str'] = ipvdf['hops'].astype(str)
        ipvdf.loc[:, 'ttls_str'] = ipvdf['ttls'].astype(str)
        ipv4df['dt'] = pd.to_datetime(ipv4df['timestamp'], unit='ms')
        
        subsetdf = ipvdf[['idx', 'route', 'hops', 'hops_str', 'ttls', 'ttls_str']].drop_duplicates(subset=['hops_str','ttls_str'], keep='first')
        pq.writeToFile(subsetdf, cleanpaths_loc)

    return ipvdf, subsetdf


# ipvdf contains all measurements for the respective protocol
# subset contains the only the ttls+hops without duplicates
# ipv6df, subset = getDataframes(location, 'ipv6')
# ipv4df, subset = getDataframes(location, 'ipv4')