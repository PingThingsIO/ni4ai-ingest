import h5py
import os
import pandas as pd
import numpy as np
import uuid
import json

from datetime import datetime, timedelta
from matplotlib import pyplot as plt

import btrdb
from btrdb.utils.timez import *

path_to_data = os.path.join('../../../../../Volumes/NO NAME/sentinel')

streams = ['ac_voltage','freq','sync_status']

fnames = ['bedford_2013-03-01_%s.h5'%(str(hr).zfill(2)) for hr in range(0,24)]
for fname in fnames:
    assert fname in os.listdir(path_to_data)
print('\n'.join(fnames))

laurels_api_key = '2301C47D67FB1C2C48D0CC7B'
db = btrdb.connect("api.ni4ai.org:4411", apikey=laurels_api_key)

f = h5py.File(os.path.join(path_to_data, fnames[0]), 'r')

collection = 'lndunn/sentinel/bedford'
annotations = {'latitude': np.average(f['alldata']['latitude']['raw']),
              'longitude': np.average(f['alldata']['longitude']['raw'])}

tags = {'ac_voltage': {'name': 'voltage', 'unit': 'volts'},
        'freq': {'name': 'frequency', 'unit': 'Hz'},
        'neutral_current': {'name': 'neutral_current', 'unit': 'amps'},
        'sync_status': {'name': 'sync_stats', 'unit': 'mask'}
        }

uuids = dict(zip(tags.keys(), [uuid.uuid4() for key in tags.keys()]))



def add_streams(fname):
    f = h5py.File(os.path.join(path_to_data, fname), 'r')
    print('ingesting', f)
    for s in streams:
        if not f['alldata'][s]['nogaps'][0]:
            # there's a flag for each stream in each 1-hr file indicating whether there are gaps
            # none of the data received so far has gaps
            # once we get more data, we'll need to figure out how they indicate where the gaps are
            print('data has gaps, skip for now')
            continue
            
        stream = db.stream_from_uuid(uuids[s])
        
        dt     = f['alldata'][s]['dt'][0]
        start  = f['alldata'][s]['tstart'][0]
        n_obs  = len(f['alldata'][s]['raw'])

        scale  = f['alldata'][s]['scale'][0]
        offset = f['alldata'][s]['offset'][0]
        
        time = (1e9*(dt*np.arange(0, n_obs) + start)).astype(int)
        meas = scale * pd.Series(f['alldata'][s]['raw']) + offset

        tnow = datetime.now()
        stream.insert(list(zip(time.tolist(), meas.tolist())))
        print('\t', 'finished ingesting %s'%(s))
        print('\t', str(datetime.now()-tnow).split('.')[0])


def handler():    
    p = Pool(4)
    p.map(add_streams, fnames[1:])

if __name__ == "__main__":

    add_streams(fnames[0])