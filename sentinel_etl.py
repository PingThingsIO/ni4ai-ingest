import uuid
import json
import glob
import time
import logging
import argparse

import tqdm
import h5py
import btrdb
import numpy as np


DESCRIPTION = "ETL a Sentinel data export (formatted as an HDF5 file) into BTrDB"


# from "Sentinel HDF5 File Structure - Approved for Public Release. Distribution Unlimited. 16-4171.pdf"
# (note: latitude/lognitude are commented out below since these values are instead derived
# from the HDF5 file during ETL.
LOCATION_TO_ANNOTATIONS = {
    'bedford': {
#         'latitude': "42.5057828",
#         'longitude': "-71.2374495",
        'physical_location': 'MITRE Chiller Plant',
        'power_company': 'eversource_energy',
        'measured_channels': "voltage, current",
    },
    'charlottesville_va': {
#         'latitude': "38.1490886",
#         'longitude': "-78.434372",
        'physical_location': 'MITRE office first level near window',
        'power_company': 'dominion_power',
        'measured_channels': "voltage",
    },
    'mclean': {
#         'latitude': "38.9232458",
#         'longitude': "-77.2049283",
        'physical_location': 'MITRE office first level near window',
        'power_company': 'dominion_power',
        'measured_channels': "voltage, current",
    },
    'mitre_1': {
#         'latitude': "38.920985",
#         'longitude': "-77.205105",
        'physical_location': 'MITRE building third floor near window; GPS attached to the metal bookshelf facing window',
        'power_company': 'dominion_power',
        'measured_channels': "voltage",
    },
    'quantico': {
#         'latitude': "38.5144488",
#         'longitude': "-77.3712153",
        'physical_location': 'MITRE Office third floor near window',
        'power_company': 'dominion_power',
        'measured_channels': "voltage",
    },
    'rome_ny': {
#         'latitude': "43.220680",
#         'longitude': "-75.411289",
        'physical_location': 'MITRE Office first floor, GPS at window',
        'power_company': 'national_grid',
        'measured_channels': "voltage",
    },
    'st_louis': {
#         'latitude': "38.5472058",
#         'longitude': "-89.8815511",
        'physical_location': 'MITRE Office first floor near window',
        'power_company': 'ameren',
        'measured_channels': "voltage, current",
    },
    'tampa': {
#         'latitude': "27.9442173",
#         'longitude': "-82.524636",
        'physical_location': 'MITRE office seventh floor near window',
        'power_company': 'tampa_electric_co',
        'measured_channels': "voltage, current",
    },
}

TAGS = {
    'ac_voltage': {
        'name': 'voltage',
        'unit': 'volts',
    },
    'freq': {
        'name': 'frequency',
        'unit': 'Hz',
    },
    'neutral_current': {
        'name': 'neutral_current',
        'unit': 'amps',
    },
    'sync_status': {
        'name': 'sync_stats',
        'unit': 'mask',
    }
}


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=DESCRIPTION, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('endpoints', type=str, help='BTrDB endpoints')
    parser.add_argument('apikey', type=str, help='BTrDB API key')
    parser.add_argument('collection_prefix', type=str, help='Prefix string used for the BTrDB collection. Full collection name will this + the location name parsed from the HDF5 filename.')
    parser.add_argument('hdf5_export_file', type=str, help='HDF5 file to be parsed & imported into BTrDB')
    parser.add_argument('--channels', '-s', type=int, help=f'Channels in the HDF5 file to be parsed & inserted as a stream. Choices: {", ".join([chan for chan in TAGS])}.', nargs='+', default=list(TAGS.keys()))
    parser.add_argument('--chunksize', '-c', type=int, help='Maximum number of points to parse & insert at once.', default=50000)
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress progress bars & info-level logging to stdout.')
    args = parser.parse_args()

    LOGGING_FORMAT = '%(asctime)-15s | %(message)s'
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    logger = logging.getLogger()
#     logging.setLevel(logging.INFO)
                        
    db = btrdb.connect(args.endpoints, apikey=args.apikey)

    location = args.hdf5_export_file.split('/')[-1].split('_')[0]
    collection = f'{args.collection_prefix}{location}'
                        
    streams = {stream.name: stream for stream in db.streams_in_collection(collection)}

    etl_start_time = time.time()
    f = h5py.File(args.hdf5_export_file, 'r')
    annotations = {
        'latitude': str(np.average(f['alldata']['latitude']['raw'])),
        'longitude': str(np.average(f['alldata']['longitude']['raw'])),
    }
    for channel, dataset in f['alldata'].items():

        if channel not in TAGS:
            continue

        if dataset['nogaps'][0] == 0:
            logger.warning(f'Channel metadata for "{channel}" indicates gaps are present, handling not yet implemented (skipping)')
            continue

        step = dataset['dt'][0]*1e9
        stream_start = dataset['tstart'][0]*1e9
        n_obs = len(dataset['raw'])

        scale = dataset['scale'][0]
        offset = dataset['offset'][0]

        stream_name = TAGS[channel]['name']
        if stream_name not in streams:
            location = collection.split('/')[-1]
            streams[stream_name] = db.create(
                uuid.uuid4(),
                collection,
                tags=TAGS[channel],
                annotations=dict(**annotations, **LOCATION_TO_ANNOTATIONS[location]),
            )

        for chunk_idx, chunk_start in enumerate(tqdm.tqdm(np.arange(0, n_obs, args.chunksize), desc=channel, disable=args.quiet)):
                        
            if chunk_idx > 5:
                break

            start_time = stream_start + chunk_start*step
            stop_time = start_time + step*args.chunksize

            # is this precise enough? check `times` and `values`.
            # this could be memory-optimized so that `times`, `values`, and `data` are not
            # materialized separately (potentially at increased CPU cost)
            times = np.linspace(start_time, stop_time, args.chunksize, endpoint=False, dtype=np.int64)
            values = scale * dataset['raw'][chunk_idx:chunk_idx+args.chunksize] + offset
            data = [(time, value) for time, value in zip(times, values)]
            streams[stream_name].insert(data)

    if not args.quiet:
        logger.info(f'completed ETL for {args.hdf5_export_file} in {time.time()-etl_start_time:.4f} seconds')
