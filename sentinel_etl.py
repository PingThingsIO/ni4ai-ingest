# This script was used to import part of the Sentinel data set (provided to us by MITRE)...
#     * from the `pt-ni4ai-sentinel` bucket
#     * to the `chris/sentinel_test`-prefixed collections in the ni4ai btrdb deployment
# A full import of all Sentinel data was not conducted since the data set is very large
# (~55 TB of HDF5 files in S3) and not high priority right now.


import os
import uuid
import json
import glob
import time
import logging
import tempfile
import argparse

import tqdm
import h5py
import boto3
import btrdb
import numpy as np


DESCRIPTION = "ETL a Sentinel data export (formatted as an HDF5 file) into BTrDB"


# from "Sentinel HDF5 File Structure - Approved for Public Release. Distribution Unlimited. 16-4171.pdf"
# (note: latitude/lognitude are commented out below since these values can be instead derived
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


def keys(client, bucket_name, prefix='/', delimiter='/', start_after=''):
    """
    List the contents of a bucket, given the inputs specified.

    Adapted from: https://stackoverflow.com/a/54014862

    Parameters
    ----------
    client : botocore.client.S3
        S3 client object connected to a Boto3 session.
    bucket_name : str
        Name of the S3 bucket to search.
    prefix : str
        Bucket prefix, used for narrowing down the key search.
    delimiter : str
        Delimiter character between "directory" levels in the bucket.
    start_after : str
        Page to start after, with respect to the s3 paginator.

    Yields
    -------
    Unqiue keys in an S3 bucket.
    """
    s3_paginator = client.get_paginator('list_objects_v2')
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            yield content['Key']


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=DESCRIPTION, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('endpoints', type=str, help='BTrDB endpoints')
    parser.add_argument('apikey', type=str, help='BTrDB API key')
    parser.add_argument('collection_prefix', type=str, help='Prefix string used for the BTrDB collection. Full collection name will be this + the location name parsed from the HDF5 filename.')
    parser.add_argument('--hdf5-export-files', '-f', type=str, nargs='+', help='HDF5 file to be parsed & imported into BTrDB')
    parser.add_argument('--s3-object-prefix', '-s3', type=str, help='S3 bucket prefix at which HDF5 files can be found (all HDF5 files at this prefix will be ETL\'d).')
    parser.add_argument('--channels', '-s', type=int, help=f'Channels in the HDF5 file to be parsed & inserted as a stream. Choices: {", ".join([chan for chan in TAGS])}.', nargs='+', default=list(TAGS.keys()))
    parser.add_argument('--chunksize', '-c', type=int, help='Maximum number of points to parse & insert at once.', default=50000)
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress progress bars & info-level logging to stdout.')
    args = parser.parse_args()

    assert bool(args.s3_object_prefix) ^ bool(args.hdf5_export_files), 'Specify either an s3 object or hdf5 file, but not both.'

    LOGGING_FORMAT = '%(asctime)-15s | %(message)s'
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    logger = logging.getLogger()
                        
    db = btrdb.connect(args.endpoints, apikey=args.apikey)

    etl_start_time = time.time()
    if args.hdf5_export_files:
        source_file_names = args.hdf5_export_files
    else:  # get a file list from S3
        session = boto3.Session(profile_name='225685591965_PTDeveloperAccess')
        client = session.client("s3")
        s3 = session.resource('s3')
        bucket_name, obj_prefix = args.s3_object_prefix.split('/')[0], '/'.join(args.s3_object_prefix.split('/')[1:])

#         s3_prefix = 'pt-ni4ai-sentinel/2016-08-16/'
        bucket_name, obj_prefix = args.s3_object_prefix.split('/')[0], '/'.join(args.s3_object_prefix.split('/')[1:])
        source_bucket = s3.Bucket(bucket_name)
        source_file_names = [key for key in keys(client, bucket_name, prefix=obj_prefix)
                             if key.endswith('.h5')]

    for source_file_name in source_file_names:
        with tempfile.TemporaryFile() if args.s3_object_prefix else open(source_file, 'r') as source_file:
            if args.s3_object_prefix:
                source_bucket.download_fileobj(source_file_name, source_file)
                dataset = h5py.File(source_file, 'r')
            else:
                dataset = h5py.File(source_file_name, 'r')

            annotations = {
                'latitude': str(np.average(dataset['alldata']['latitude']['raw'])),
                'longitude': str(np.average(dataset['alldata']['longitude']['raw'])),
            }
            for channel, dataset in dataset['alldata'].items():

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

                location = os.path.basename(source_file_name).split('_')[0]
                collection = '/'.join([args.collection_prefix.strip('/'), location])
                stream_name = TAGS[channel]['name']
                        
                try:
                    stream, = db.streams(f'{collection}/{stream_name}')
                except btrdb.exceptions.NotFound:
                    stream = db.create(
                        uuid.uuid4(),
                        collection,
                        tags=TAGS[channel],
                        annotations=dict(**annotations, **LOCATION_TO_ANNOTATIONS[location]),
                    )

                if not args.quiet:
                    print(f'Inserting into: {collection}/{stream_name}')

                for chunk_idx, chunk_start in enumerate(tqdm.tqdm(np.arange(0, n_obs, args.chunksize), desc=channel, disable=args.quiet)):

                    start_time = stream_start + chunk_start*step
                    stop_time = start_time + step*args.chunksize

                    # is this method of inferring start time & stop time precise enough?
                    # check `times` and `values`.
                    # this could be memory-optimized so that `times`, `values`, and `data` are not
                    # materialized separately (potentially at increased CPU cost)
                    times = np.linspace(start_time, stop_time, args.chunksize, endpoint=False, dtype=np.int64)
                    values = scale * dataset['raw'][chunk_idx:chunk_idx+args.chunksize] + offset
                    data = [(time, value) for time, value in zip(times, values)]
                    stream.insert(data, merge='equal')

        if not args.quiet:
            logger.info(f'completed ETL for {source_file_name} in {time.time()-etl_start_time:.4f} seconds')
