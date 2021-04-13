
##########################################################################
## Imports
##########################################################################

import uuid
import warnings
from tqdm import tqdm
from btrdb.utils.timez import to_nanoseconds

from parse import Stream

##########################################################################
## Module Variables and Constants
##########################################################################

INSERT_CHUNK_SIZE = 50000
MERGE_POLICIES = ["never", "retain", "replace", "equal"]

##########################################################################
## DataIngestor
##########################################################################

class DataIngestor(object):

    def __init__(self, conn, merge_policy="never", total_points=None):
        """
        Parameters
        ----------
        conn: btrdb.Connection
        merge_policy: str
            merge policy to use when inserting BTrDB points
        total_points: int
            specifies total number of points to be inserted. Used to create a progess bar.
        """
        # NOTE: Using a BTrDB connection here will make it tricky to use this class with Ray or 
        # multiprocessing until we make updates to the bindings
        self.conn = conn

        if total_points is None:
            warnings.warn("total points not provided. Progress bar will not be displayed")
            self.pbar = None
        else:
            self.pbar = tqdm(total=total_points)
        
        if merge_policy in MERGE_POLICIES:
            self.merge_policy = merge_policy
        else:
            raise Exception(f"'{merge_policy}' is not a valid merge policy. Options are: {','.join(MERGE_POLICIES)}")
    
    @staticmethod
    def _chunk_points(times, values, chunk_size):
        """
        Parameters
        ----------
        times: pd.Series of timestamps, which can be datetime, datetime64, float, str (RFC 2822)
        values: pd.Series of float values
        chunk_size: int
            specifies number of (time, value) pairs to insert at a time
        """
        points = [(to_nanoseconds(t), v) for t, v in zip(times, values)]
        for i in range(0, len(points), chunk_size):
            yield points[i:i + chunk_size]
    
    # NOTE: Ideally this function would listen to a queue and would pick up Stream
    # objects from the DataParser and insert as they are produced
    def ingest(self, streams, chunk_size=None):
        """
        Parameters
        ----------
        streams: list of Streams
        chunk_size: int
            specifies number of (time, value) pairs to insert at a time
        """
        for s in streams:
            if not isinstance(s, Stream):
                raise TypeError(f"Stream object expected. Received {type(s)}")

            # check if stream exists already, create it if it doesn't
            query = f"select uuid from streams where collection = '{s.collection}' and name = '{s.tags['name']}'"
            uuids = self.conn.query(query)

            num_streams = len(uuids)
            if num_streams > 1:
                raise Exception(f"{num_streams} streams found in collection {s.collection} named {s.tags['name']}. There should only be 1")
            elif num_streams == 0:
                stream = self.conn.create(uuid.uuid4(), s.collection, s.tags, s.annotations)
            else:
                stream = self.conn.stream_from_uuid(uuids[0]["uuid"])
            
            # convert time and value arrays into list of tuples and split into chunks for insertion
            chunk_size = chunk_size or INSERT_CHUNK_SIZE
            for points in self._chunk_points(s.times, s.values, chunk_size):
                stream.insert(points, self.merge_policy)
                if self.pbar:
                    self.pbar.update(len(points))