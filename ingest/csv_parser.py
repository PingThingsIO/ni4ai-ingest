##########################################################################
## Imports
##########################################################################

import re
import csv
import pandas as pd
from pathlib import Path

from parse import DataParser, Stream, File

##########################################################################
## Module Variables and Constants
##########################################################################

# NOTE: Matching metadata to streams is, in my experience, one of the most 
# challenging parts of any ingestion. It would be nice to be able to offer 
# users the option to provide metadata schemas via yaml or json files, but 
# we would need to document how this could/should be done
METADATA = {
    "Freq": {"tags": {"name": "Freq", "unit": "Hz"},
            "annotations": {}},
    "PhA_Vmag": {"tags": {"name": "PhA_Vmag", "unit": "volts"},
                "annotations": {"phase": "A"}},
    "PhA_Vang": {"tags": {"name": "PhA_Vang", "unit": "degrees"},
                "annotations": {"phase": "A"}},
    "PhB_Vmag": {"tags": {"name": "PhB_Vmag", "unit": "volts"},
                "annotations": {"phase": "B"}},
    "PhB_Vang": {"tags": {"name": "PhB_Vang", "unit": "degrees"},
                "annotations": {"phase": "B"}},
    "PhC_Vmag": {"tags": {"name": "PhC_Vmag", "unit": "volts"},
                "annotations": {"phase": "C"}},
    "PhC_Vang": {"tags": {"name": "PhC_Vang", "unit": "degrees"},
                "annotations": {"phase": "C"}},
    "PhA_Cmag": {"tags": {"name": "PhA_Cmag", "unit": "amps"},
            "annotations": {"phase": "A"}},
    "PhA_Cang": {"tags": {"name": "PhA_Cang", "unit": "degrees"},
                "annotations": {"phase": "A"}},
    "PhB_Cmag": {"tags": {"name": "PhB_Cmag", "unit": "amps"},
                "annotations": {"phase": "B"}},
    "PhB_Cang": {"tags": {"name": "PhB_Cang", "unit": "degrees"},
                "annotations": {"phase": "B"}},
    "PhC_Cmag": {"tags": {"name": "PhC_Cmag", "unit": "amps"},
                "annotations": {"phase": "C"}},
    "PhC_Cang": {"tags": {"name": "PhC_Cang", "unit": "degrees"},
                "annotations": {"phase": "C"}},
    "P": {"tags": {"name": "P", "unit": "MW"},
            "annotations": {}},
    "Q": {"tags": {"name": "Q", "unit": "MVAR"},
            "annotations": {}}
}

##########################################################################
## Helper Functions
##########################################################################

def retrieve_meta(name, key="tags", meta=METADATA):
    try:
        return meta[name][key]
    except KeyError:
        raise Exception(f"could not find {key} for {name}")

##########################################################################
## CSV Parser
##########################################################################

class CSVFile(File):
    def __init__(self, path, count=True, header=False):
        # Start with File properties
        super().__init__(path)
        if count:
            header_rows = 1 if header else 0
            with open(path, "r") as f:
                reader = csv.reader(f)
                row = next(reader)
                # estimate point count = # of rows (minus header) * # of columns (minus timestamps)
                self.count = (len(list(reader)) - header_rows) * (len(row) -1) 

class CSVParser(DataParser):
    def __init__(self, fpath=None, collection_prefix=None):
        """
        Parameters
        ----------
        path: str 
            path to directory containing data
        prefix: str
            prefix to add to all streams' collection names
        """
        self.path = fpath
        self.prefix = collection_prefix
    
    def collect_files(self):
        """
        Recursively searches provided directory and returns a CSVFile object for each csv

        Returns
        -------
        fobjs: list
            list of CSVFile objects referring to csv files that contain data to ingest
        """
        return [CSVFile(str(fpath), header=True) for fpath in Path(self.path).rglob(f"*.csv")]
    
    def _parse_collection(self, fname):
        """
        Parameters
        ----------
        fname: str
            name of file
        
        Returns
        -------
        str: collection name
        """
        match = re.search("PMU[\d]*", str(fname))
        if match:
            return "/".join([self.prefix, match.group(0)]) if self.prefix else match.group(0)
        else:
            raise Exception(f"could not determine collection to use for file {fname}")
    
    def create_streams(self, files):
        """
        Parameters
        ----------
        files: list
            list of CSVFile objects
        
        Yields
        ------
        streams: list
            list of Stream objects, yielded as chunks per file
        """
        for file in files:
            if not isinstance(file, File):
                raise TypeError(f"Expected File inputs. Received {type(file)}")

            streams = []
            df = pd.read_csv(file.path)
            
            # relies on timestamps being in the first column
            # probs should add some checking to verify
            times = df.iloc[:,0]
            collection = self._parse_collection(file.path)

            streams.extend([
                Stream(collection, times, df[col], retrieve_meta(col), retrieve_meta(col, key="annotations"), len(df))
                for col in df.columns[1:]
            ])
            yield streams