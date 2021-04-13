##########################################################################
## Imports
##########################################################################

import abc

##########################################################################
## Stream & File objects
##########################################################################

class Stream(object):
    def __init__(self, collection, times, values, tags, annotations, count):
        self.collection = collection
        self.times = times
        self.values = values
        self.tags = tags
        self.annotations = annotations
        self.count = count

# In the future I anticipate File to include things such as is_S3, s3_bucket, or anything else
# you would need to locate files or parse data in them.
class File(object):
    def __init__(self, path):
        self.path = path

##########################################################################
## DataParser Interface
##########################################################################

# NOTE: This doesn't need to be an actual abc. I thought it might be nice
# if it errors if a user tries to subclass without including these methods
class DataParser(metaclass=abc.ABCMeta):
    
    @abc.abstractmethod
    def collect_files(self):
        """
        This method can take in any parameters it needs to and is meant to return 
        a list of Files

        Returns
        -------
        list of File objects
        """
        pass
    
    @abc.abstractmethod
    def create_streams(self, files):
        """
        This method parses Files and should return or yield lists of Streams

        Parameters
        ----------
        files: list of File objects

        Returns
        -------
        streams: list of Stream objects
        """
        pass