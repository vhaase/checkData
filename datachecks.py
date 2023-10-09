import json
import sys
from abc import *

import polars as pl


class checksInterface(ABC):
    
    @abstractmethod
    def replace(self, df, colsDict):
        pass
    
    @abstractmethod
    def load_colsDict(self, path):
        pass


class Checks(checksInterface):
    """
    This script will do some character substitutions at the data before processing the data master update.
    Only doing those checks, where the data frame needs to be modified. The
    pure alerting checks will be made by great expectations.
    
    process: load the data table,load the dict with the details regarding the columns, do the substitutions,
    save the modified data table
    """
    
    # TODO: a log for this process
    
    def replace(self, df: pl.DataFrame, colsDict: dict) -> pl.DataFrame:
        """
        this is a very simple function to fix some typical details by simply
        replacing some characters
        :param df: the data frame to process
        :param colsDict:
        :return:
        """
        
        self.df = df
        self.colsDict = colsDict
        
        # walk through the keys
        for k in self.colsDict.keys():
            # walk through the values
            for v in range(0, len(self.colsDict[k])):
                self.df = self.df.with_columns(
                    pl.col(k).str.replace_all(self.colsDict[k][v][0], self.colsDict[k][v][1]))
        # FIXME: why does this not work?
        self.df = self.df.fill_null(0)
        return self.df
    
    def load_colsDict(self, path: str) -> dict:
        """
        this method is loading the dictionary with the details
        about the columns of the file to process. When we do not find a suitable file,
        the script will stop
        :type path: str
        :param path:
        :return: the dictionary
        """
        self.path = path
        colsDict = {}
        try:
            file = open(self.path, 'r')
            colsDict = json.load(file)
        except FileNotFoundError:
            # without the details processing is useless
            print(f"File '{self.path}' not found.")
            sys.exit(1)
        except Exception as e:
            # without the details processing is useless
            print(f"An error occurred: {e}")
            sys.exit(1)
        return colsDict


