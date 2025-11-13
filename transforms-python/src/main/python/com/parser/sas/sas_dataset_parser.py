"""
SAS Dataset Parser Interface
Matches the Java SASDatasetParser interface
"""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class SASDatasetParser(ABC):
    """
    Interface for parsing SAS datasets.
    Takes a DataFrame of file information and returns a DataFrame of parsed rows.
    """
    
    @abstractmethod
    def parse(self, files_df: DataFrame) -> DataFrame:
        """
        Parse SAS files from a DataFrame of file information.
        
        Args:
            files_df: DataFrame containing file information (must have 'logical_path' and 'content' columns)
            
        Returns:
            DataFrame with parsed SAS data in skinny format
        """
        pass

