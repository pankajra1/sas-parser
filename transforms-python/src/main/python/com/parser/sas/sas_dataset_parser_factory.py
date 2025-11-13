"""
SAS Dataset Parser Factory
Matches the Java SASDatasetParserFactory
"""
from .sas_dataset_parser import SASDatasetParser
from .pyreadstat_parser import PyReadstatParser


class SASDatasetParserFactory:
    """
    Factory for creating SAS dataset parsers.
    Matches the Java SASDatasetParserFactory implementation.
    """
    
    @staticmethod
    def create() -> SASDatasetParser:
        """
        Create a SAS dataset parser instance.
        
        Returns:
            SASDatasetParser implementation (PyReadstatParser)
        """
        return PyReadstatParser()

