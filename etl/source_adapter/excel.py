import logging
import requests
import pandas as pd
from io import BytesIO

from etl.source_adapter.csv import DefaultCSVExtractor

log = logging.getLogger(__name__)


class ExcelExtractor(DefaultCSVExtractor):
    """Custom extractor for Excel file downloads"""
    
    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.sheet_name = kwargs.get("SHEET_NAME", None)
        self.target_column = kwargs.get("TARGET_COLUMN", None)
    
    def _download(self):
        # Download the Excel file
        response = requests.get(self.endpoint)
        response.raise_for_status()
        
        # Read the Excel file
        excel_data = BytesIO(response.content)
        
        if self.sheet_name:
            # Read specific sheet
            raw_data = pd.read_excel(excel_data, sheet_name=self.sheet_name)
        else:
            # Read first sheet
            raw_data = pd.read_excel(excel_data)
        
        return raw_data 