import logging
import zipfile
import requests
import pandas as pd
from io import BytesIO, StringIO

from etl.source_adapter.csv import DefaultCSVExtractor

log = logging.getLogger(__name__)


class WorldBankExtractor(DefaultCSVExtractor):
    """Custom extractor for World Bank sources that download zip files"""
    
    def _download(self):
        # Download the zip file
        response = requests.get(self.endpoint)
        response.raise_for_status()
        
        # Extract the zip file
        zip_data = BytesIO(response.content)
        
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            # Look for the main data CSV file (the one with the indicator name)
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and 'Female share of employment' in f]
            
            if not csv_files:
                raise ValueError(f"No suitable CSV file found in zip. Available files: {zip_ref.namelist()}")
            
            # Read the first matching CSV file
            csv_filename = csv_files[0]
            with zip_ref.open(csv_filename) as csv_file:
                csv_content = csv_file.read().decode('utf-8')
                
            # Parse the CSV content
            csv_buffer = StringIO(csv_content)
            raw_data = pd.read_csv(csv_buffer)
            
            return raw_data 