import pandas as pd

from etl.source_adapter import SourceAdapter, ManualTransformer, read_excel_with_engine_fallback


class GenericExcelExtractor(SourceAdapter):
    """
    Generic Excel extractor that can handle any Excel file (.xls, .xlsx, .xlsm).
    Automatically detects the appropriate engine and returns a pandas DataFrame.
    """

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        # Get optional parameters for Excel reading
        self.sheet_name = kwargs.get("SHEET_NAME", 0)  # Default to first sheet
        self.header = kwargs.get("HEADER", 0)  # Default to first row as header
        self.usecols = kwargs.get("USECOLS", None)  # Optional column selection

    _transform = ManualTransformer._transform

    def _download(self):
        """
        Download and read Excel file using the robust Excel parser.
        Returns a pandas DataFrame ready for processing.
        """
        if not self.file_path:
            raise ValueError(f"No file path specified for source {self.source_id}")
        
        # Build the full file path
        file_path = self.config.input_data_data / self.file_path
        
        # Read the Excel file with our robust parser
        self.dataframe = read_excel_with_engine_fallback(
            file_path,
            sheet_name=self.sheet_name,
            header=self.header,
            usecols=self.usecols
        )
        
        return self.dataframe


class ExcelFileExtractor(SourceAdapter):
    """
    Alternative Excel extractor for files that need more specific handling.
    Useful when you need to specify exact parameters for Excel reading.
    """

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        self.sheet_name = kwargs.get("SHEET_NAME", 0)
        self.header = kwargs.get("HEADER", 0)
        self.usecols = kwargs.get("USECOLS", None)
        self.skiprows = kwargs.get("SKIPROWS", None)

    _transform = ManualTransformer._transform

    def _download(self):
        """
        Download and read Excel file with specific parameters.
        """
        if not self.file_path:
            raise ValueError(f"No file path specified for source {self.source_id}")
        
        file_path = self.config.input_data_data / self.file_path
        
        self.dataframe = read_excel_with_engine_fallback(
            file_path,
            sheet_name=self.sheet_name,
            header=self.header,
            usecols=self.usecols,
            skiprows=self.skiprows
        )
        
        return self.dataframe 