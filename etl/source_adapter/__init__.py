import pandas as pd
from pathlib import Path
from typing import Union, Any, Literal
from io import StringIO

from .abstract import SourceAdapter
from .abstract import ManualTransformer
from .abstract import ExtractionError
from .abstract import EmptyExtractor


def _try_read_excel_with_engine(
    file_path: Union[str, Path], 
    engine: Literal['openpyxl', 'xlrd'], 
    **kwargs: Any
) -> pd.DataFrame:
    """Attempt reading an Excel file with a specific engine."""
    return pd.read_excel(file_path, engine=engine, **kwargs)


def read_excel_with_engine_fallback(
    file_path: Union[str, Path], 
    **kwargs: Any
) -> pd.DataFrame:
    """
    Read Excel file with appropriate engine based on file extension.
    Uses openpyxl for .xlsx files and xlrd for .xls files.
    """
    file_path_str = str(file_path).lower()
    
    # Choose engine based on file extension
    if file_path_str.endswith('.xlsx') or file_path_str.endswith('.xlsm'):
        engine = 'openpyxl'
    elif file_path_str.endswith('.xls'):
        engine = 'xlrd'
    else:
        # Default to openpyxl for unknown extensions
        engine = 'openpyxl'
    
    try:
        return _try_read_excel_with_engine(file_path, engine, **kwargs)
    except Exception as e:
        raise ExtractionError(
            f"Failed to read Excel file '{file_path}' with {engine} engine: {str(e)}.",
            str(file_path)
        )


def read_csv_with_robust_parsing(
    file_path: Union[str, Path, StringIO], 
    **kwargs: Any
) -> pd.DataFrame:
    """
    Read CSV file with robust parsing that handles quoted fields and mixed delimiters.
    """
    # Check if the input is empty or contains no data
    if isinstance(file_path, StringIO):
        content = file_path.getvalue()
        if not content.strip():
            raise ExtractionError(
                f"CSV file is empty or contains no data.",
                str(file_path)
            )
    
    # Try common delimiters explicitly
    delimiters = [',', ';', '\t', '|']
    
    for delimiter in delimiters:
        try:
            df = pd.read_csv(
                file_path, 
                sep=delimiter,
                engine='python',
                quoting=3,  # QUOTE_NONE - ignore quotes
                **kwargs
            )
            # Check if we got a valid DataFrame with data
            if df is not None and len(df) > 0 and len(df.columns) > 0:
                return df
        except Exception:
            continue
    
    # Try with different quoting strategies
    quoting_strategies = [3, 0, 1]  # QUOTE_NONE, QUOTE_MINIMAL, QUOTE_ALL
    
    for quoting in quoting_strategies:
        try:
            df = pd.read_csv(
                file_path, 
                sep=',',  # Default to comma
                engine='python',
                quoting=quoting,
                **kwargs
            )
            # Check if we got a valid DataFrame with data
            if df is not None and len(df) > 0 and len(df.columns) > 0:
                return df
        except Exception:
            continue
    
    # Last resort: try with C engine and minimal settings
    try:
        df = pd.read_csv(
            file_path, 
            sep=',',
            engine='c',
            **kwargs
        )
        # Check if we got a valid DataFrame with data
        if df is not None and len(df) > 0 and len(df.columns) > 0:
            return df
    except Exception as e:
        # Only collect error details if all attempts fail
        raise ExtractionError(
            f"Failed to read CSV file '{file_path}': {str(e)}.",
            str(file_path)
        )
    
    # If we get here, all parsing attempts succeeded but returned empty DataFrames
    raise ExtractionError(
        f"CSV file parsed successfully but contains no data or columns.",
        str(file_path)
    )


def is_excel_file(file_path: Union[str, Path]) -> bool:
    """
    Check if a file path points to an Excel file (.xls, .xlsx, .xlsm).
    """
    if not file_path:
        return False
    
    file_path_str = str(file_path).lower()
    return file_path_str.endswith(('.xls', '.xlsx', '.xlsm'))


def get_default_extractor_class(file_path: Union[str, Path, None]) -> str:
    """
    Determine the default extractor class based on file extension.
    Returns the appropriate extractor class for the file type.
    """
    if not file_path:
        return "etl.source_adapter.EmptyExtractor"
    
    if is_excel_file(file_path):
        return "etl.source_adapter.excel.GenericExcelExtractor"
    
    # For now, default to CSV extractor for other files
    # This can be expanded later for other file types
    return "etl.source_adapter.csv.DefaultCSVExtractor"


def should_skip_source(source_config: dict) -> tuple[bool, str]:
    """
    Determine if a source should be skipped based on its configuration.
    Returns (should_skip, reason) tuple.
    """
    # Check if source has only time period fields and no other meaningful data
    meaningful_fields = [
        'FILE_PATH', 'ENDPOINT_URL', 'ADDRESS', 'EXTRACTOR_CLASS',
        'RAW_OBS_VALUE_COLUMN_NAME', 'COUNTRY_NAME_COLUMN_NAME',
        'WPA_OBS_RAW_COL', 'WPA_YEAR_COL', 'RAW_OBS_VALUE_TYPE'
    ]
    
    has_meaningful_data = any(source_config.get(field) for field in meaningful_fields)
    
    # If no meaningful data and only has time period fields, skip it
    if not has_meaningful_data:
        time_period_fields = ['STARTPERIOD', 'ENDPERIOD', 'TIMEPERIOD', 'TIME_PERIOD']
        has_only_time_fields = all(
            source_config.get(field) in ['xxx', None] or field in time_period_fields
            for field in source_config.keys()
        )
        
        if has_only_time_fields:
            return True, "No meaningful data to extract (only time period fields)"
    
    # Check for other skip conditions
    if not source_config.get('FILE_PATH') and not source_config.get('ENDPOINT_URL') and not source_config.get('ADDRESS'):
        return True, "No data source specified (no FILE_PATH, ENDPOINT_URL, or ADDRESS)"
    
    return False, ""