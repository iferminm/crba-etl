import pandas as pd
from pathlib import Path
from typing import Union, Any, Literal

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
    if file_path_str.endswith('.xlsx'):
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