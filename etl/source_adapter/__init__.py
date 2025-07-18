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
    Read Excel file with automatic engine detection and fallback.
    Tries openpyxl first, then xlrd if that fails.
    """
    engines: list[tuple[Literal['openpyxl', 'xlrd'], str]] = [
        ('openpyxl', 'xlsx'), 
        ('xlrd', 'xls')
    ]
    errors = []
    
    for engine, format_name in engines:
        try:
            return _try_read_excel_with_engine(file_path, engine, **kwargs)
        except (ValueError, ImportError, FileNotFoundError) as e:
            errors.append(f"{engine} ({format_name}): {str(e)}")
        except Exception as e:
            raise ExtractionError(
                f"Failed to read Excel file '{file_path}' with {engine} engine: {str(e)}.",
                str(file_path)
            )
    
    error_details = "; ".join(errors)
    raise ExtractionError(
        f"Failed to read Excel file '{file_path}'. "
        f"Tried engines: {error_details}. "
        f"Please ensure the file is a valid Excel file (.xls or .xlsx format).",
        str(file_path)
    )