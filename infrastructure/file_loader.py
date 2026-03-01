# 5
import pandas as pd
from pathlib import Path
from logging import Logger

class FileLoader:
    """
    Handles loading Excel and CSV files.
    Supports chenked loading for large CSV files.
    """

    def __init__(self, logger: Logger, chunk_size: int = 200_000) -> None:
        self.logger = logger
        self.chunk_size = chunk_size
    
    def load(self, file_path: str | Path) -> list[pd.DataFrame]:
        """
        Load file and return a list of DataFrames.
        - For excel: one DataFrame per sheet
        - For CSV: list of chunks (or single df if small)
        """

        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        suffix = path.suffix.lower()
        
        self.logger.info(f"Loading file: {path.name}")

        if suffix in [".xlsx", ".xls"]:
            return self._load_excel(path)
        
        elif suffix == ".csv":
            return self._load_csv(path)
        
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
    
    # -------------------------

    def _load_excel(self, path: Path) -> list[pd.DataFrame]:
        """
        Load all sheets from Excel file.
        """

        self.logger.info("Reading Excel file...")

        sheets = pd.read_excel(path, sheet_name=None, engine='openpyxl')

        dataframes: list[pd.DataFrame] = []

        for sheet_name, df in sheets.items():
            self.logger.info(
                f"Loaded sheet '{sheet_name}' | Rows: {len(df)}"
            )
            dataframes.append(df)
        
        return dataframes
    
    # -------------------------

    def _load_csv(self, path: Path) -> list[pd.DataFrame]:
        """
        Load CSV file in chunks (memory-safe for large files).
        """

        self.logger.info(
            f"Reading CSV file in chunks (size={self.chunk_size})"
        )

        chunks: list[pd.DataFrame] = []

        for chunk in pd.read_csv(path, chunksize=self.chunk_size):
            self.logger.info(f"Loaded chunk | Rows {len(chunk)}")
            chunks.append(chunk)
        
        return chunks