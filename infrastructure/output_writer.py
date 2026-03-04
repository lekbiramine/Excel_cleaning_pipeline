import pandas as pd
from pathlib import Path
from logging import Logger
from datetime import datetime

class OutputWriter:
    """
    Responsible for writing cleaned and rejected data to disk.
    Infrastructure layer: filesystem interaction.
    """

    def __init__(self, logger: Logger, base_output_dir: str | Path = "output") -> None:
        self.logger = logger
        self.base_output_dir = Path(base_output_dir)
    
    # ------------------------------------------

    def write(
            self,
            cleaned_df: pd.DataFrame,
            rejected_df: pd.DataFrame,
            original_filename: str,
            formats: list[str] | None = None
    ) -> list[Path]:
        """
        Write cleaned and rejected data.
        Returns paths of generated files.
        """

        if formats is None:
            formats = ["csv"] # default
        
        if not formats:
            self.logger.info("No output formats specified. Skipping file writing.")
            return []

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create output directories if not exist
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        cleaned_dir = self.base_output_dir / "cleaned"
        rejected_dir = self.base_output_dir / "rejected"
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        rejected_dir.mkdir(parents=True, exist_ok=True)

        name_stem = Path(original_filename).stem
        generated_paths: list[Path] = []

        for fmt in formats:

            if fmt not in ("csv", "xlsx"):
                raise ValueError(f"Unsupported output format: {fmt}")

            # Cleaned
            if not cleaned_df.empty:
                cleaned_path = cleaned_dir / f"{name_stem}_cleaned_{timestamp}.{fmt}"
                self._write_file(cleaned_df, cleaned_path, fmt)
                generated_paths.append(cleaned_path)
                self.logger.info(f"Cleaned file written: {cleaned_path}")
            
            # Rejected
            if not rejected_df.empty:
                rejected_path = rejected_dir / f"{name_stem}_rejected_{timestamp}.{fmt}"
                self._write_file(rejected_df, rejected_path, fmt)
                generated_paths.append(rejected_path)
                self.logger.info(f"Rejected file written: {rejected_path}")
        
        return generated_paths
    
    # ------------------------------------------

    def _write_file(
            self,
            df: pd.DataFrame,
            path: Path,
            fmt: str
    ) -> None:
        
        if fmt == "csv":
            df.to_csv(path, index=False)
        
        elif fmt == "xlsx":
            df.to_excel(path, index=False, engine="openpyxl")
        
        else:
            raise ValueError(f"Unsupported output format: {fmt}")