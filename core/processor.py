import pandas as pd
from logging import Logger
from .schema import SchemaAligner
from .validation import ValidationEngine

class DataProcessor:
    """
    Orchestrates schema alignment and validation.
    """

    def __init__(
            self,
            logger: Logger,
            schema_aligner: SchemaAligner,
            validation_engine: ValidationEngine
    ) -> None:
        self.logger = logger
        self.schema_aligner = schema_aligner
        self.validation_engine = validation_engine

    def process(
            self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process a single DataFrame
        """
        # Align schema
        df_aligned = self.schema_aligner.align(df)

        # Validate
        cleaned_df, rejected_df = self.validation_engine.validate(df_aligned)

        self.logger.info(
            f"Processing complete | Clean: {len(cleaned_df)} | Rejected: {len(rejected_df)}"
        )

        return cleaned_df, rejected_df
    
    def process_multiple(
            self, dfs: list[pd.DataFrame]
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        
        cleaned_frames = []
        rejected_frames = []

        for df in dfs:
            clean, rejected = self.process(df)

            if not clean.empty:
                cleaned_frames.append(clean)
            
            if not rejected.empty:
                rejected_frames.append(rejected)
        
        final_cleaned = (
            pd.concat(cleaned_frames, ignore_index=True)
            if cleaned_frames
            else pd.DataFrame()
        )

        final_rejected = (
            pd.concat(rejected_frames, ignore_index=True)
            if rejected_frames
            else pd.DataFrame()
        )

        self.logger.info(
            f"Aggregated results |\n"
            f"Total Clean: {len(final_cleaned)}\n"
            f"Total Rejected: {len(final_rejected)}"
        )

        return final_cleaned, final_rejected