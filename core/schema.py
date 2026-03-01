# 2
import pandas as pd
from logging import Logger

class SchemaAligner:
    """
    Aligns incoming DataFrame columns to canonical schema.
    """

    def __init__(self, logger: Logger, schema: dict) -> None:
        self.logger = logger
        self.required_columns: list[str] = schema.get("required_columns", [])
        self.aliases: dict[str,str] = schema.get("aliases", {})

        self._normalize_aliases()
    
    def _normalize_aliases(self) -> None:
        """
        Normalize alias keys and values once at initialization
        """
        normalized = {}

        for k,v in self.aliases.items():
            normalized_key = self._normalize_column(k)
            normalized_value = self._normalize_column(v)
            normalized[normalized_key] = normalized_value
        
        self.aliases = normalized
        print(normalized)
    
    def _normalize_column(self, name: str) -> str:
        return name.strip().lower().replace(" ","_")
    
    def align(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Normalize incoming column names
        df.columns = [self._normalize_column(c) for c in df.columns]

        self.logger.info(f"Normalized columns: {df.columns.tolist()}")

        # Apply aliases
        df = df.rename(columns=self.aliases)

        # Ensure required columns exist
        for col in self.required_columns:
            normalized = self._normalize_column(col)

            if normalized not in df.columns:
                df[normalized] = None
                self.logger.info(f"Added missing column: {normalized}")

        # Reorder columns
        normalized_required = [
            self._normalize_column(c) for c in self.required_columns
        ]

        oredered_columns = (
            [c for c in normalized_required if c in df.columns]
            + [c for c in df.columns if c not in normalized_required]
        )

        df = df[oredered_columns]

        self.logger.info("Schema alignment completed.")

        return df