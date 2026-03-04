# 9
import pandas as pd
from collections import Counter
from logging import Logger

# ----------------------------

def null_ratios(df: pd.DataFrame, logger: Logger | None = None) -> dict[str, float]:
    """
    Calculate null ratio for each column in the dataframe.
    Returns: {column_name: null_fraction}
    """
    if df.empty:
        return {}
    
    ratios = df.isna().mean().to_dict()

    if logger:
        logger.info(f"Null ratios per column: {ratios}")
    
    return ratios

# ----------------------------

def duplicate_rate(df: pd.DataFrame, subset: list[str] | None = None, logger: Logger | None = None) -> float:
    """
    compute fraction of duplicate row.
    subset: list of columns to check duplicates on (optional)
    Returns float between 0.0 and 1.0
    """
    if df.empty:
        return 0.0
    
    rate = df.duplicated(subset=subset).mean()
    
    return rate

# ----------------------------

def top_rejection_reasons(df: pd.DataFrame, top_n: int = 10, logger: Logger | None = None) -> dict[str, int]:
    """
    Return top N rejection reasons.
    Handles multiple reasons per row separated by semicolon.
    """
    if df.empty or "rejection_reason" not in df.columns:
        return {}
    
    all_reasons = df["rejection_reason"].dropna().str.split(";").explode()
    all_reasons = all_reasons[all_reasons != ""]

    counter = Counter(all_reasons)
    top_reasons = dict(counter.most_common(top_n))

    if logger:
        logger.info(f"Top rejection reasons: {top_reasons}")
    
    return top_reasons