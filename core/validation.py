import pandas as pd
from logging import Logger


class ValidationEngine:
    """
    Apply validation rules from rules.yaml.

    Returns:
        cleaned_df
        rejected_df (with rejection_reason column)
    """

    def __init__(self, logger: Logger, rules: dict) -> None:
        self.logger = logger
        self.rules = rules

    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

        df = df.copy()
        df["rejection_reason"] = ""

        for column, rule in self.rules.items():

            if column not in df.columns:
                self.logger.warning(f"Column '{column}' not found in dataframe.")
                continue

            # 1️⃣ NULL RULE
            self._apply_null_rule(df, column, rule)

            # 2️⃣ TYPE-SPECIFIC RULES
            rule_type = rule.get("type")

            if rule_type == "numeric":
                self._apply_numeric_rule(df, column, rule)

            elif rule_type == "datetime":
                self._apply_datetime_rule(df, column, rule)

            elif rule_type == "categorical":
                self._apply_categorical_rule(df, column, rule)

        rejected_df = df[df["rejection_reason"] != ""].copy()
        cleaned_df = df[df["rejection_reason"] == ""].copy()

        cleaned_df.drop(columns=["rejection_reason"], inplace=True)

        self.logger.info(
            f"Validation finished | Clean: {len(cleaned_df)} | Rejected: {len(rejected_df)}"
        )

        return cleaned_df, rejected_df

    # ==========================================================
    # RULE IMPLEMENTATIONS
    # ==========================================================

    def _apply_null_rule(self, df: pd.DataFrame, column: str, rule: dict) -> None:

        if rule.get("allow_null", True):
            return

        mask = df[column].isna() | (
            df[column].astype(str).str.strip() == ""
        )

        df.loc[mask, "rejection_reason"] += f"{column}_null;"

    # ----------------------------------------------------------

    def _apply_numeric_rule(self, df: pd.DataFrame, column: str, rule: dict) -> None:

        # Clean common numeric formatting issues
        df[column] = (
            df[column]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.strip()
        )

        df[column] = pd.to_numeric(df[column], errors="coerce")

        # Reject if conversion failed AND null not allowed
        if not rule.get("allow_null", True):
            mask_null = df[column].isna()
            df.loc[mask_null, "rejection_reason"] += f"{column}_invalid_numeric;"

        if "min" in rule:
            mask_min = df[column] < rule["min"]
            df.loc[mask_min, "rejection_reason"] += f"{column}_below_min;"

        if "max" in rule:
            mask_max = df[column] > rule["max"]
            df.loc[mask_max, "rejection_reason"] += f"{column}_above_max;"

    # ----------------------------------------------------------

    def _apply_datetime_rule(self, df: pd.DataFrame, column: str, rule: dict) -> None:

        df[column] = pd.to_datetime(
            df[column],
            errors="coerce"
        )

        # Reject invalid dates if null not allowed
        if not rule.get("allow_null", True):
            mask_null = df[column].isna()
            df.loc[mask_null, "rejection_reason"] += f"{column}_invalid_date;"

        # Reject future dates if not allowed
        if not rule.get("allow_future", True):
            mask_future = df[column] > pd.Timestamp.today()
            df.loc[mask_future, "rejection_reason"] += f"{column}_future;"

    # ----------------------------------------------------------

    def _apply_categorical_rule(self, df: pd.DataFrame, column: str, rule: dict) -> None:

        allowed_values = rule.get("allowed_values")

        # 🚨 IMPORTANT: Only validate if allowed_values is defined and not empty
        if not allowed_values:
            self.logger.warning(
                f"Column '{column}' has categorical type but no allowed_values defined."
            )
            return

        # Normalize both sides to lowercase for safe comparison
        allowed = {str(v).lower() for v in allowed_values}

        mask = ~df[column].astype(str).str.lower().isin(allowed)

        df.loc[mask, "rejection_reason"] += f"{column}_invalid;"