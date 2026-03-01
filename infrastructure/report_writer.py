# 7 
import pandas as pd
from pathlib import Path
from logging import Logger
from datetime import datetime

class ReportWriter:
    """
    Generates a report summarizing cleand and rejected data.
    Infrastructure layer: filesystem + Excel/CSV output.
    """

    def __init__(self, logger: Logger, report_dir: str | Path = "output/reports") -> None:
        self.logger = logger
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    # --------------------------------------------------

    def generate(
            self,
            cleaned_df: pd.DataFrame,
            rejected_df: pd.DataFrame,
            original_filename: str,
            output_format: str = "txt"
    ) -> Path | None:
        """
        Generates a summary report.
        Returns path to report, or None if no Data
        """

        if cleaned_df.empty and rejected_df.empty:
            self.logger.warning("No data to report.")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name_stem = Path(original_filename).stem
        report_path = self.report_dir / f"{name_stem}_report_{timestamp}.{output_format}"

        total_rows = len(cleaned_df) + len(rejected_df)
        cleaned_rows = len(cleaned_df)
        rejected_rows = len(rejected_df)
        rejection_rate = (rejected_rows / total_rows * 100) if total_rows else 0.0

        # Breakdown of rejection reasons if available
        if not rejected_df.empty and "rejection_reason" in rejected_df.columns:
            rejection_breakdown = (
                rejected_df["rejection_reason"]
                .value_counts()
                .to_dict()
            )
        else:
            rejection_breakdown = {}
        
        # Write report
        if output_format == "txt":
            with open(report_path, "w", encoding="utf-8") as f:

                f.write(f"Report for: {original_filename}\n")
                f.write(f"Generated at: {datetime.now()}\n")
                f.write(f"Total rows processed: {total_rows}\n")
                f.write(f"Cleaned rows: {cleaned_rows}\n")
                f.write(f"Rejected rows: {rejected_rows}\n")
                f.write(f"Rejection rate: {rejection_rate:.2f}%\n\n")

                f.write("Rejection breakdown:\n")
                for reason, count in rejection_breakdown.items():
                    f.write(f" - {reason}: {count}\n")
        
        elif output_format in ("csv", "xlsx"):
            summary = {
                "metric": ["total_rows", "cleaned_rows", "rejection_rows", "rejection_rate"],
                "value": [total_rows, cleaned_rows, rejected_rows, f"{rejection_rate:.2f}%"]
            }
            summary_df = pd.DataFrame(summary)

            # For CSV
            if output_format == "csv":
                summary_df.to_csv(report_path, index=False)
            
            else:
                with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
                    summary_df.to_excel(writer, index=False, sheet_name="summary")
                    if rejection_breakdown:
                        breakdown_df = pd.DataFrame(
                            list(rejection_breakdown.items()),
                            columns=["reason", "count"]
                        )
                        breakdown_df.to_excel(writer, index=False, sheet_name="rejection_breakdown")
        
        else:
            raise ValueError(f"Unsupported report format: {output_format}")
        
        self.logger.info(f"Report generated: {report_path}")
        return report_path