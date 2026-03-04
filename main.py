from infrastructure.environment import AppEnvironment
from infrastructure.logger import get_logger
from infrastructure.file_loader import FileLoader
from infrastructure.output_writer import OutputWriter
from infrastructure.report_writer import ReportWriter
from infrastructure.email_sender import EmailSender
from core.config import Config
from core.schema import SchemaAligner
from core.validation import ValidationEngine
from core.processor import DataProcessor
from diagnostics.quality import null_ratios, duplicate_rate, top_rejection_reasons
from pathlib import Path

def main():

    # Configuration
    CONFIG = {
        "output_formats": ["csv", "xlsx"],
        "generate_report": True,
        "send_email": True
    }

    # Initialize environment & directories
    env = AppEnvironment()
    env.create_dirs()

    # Initialize logger
    logger = get_logger(env.logs_dir)
    logger.info("Pipeline started")

    # Load config (schema + rules) once via core.config
    config = Config(schema_path=env.schema_path, rules_path=env.rules_path)

    # Initialize components
    file_loader = FileLoader(logger)
    schema_aligner = SchemaAligner(logger, config.schema)
    validation_engine = ValidationEngine(logger, config.rules)

    data_processor = DataProcessor(
        logger=logger,
        schema_aligner=schema_aligner,
        validation_engine=validation_engine
    )

    output_writer = OutputWriter(logger, base_output_dir=env.output_dir)
    report_writer = ReportWriter(logger, env.reports_dir)
    email_sender = EmailSender(logger)

    # Load files
    files: list[Path] = list(env.input_dir.iterdir())
    dfs: list = []

    for file in files:
        dfs.extend(file_loader.load(file))

    # Process DataFrames
    cleaned_df, rejected_df = data_processor.process_multiple(dfs)

    # Write outputs (per file)
    generated_files: list[Path] = []

    for file in files:
        paths = output_writer.write(
            cleaned_df=cleaned_df,
            rejected_df=rejected_df,
            original_filename=file.name,
            formats=CONFIG["output_formats"]
        )
        generated_files.extend(paths)

    # Generate report
    report_path = None

    if CONFIG["generate_report"]:
        report_path = report_writer.generate(
            cleaned_df=cleaned_df,
            rejected_df=rejected_df,
            original_filename="pipeline",
            output_format="txt"
        )

        if report_path:
            generated_files.append(report_path)

    # Diagnostics
    logger.info(f"Top rejection reasons:\n{top_rejection_reasons(df=rejected_df, logger=logger)}")
    logger.info(f"Null ratios:\n{null_ratios(cleaned_df, logger=logger)}")
    logger.info(f"Duplicate rate: {duplicate_rate(cleaned_df, logger=logger):.2%}")

    # Send email
    if CONFIG["send_email"] and generated_files:
        email_sender.send(
            attachments=generated_files,
            subject="Excel Cleaning Pipeline Completed",
            body=(
                "Hello,\n\n"
                "The Excel cleaning pipeline has finished successfully.\n"
                "Attached are the generated outputs and report.\n\n"
            ),
            enable=True
        )

    logger.info("Pipeline finished sucessfully")

if __name__ == '__main__':
    main()