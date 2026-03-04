from pathlib import Path

class AppEnvironment:
    """
    Stores application paths:
    - input/output dirs
    - cleaned/rejected/report dirs
    - config paths (schema/rules)
    """
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent

        # Input
        self.input_dir = self.base_dir / "input"

        # Output
        self.output_dir = self.base_dir / "output"
        self.cleaned_dir = self.output_dir / "cleaned"
        self.rejected_dir = self.output_dir / "rejected"
        self.reports_dir = self.output_dir / "reports"
        self.logs_dir = self.output_dir / "logs"

        # Config
        self.config_dir = self.base_dir / "config"
        self.schema_path = self.config_dir / "schema.json"
        self.rules_path = self.config_dir / "rules.yaml"
    
    def create_dirs(self):
        """
        Automatically create all Path attributes ending with '_dir'
        To Ensure all directories exist.
        """
        for attr_name, path in vars(self).items():
            if attr_name.endswith("_dir") and isinstance(path, Path):
                path.mkdir(parents=True, exist_ok=True)