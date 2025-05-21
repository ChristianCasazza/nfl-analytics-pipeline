from pathlib import Path
from dagster_dbt import DbtProject

# Path(__file__).resolve().parents[0] → .../resources
#                             parents[1] → .../defs
#                             parents[2] → .../pipeline
#                             parents[3] → your_project_root
ROOT_DIR = Path(__file__).resolve().parents[3]

dbt_project = DbtProject(
    project_dir=ROOT_DIR / "transformations" / "dbt",
)

dbt_project.prepare_if_dev()
