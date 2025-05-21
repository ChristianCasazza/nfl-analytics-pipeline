# pipeline/definitions.py
"""
Root entry-point for Dagster-Components.

* Defines one global `defs` object.
* Injects shared resources, then wires up assets, jobs, and sensors.
"""
import dagster as dg
from dagster_components import load_defs

import pipeline.defs
from pipeline.constants import RAW_LAKE_PATH, CLEAN_LAKE_PATH

# ────────────────────────── shared resources ────────────────────────────

from pipeline.defs.resources.single_file_polars_parquet_io_manager import (
    SingleFilePolarsParquetIOManager,
)


from pipeline.defs.assets.dbt_assets import dbt_project_assets, dbt_project
from dagster_dbt import DbtCliResource

dbt = DbtCliResource(project_dir=dbt_project.project_dir)

_shared_resources = {
    "dbt": dbt,
    "raw_single_io_manager":   SingleFilePolarsParquetIOManager(base_dir=RAW_LAKE_PATH),
    "clean_single_io_manager": SingleFilePolarsParquetIOManager(base_dir=CLEAN_LAKE_PATH),
}

# ────────────────────────── auto-load assets ─────────────────────────────
_assets = load_defs(pipeline.defs).assets

# ─────────────────────────── jobs ────────────────────────────────────────
from pipeline.defs.jobs.asset_jobs import materialize_all_assets_job


_jobs = [
    materialize_all_assets_job,
]



# ─────────────────────────── Definitions ─────────────────────────────────
defs = dg.Definitions(
    resources=_shared_resources,
    assets=_assets,
    jobs=_jobs,
)
