"""
Expose JobDefinitions so Dagster-Components can auto-discover them.
Simply importing them at module scope is sufficient.
"""
from pipeline.defs.jobs.asset_jobs import (
    materialize_all_assets_job,
)
# Nothing else needed – the imported names remain in the module namespace
