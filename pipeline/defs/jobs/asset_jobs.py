# pipeline/jobs/asset_jobs.py
"""
Utility helpers – no Dagster code at import time to avoid cycles.

• materialize_all_assets_job  – runs the entire graph
• build_pair_jobs(keys)       – 1 job per “<name>_raw ↔ <name>” pair
• build_multi_pair_job(names) – combine any number of those pairs into
                                a single custom job
"""
from typing import Iterable, List, Sequence

from dagster import AssetKey, AssetSelection, JobDefinition, define_asset_job

# ────────────────────────────────────────────────────────────────────
# Helper ▸ turn AssetKey → plain string
# ────────────────────────────────────────────────────────────────────
def key_name(k: AssetKey) -> str:
    return "/".join(k.path)


# ────────────────────────────────────────────────────────────────────
# 1.  “Run everything” job
# ────────────────────────────────────────────────────────────────────
materialize_all_assets_job = define_asset_job(
    name="materialize_all_assets_job",
    selection=AssetSelection.all(),
)

