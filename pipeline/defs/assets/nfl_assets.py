# pipeline/assets/ingestion/nfl_pbp_polars_assets.py
# ---------------------------------------------------------------------------
# Lazy-Polars NFL assets – raw downloads, 2024 enrichment, ten-year enrichment
# ---------------------------------------------------------------------------

from __future__ import annotations
import os
from pathlib import Path
from typing import List

import polars as pl
from dagster import asset, AssetKey

# ------------------------------------------------------------------ Config --
PBP_2024_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
    "play_by_play_2024.parquet"
)
PLAYERS_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/players/"
    "players.parquet"
)

YEARS = range(2014, 2025)  # 2014 … 2024 inclusive
BASE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/"
    "play_by_play_{year}.parquet"
)

# Lake roots (set in pipeline/constants.py)
from pipeline.constants import RAW_LAKE_PATH, CLEAN_LAKE_PATH

# ═══════════════════════════════════════════════════════════════════════════
# 1.  RAW download assets
# ═══════════════════════════════════════════════════════════════════════════
@asset(io_manager_key="raw_single_io_manager",
       name="nfl_pbp_2024_raw",
       group_name="NFL",
       description="Raw 2024 play-by-play parquet.")
def nfl_pbp_2024_raw() -> pl.DataFrame:
    return pl.read_parquet(PBP_2024_URL)


@asset(io_manager_key="raw_single_io_manager",
       name="nfl_players_raw",
       group_name="NFL",
       description="Raw player reference table.")
def nfl_players_raw() -> pl.DataFrame:
    return pl.read_parquet(PLAYERS_URL)


# ═══════════════════════════════════════════════════════════════════════════
# 2.  Shared helper functions
# ═══════════════════════════════════════════════════════════════════════════
def _players_lookup_lf(players_path: str) -> pl.LazyFrame:
    return (
        pl.scan_parquet(players_path)
          .select(["gsis_id", "display_name", "position"])
    )


def _add_role(pbp_lf: pl.LazyFrame,
              lookup_lf: pl.LazyFrame,
              role: str) -> pl.LazyFrame:
    renamed = lookup_lf.rename(
        {"gsis_id": f"{role}_player_id",
         "display_name": f"{role}_display_name",
         "position": f"{role}_position"}
    )
    return pbp_lf.join(renamed, on=f"{role}_player_id", how="left")


def _normalize_lazyframes(lfs: List[pl.LazyFrame]) -> List[pl.LazyFrame]:
    if not lfs:
        return lfs
    ref_cols = lfs[0].collect_schema().names()
    out = []
    for lf in lfs:
        cols = lf.collect_schema().names()
        if cols == ref_cols:
            out.append(lf)
        else:
            missing = [pl.lit(None).alias(c) for c in ref_cols if c not in cols]
            out.append(lf.with_columns(missing).select(ref_cols))
    return out


# ═══════════════════════════════════════════════════════════════════════════
# 3.  2024 enrichment  (unchanged)
# ═══════════════════════════════════════════════════════════════════════════
@asset(name="nfl_pbp_2024",
       group_name="NFL",
       io_manager_key="clean_single_io_manager",
       deps=[AssetKey("nfl_pbp_2024_raw"), AssetKey("nfl_players_raw")],
       description="2024 PBP enriched with player names & positions.",
       metadata={"r2_type": "single"})
def nfl_pbp_2024_enriched(context) -> pl.LazyFrame:
    pbp_path = os.path.join(
        RAW_LAKE_PATH, "nfl_pbp_2024_raw", "nfl_pbp_2024_raw.parquet"
    )
    players_path = os.path.join(
        RAW_LAKE_PATH, "nfl_players_raw", "nfl_players_raw.parquet"
    )
    pbp_lf = pl.scan_parquet(pbp_path)
    lookup_lf = _players_lookup_lf(players_path)
    for role in ("passer", "rusher", "receiver"):
        pbp_lf = _add_role(pbp_lf, lookup_lf, role)
    context.log.info("2024 enrichment plan built lazily.")
    return pbp_lf


# ═══════════════════════════════════════════════════════════════════════════
# 4.  Combined 2014-24 raw table  – lazy union
# ═══════════════════════════════════════════════════════════════════════════
@asset(name="nfl_pbp_2014_2024_raw",
       group_name="NFL",
       io_manager_key="clean_single_io_manager",
       description="Combined 2014-2024 play-by-play (lazy union).",
       metadata={"r2_type": "single"})
def nfl_pbp_2014_2024_raw(context) -> pl.LazyFrame:
    lfs: List[pl.LazyFrame] = [
        pl.scan_parquet(BASE_URL.format(year=yr)) for yr in YEARS
    ]
    context.log.info("Combined 11-season PBP defined lazily.")
    return pl.concat(_normalize_lazyframes(lfs), how="vertical")


# ═══════════════════════════════════════════════════════════════════════════
# 5.  ★ NEW ★ Ten-year enrichment   →  nfl_pbp_ten_years
# ═══════════════════════════════════════════════════════════════════════════
@asset(name="nfl_pbp_ten_years",
       group_name="NFL",
       io_manager_key="clean_single_io_manager",
       deps=[AssetKey("nfl_pbp_2014_2024_raw"), AssetKey("nfl_players_raw")],
       description="2014-2024 PBP enriched with player metadata.",
       metadata={"r2_type": "single"})
def nfl_pbp_ten_years(context) -> pl.LazyFrame:
    tenyr_path = os.path.join(
        CLEAN_LAKE_PATH, "nfl_pbp_2014_2024_raw", "nfl_pbp_2014_2024_raw.parquet"
    )  # ← corrected folder
    players_path = os.path.join(
        RAW_LAKE_PATH, "nfl_players_raw", "nfl_players_raw.parquet"
    )

    pbp_lf = pl.scan_parquet(tenyr_path)
    lookup_lf = _players_lookup_lf(players_path)

    for role in ("passer", "rusher", "receiver"):
        pbp_lf = _add_role(pbp_lf, lookup_lf, role)

    context.log.info("Ten-year enrichment plan built lazily.")
    return pbp_lf
