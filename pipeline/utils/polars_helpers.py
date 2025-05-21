from __future__ import annotations
import re
from typing import Sequence, Optional, Union
import polars as pl

# ────────────────────────────────
# helpers to fetch column names safely
# ────────────────────────────────
def _colnames(df: Union[pl.DataFrame, pl.LazyFrame]) -> list[str]:
    """Return column names for both DataFrame and LazyFrame without side-effects."""
    if isinstance(df, pl.LazyFrame):
        return df.collect_schema().names()
    return list(df.columns)

# ────────────────────────────────
# Generic helpers
# ────────────────────────────────
def to_snake_case(df: Union[pl.DataFrame, pl.LazyFrame]):
    mapping = {c: c.lower().replace(" ", "_").replace("-", "_") for c in _colnames(df)}
    return df.rename(mapping, strict=False)

def safe_rename(df: Union[pl.DataFrame, pl.LazyFrame], mapping: dict[str, str]):
    present = {src: dst for src, dst in mapping.items() if src in _colnames(df)}
    return df.rename(present, strict=False)

def multi_parse_date(
    col: str,
    formats: Sequence[str] = (
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d",
    ),
) -> pl.Expr:
    """
    Try several timestamp formats; first one that parses wins.
    We *first* cast to Utf8 with strict=False so Null-typed columns don’t error.
    """
    base = pl.col(col).cast(pl.Utf8, strict=False)
    expr: Optional[pl.Expr] = None
    for fmt in formats:
        attempt = base.str.strptime(pl.Datetime, format=fmt, strict=False)
        expr = attempt if expr is None else expr.fill_null(attempt)
    return expr

def safe_int(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Int64, strict=False)

def add_bbl(df: Union[pl.DataFrame, pl.LazyFrame]):
    cols = _colnames(df)
    if {"borough", "block", "lot"}.issubset(cols):
        return df.with_columns(pl.format("bbl_{borough}_{block:0>5}_{lot:0>4}").alias("bbl"))
    if {"boro", "block", "lot"}.issubset(cols):
        return df.with_columns(pl.format("bbl_{boro}_{block:0>5}_{lot:0>4}").alias("bbl"))
    return df


def ensure_lazy(frame: Union[pl.DataFrame, pl.LazyFrame]) -> pl.LazyFrame:
    """Return a LazyFrame no matter the input."""
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def as_utf8(col: str) -> pl.Expr:
    """Cast column to Utf8 (string)."""
    return pl.col(col).cast(pl.Utf8)
