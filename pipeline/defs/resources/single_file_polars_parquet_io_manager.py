import os
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext

class SingleFilePolarsParquetIOManager(ConfigurableIOManager):
    """Store one Polars (Lazy)DataFrame per asset as `{asset}/{asset}.parquet`."""
    base_dir: str

    # ————————————————————————————————————————————————
    def _write_parquet(self, path: str, obj):
        if isinstance(obj, pl.LazyFrame):
            obj.sink_parquet(path, compression="zstd", row_group_size=50_000)
        elif isinstance(obj, pl.DataFrame):
            obj.write_parquet(path, compression="zstd")
        else:
            raise ValueError(
                "SingleFilePolarsParquetIOManager only supports Polars "
                "DataFrame or LazyFrame objects."
            )

    # ————————————————————————————————————————————————
    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            context.log.info("No data to write (None).")
            return

        asset = context.asset_key.to_python_identifier()
        dir_path = os.path.join(self.base_dir, asset)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f"{asset}.parquet")

        self._write_parquet(file_path, obj)
        context.log.info(f"[SingleFileIO] Wrote → {file_path}")

    # ————————————————————————————————————————————————
    def load_input(self, context: InputContext) -> pl.DataFrame:
        asset = context.asset_key.to_python_identifier()
        file_path = os.path.join(self.base_dir, asset, f"{asset}.parquet")
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)
        df = pl.read_parquet(file_path)
        context.log.info(f"[SingleFileIO] Loaded {len(df):,} rows ← {file_path}")
        return df
