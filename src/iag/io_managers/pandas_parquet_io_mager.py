import os
import pandas as pd
from dagster import IOManager, io_manager

class PandasParquetIOManager(IOManager):
    def __init__(self):
        self._base_dir = "/opt/dagster/data"

    def _get_path(self, context):
        asset_name = context.asset_key.path[-1]
        return os.path.join(self._base_dir, f"{asset_name}.parquet")

    def handle_output(self, context, obj: pd.DataFrame):
        context.log.info("Iniciando salvamento de DataFrame como Parquet")
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        context.log.info(f"Salvando dados em: {path}")
        obj.to_parquet(path)

    def load_input(self, context):
        path = self._get_path(context.upstream_output)
        context.log.info(f"Carregando dados de: {path}")
        return pd.read_parquet(path)

@io_manager
def pandas_parquet_io_manager():
    return PandasParquetIOManager()