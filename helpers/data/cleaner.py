from .loader import DataLoader
from pathlib import Path
from dotenv import load_dotenv
import polars as pl, os

load_dotenv("config/.env")


class CleanerBase:
    def __init__(self):
        self.os_user = os.getenv("USERNAME")

    def _resolve_path(self, path_template):
        return Path(path_template.format(username=self.os_user)).resolve()

    def _get_path(self, env_key: str) -> Path:
        return Path(self._resolve_path(os.getenv(env_key)))

    def _load_file(self, env_key):
        path = self._get_path(env_key)
        data_map = DataLoader(path).load_data()
        return data_map[path]

    def _rename(self, df: pl.DataFrame, rename_map: dict) -> pl.DataFrame:
        return df.select(list(rename_map.keys())).rename(rename_map)