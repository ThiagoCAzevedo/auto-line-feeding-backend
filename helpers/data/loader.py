import polars as pl
from typing import Union, List


class DataLoader:
    def __init__(self, file_paths: Union[str, List[str]]):
        self.file_paths = [file_paths]

    def define_ext_file(self, file_path: str) -> str:
        return file_path.suffix

    def load_data(self):
        loaded_data = {}

        for file_path in self.file_paths:
            ext = self.define_ext_file(file_path)

            if ext in [".xlsx", ".xls", ".xlsm", ".XLSX"]:
                df = pl.read_excel(
                    file_path,
                    raise_if_empty=False,
                    read_csv_options={"infer_schema_length": 10000}
                )
            elif ext == ".parquet":
                df = pl.scan_parquet(file_path).collect()
            elif ext in [".csv", ".txt"]:
                df = pl.scan_csv(
                        file_path,
                        truncate_ragged_lines=True,
                        encoding="utf8-lossy",
                        has_header=True,
                    )

            loaded_data[file_path] = df

        return loaded_data