import polars as pl


class DefineDataFrame:
    def __init__(self, response: dict):
        self.response = response

    def _extract_car_records(self, cleaned):
        registers = []
        for lane_key, lane_val in cleaned.items():
            if lane_key.startswith("lane_") or lane_key.startswith("reception"):
                for fb_key, fb_val in lane_val.items():
                    for tact_key, tact_val in fb_val.items():
                        if isinstance(tact_val, dict) and "CAR" in tact_val and tact_val["CAR"]:
                            car = tact_val["CAR"]
                            registers.append({
                                "knr": car.get("KNR"),
                                "model": car.get("MODELL"),
                                "lfdnr_sequence": car.get("LFDNR"),
                                "werk": car.get("WERK"),
                                "spj": car.get("SPJ"),
                                "lane": tact_val.get("LANE", lane_key)
                            })
        return pl.DataFrame(registers)
    

class TransformDataFrame:
    def __init__(self, df):
        self.df = df

    def transform(self):
        return (
            self.df
            .with_columns([
                pl.col("lane").str.replace("lane_", ""),
                pl.col("lfdnr_sequence").cast(pl.Utf8)
            ])
        )
    
    def create_fx4pd_column(self):
        return self.df.with_columns(
            (pl.col("werk") + pl.col("spj") + pl.col("knr")).alias("knr_fx4pd")
        )