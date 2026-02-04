import json
import polars as pl

def extract_car_info_polars(data: dict):
    registros = []

    # Remover "reception" recursivamente
    def remove_reception(d):
        if isinstance(d, dict):
            return {k: remove_reception(v) for k, v in d.items() if k != "reception"}
        elif isinstance(d, list):
            return [remove_reception(x) for x in d]
        return d

    data = remove_reception(data)

    # Percorrer lanes
    for lane_key, lane_val in data.items():
        if lane_key.startswith("lane_"):

            for fb_key, fb_val in lane_val.items():

                for tact_key, tact_val in fb_val.items():

                    if isinstance(tact_val, dict) and "CAR" in tact_val and tact_val["CAR"]:
                        car = tact_val["CAR"]

                        registros.append({
                            "KNR": car.get("KNR"),
                            "MODELL": car.get("MODELL"),
                            "LFDNR": car.get("LFDNR"),
                            "LANE": tact_val.get("LANE", lane_key)
                        })

    return pl.DataFrame(registros)


# -----------------------------
# USO CORRETO
# -----------------------------

# 1) carregar JSON do arquivo
# with open(r"C:\Users\thiago.azevedo\OneDrive - Sese\thiago_sese\auto_line_feeding\backend\teste.json", "r", encoding="utf-8") as f:
#     json_data = json.load(f)

# # 2) processar
# df = extract_car_info_polars(json_data)

# # 3) visualizar
# print(df)