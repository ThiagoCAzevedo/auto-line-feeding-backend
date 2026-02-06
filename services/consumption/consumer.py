from database.queries import SelectInfos, UpdateInfos
import polars as pl
    

class ConsumeValues(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def values_to_consume(self):
        df_values_to_consume = self.select_bd_infos_polars("""
            SELECT 
                forecast.partnumber, 
                forecast.takt, 
                forecast.rack, 
                forecast.knr_fx4pd, 
                forecast.qty_usage, 
                assembly_line.takt,
                pkmc.partnumber,
                pkmc.lb_balance,
                pkmc.lb_balance        
            FROM forecast
            INNER JOIN assembly_line 
                ON forecast.knr_fx4pd = assembly_line.knr_fx4pd 
               AND forecast.takt = assembly_line.takt
            INNER JOIN pkmc
                ON pkmc.partnumber = forecast.partnumber
        """)

        return df_values_to_consume.with_columns(
            (pl.col("lb_balance") - pl.col("qty_usage").fill_null(0)).alias("lb_balance")
        ).select(["partnumber", "lb_balance"]).collect()

    def _update_infos(self, df):
        UpdateInfos().update_df(
            table="pkmc",
            df=df,
            key_column="partnumber",
            batch_size=1000
        )