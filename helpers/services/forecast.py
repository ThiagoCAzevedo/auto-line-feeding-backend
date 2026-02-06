from services.forecast.fx4pd import ReturnFX4PDValues
from database.queries import UpsertInfos
from services.forecast.buff_al import ReturnBuffAssemblyLineValues
from services.forecast.fx4pd import ReturnFX4PDValues
from services.forecast.forecaster import DefineForecastValues


class BuildPipeline:
    @staticmethod
    def build_forecast(svc: ReturnFX4PDValues):
        df = svc.create_fx4pd_df()
        df = svc.rename_select_columns(df)
        df = svc.clean_column(df)
        return df
    

class DependenciesInjection:
    @staticmethod
    def get_fx4pd_service() -> ReturnFX4PDValues:
        return ReturnFX4PDValues()

    @staticmethod
    def get_buff_al_service() -> ReturnBuffAssemblyLineValues:
        return ReturnBuffAssemblyLineValues()

    @staticmethod
    def get_forecast_service() -> DefineForecastValues:
        return DefineForecastValues()

    @staticmethod
    def get_upsert_service() -> UpsertInfos:
        return UpsertInfos()