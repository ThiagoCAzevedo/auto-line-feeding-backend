from services.pkmc.pkmc import PKMC_Cleaner, PKMC_DefineDataframe
from database.queries import UpsertInfos

class BuildPipeline:
    @staticmethod
    def build_pkmc(raw_svc: PKMC_DefineDataframe, cleaner_svc: PKMC_Cleaner):
        df = raw_svc.create_df()
        df = cleaner_svc.rename_columns(df)
        df = cleaner_svc.filter_columns(df)
        df = cleaner_svc.clean_columns(df)
        df = cleaner_svc.create_columns(df)
        return df   

    
class DependenciesInjection:
    @staticmethod
    def get_pkmc() -> PKMC_DefineDataframe:
        return PKMC_DefineDataframe()

    @staticmethod
    def get_pkmc_cleaner() -> PKMC_Cleaner:
        return PKMC_Cleaner()
    
    @staticmethod
    def get_upsert_service() -> UpsertInfos:
        return UpsertInfos()