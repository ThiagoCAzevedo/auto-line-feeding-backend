from services.pk05.pk05 import PK05_Cleaner, PK05_DefineDataframe
from database.queries import UpsertInfos

class BuildPipeline:
    @staticmethod
    def build_pk05(raw_svc: PK05_DefineDataframe, cleaner_svc: PK05_Cleaner):
        df = raw_svc.create_df()
        df = cleaner_svc.rename_columns(df)
        df = cleaner_svc.create_columns(df)
        df = cleaner_svc.filter_columns(df)
        return df   

    
class DependenciesInjection:
    @staticmethod
    def get_pk05() -> PK05_DefineDataframe:
        return PK05_DefineDataframe()

    @staticmethod
    def get_pk05_cleaner() -> PK05_Cleaner:
        return PK05_Cleaner()
    
    @staticmethod
    def get_upsert_service() -> UpsertInfos:
        return UpsertInfos()