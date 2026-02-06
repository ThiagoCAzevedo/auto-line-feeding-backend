from services.pkmc.pkmc import PKMC_Cleaner, PKMC_DefineDataframe
from database.queries import UpsertInfos

    
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