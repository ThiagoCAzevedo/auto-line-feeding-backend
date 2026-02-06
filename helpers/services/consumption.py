from services.consumption.consumer import ConsumeValues


class DependeciesInjection:
    @staticmethod
    def get_consume() -> ConsumeValues:
        return ConsumeValues()