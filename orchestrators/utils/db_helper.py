from backend.database import db_manager_general
from backend.log import log_files


logger = log_files.get_logger('orquestrador | utils | db_helper', 'orchestrator')

class DBReturnValues:
    def sequenciamento_values(self):
        try:
            logger.info("Buscando valores em 'tacto_sequenciamento'")

            exist_value_in_tacto_sequenciamento = db_manager_general.execute_query(
                "SELECT COUNT(*) FROM sequenciamento.tacto_sequenciamento"
            )

            if exist_value_in_tacto_sequenciamento and exist_value_in_tacto_sequenciamento[0]["count_star"]:
                total = exist_value_in_tacto_sequenciamento[0]["count_star"]
                logger.info(f"{total} registros encontrados em 'tacto_sequenciamento'")
                return total

            logger.warning("Nenhum valor encontrado em 'tacto_sequenciamento'")
            return None

        except Exception as e:
            logger.error(f"Erro ao buscar valores em 'tacto_sequenciamento': {e}", exc_info=True)
            return None

    def knrs_vivos_values(self):
        try:
            logger.info("Buscando datas em 'knrs_vivos' e 'knrs_fx4pd'")

            exist_value_in_knrs_vivos = db_manager_general.execute_query(
                "SELECT MAX(criado_em) as max_date FROM knr.knrs_vivos"
            )
            exist_value_in_knrs_fx4pd = db_manager_general.execute_query(
                "SELECT MAX(criado_em) as max_date FROM knr.knrs_fx4pd"
            )

            vivo_date = (
                exist_value_in_knrs_vivos[0]["max_date"] if exist_value_in_knrs_vivos else None
            )
            knrs_fx4pd = (
                exist_value_in_knrs_fx4pd[0]["max_date"] if exist_value_in_knrs_fx4pd else None
            )

            if vivo_date and knrs_fx4pd:
                logger.info(f"Últimas datas encontradas - knrs_vivos: {vivo_date}, knrs_fx4pd: {knrs_fx4pd}")
                return vivo_date, knrs_fx4pd

            logger.warning("Nenhuma data encontrada em 'knrs_vivos' ou 'knrs_fx4pd'")
            return None

        except Exception as e:
            logger.error(f"Erro ao buscar datas em 'knrs_vivos' ou 'knrs_fx4pd': {e}", exc_info=True)
            return None

    def knrs_comuns_values(self):
        try:
            logger.info("Buscando contagem de registros em 'knrs_comum'")

            exist_value_in_knrs_comum = db_manager_general.execute_query(
                "SELECT COUNT(knr) as knrs_comum_count FROM knr.knrs_comum"
            )

            if exist_value_in_knrs_comum and exist_value_in_knrs_comum[0]["knrs_comum_count"]:
                total = exist_value_in_knrs_comum[0]["knrs_comum_count"]
                logger.info(f"{total} registros encontrados em 'knrs_comum'")
                return total

            logger.warning("Nenhum valor encontrado em 'knrs_comum'")
            return None

        except Exception as e:
            logger.error(f"Erro ao buscar valores em 'knrs_comum': {e}", exc_info=True)
            return None

    def linha_montagem_values(self):
        try:
            logger.info("Buscando contagem de registros em 'linha_montagem'")

            exist_value_in_linha_montagem = db_manager_general.execute_query(
                "SELECT COUNT(knr) as knrs_linha_montagem_count FROM linha.linha_montagem"
            )

            if exist_value_in_linha_montagem and exist_value_in_linha_montagem[0]["knrs_linha_montagem_count"]:
                total = exist_value_in_linha_montagem[0]["knrs_linha_montagem_count"]
                logger.info(f"{total} registros encontrados em 'linha_montagem'")
                return total

            logger.warning("Nenhum valor encontrado em 'linha_montagem'")
            return None

        except Exception as e:
            logger.error(f"Erro ao buscar valores em 'linha_montagem': {e}", exc_info=True)
            return None
        

db_return_values = DBReturnValues()