import polars as pl
from datetime import date
from backend.log import log_files
from backend.database import db_manager_general, db_manager_lines, db_manager_knr


logger = log_files.get_logger('lines | lb_balance | define_values', 'lines')

class ValuesSaldoLBWithQuery:
    def __init__(self):
        self.df_values_pkmc_pk05 = pl.DataFrame()
        self.df_values_saldo_lb = pl.DataFrame()
        self.df_consumo_saldo = pl.DataFrame()
        self.df_values_lt22 = pl.DataFrame()
        self.actual_date = date.today()
        self.db_manager = db_manager_general
        self.db_manager_lines = db_manager_lines
        self.db_manager_knr = db_manager_knr

        logger.info("ValuesSaldoLBWithQuery inicializado")

    def sql_values_join_linha_knr(self):
        try:
            logger.info("Iniciando pipeline de cálculo do saldo_lb")

            query = """
                WITH knrs_validos AS (
                    SELECT DISTINCT 
                        lm.tacto, 
                        lm.knr, 
                        lm.lfdnr_sequencia,
                        kc.knr_fx4pd,
                        kc.partnumber,
                        kc.quantidade,
                        kc.quantidade_unidade
                    FROM linha.linha_montagem lm
                    INNER JOIN knr.knrs_comum kc 
                        ON lm.knr = kc.knr
                    WHERE lm.knr IS NOT NULL
                )
                SELECT 
                    kv.tacto,
                    kv.knr,
                    kv.knr_fx4pd,
                    kv.partnumber,
                    kv.quantidade,
                    kv.quantidade_unidade
                FROM knrs_validos kv
                INNER JOIN knr.pkmc_pk05 pk 
                    ON kv.tacto = pk.tacto 
                   AND TRIM(kv.partnumber) = TRIM(pk.partnumber)
            """

            results = self.db_manager.execute_query(query)
            logger.info(f"Consulta retornou {len(results)} registros")
            

            self.df_consumo_saldo = (
                pl.DataFrame(results)
                .with_columns(
                    pl.col("partnumber")
                    .cast(pl.Utf8)
                    .str.strip_chars()
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase(),
                    pl.col("tacto")
                    .cast(pl.Utf8)
                    .str.strip_chars()
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase()
                )
            )
            
            logger.debug(f"Formato do DataFrame consumo_saldo: {self.df_consumo_saldo.shape}")

            logger.info("Filtrando consumos já processados existentes em linha.saldo_lb")

            df_consumidos = pl.DataFrame(
                self.db_manager.execute_query("""
                    SELECT tacto, knr, partnumber, quantidade
                    FROM linha.saldo_lb
                """)
            )

            if len(df_consumidos) > 0:
                antes = len(self.df_consumo_saldo)
                self.df_consumo_saldo = self.df_consumo_saldo.join(
                    df_consumidos,
                    on=["tacto", "knr", "partnumber", "quantidade"],
                    how="anti"
                )
                depois = len(self.df_consumo_saldo)
                logger.info(
                    f"Removidos {antes - depois} consumos duplicados; {depois} novos registros permanecem."
                )
            else:
                logger.info("Tabela linha.saldo_lb vazia — todos os consumos serão processados.")

            if len(self.df_consumo_saldo) == 0:
                logger.info("Nenhum consumo novo a processar — atualização de saldo ignorada.")
                return

            self._save_values_bd()
            self._update_values_pkmc_pk05()
            logger.info("Pipeline de saldo_lb executado com sucesso")

        except Exception as e:
            logger.error(f"Falha no pipeline do saldo_lb: {e}", exc_info=True)
            raise

    def _save_values_bd(self):
        try:
            logger.info("Salvando consumo_saldo na tabela linha.saldo_lb")
            self.db_manager_lines.insert_saldo_lb(self.df_consumo_saldo)
            logger.info(f"{len(self.df_consumo_saldo)} registros inseridos/atualizados em saldo_lb")
        except Exception as e:
            logger.error(f"Falha ao inserir dados em saldo_lb: {e}", exc_info=True)
            raise

    def _update_values_pkmc_pk05(self):
        try:
            logger.info("Atualizando pkmc_pk05 com ajustes de consumo e LT22")

            df_lt22 = pl.DataFrame(
                self.db_manager.execute_query("""
                    SELECT partnumber, quantidade, prateleira, num_ot
                    FROM knr.lt22 
                    WHERE num_ot_usado IS FALSE
                """),
                schema={
                    "partnumber": pl.Utf8,
                    "quantidade": pl.Int64,
                    "prateleira": pl.Utf8,
                    "num_ot": pl.Utf8
                }
            )

            if len(df_lt22) > 0:
                logger.info(f"Dados LT22 carregados: {len(df_lt22)} registros para ajustar estoque")

                # Renomeia e AGGREGA todos os valores duplicados
                df_lt22_agg = (
                    df_lt22
                    .group_by(["partnumber", "prateleira"])
                    .agg(pl.sum("quantidade").alias("estoque_lt22"))
                )

            else:
                logger.warning("Nenhum registro LT22 encontrado para hoje — ajustes de estoque serão nulos")
                df_lt22_agg = pl.DataFrame(
                    schema={"partnumber": pl.Utf8, "prateleira": pl.Utf8, "estoque_lt22": pl.Int64}
                )

            df_pkmc_pk05 = pl.DataFrame(
                self.db_manager.execute_query(
                    "SELECT partnumber, tacto, saldo_lb, prateleira FROM knr.pkmc_pk05"
                )
            )
            logger.info(f"{len(df_pkmc_pk05)} registros carregados de pkmc_pk05 para atualização")

            df_consumo_aggregado = (
                self.df_consumo_saldo
                .select(["tacto", "partnumber", "quantidade"])
                .group_by(["tacto", "partnumber"])
                .agg(pl.sum("quantidade").alias("consumo_total"))
            )

            logger.debug(f"Consumos agregados: {len(df_consumo_aggregado)} registros")

            df_result = (
                df_pkmc_pk05
                .join(df_consumo_aggregado, on=["tacto", "partnumber"], how="left")
                .join(df_lt22_agg, on=["partnumber", "prateleira"], how="left")
            )

            df_result = df_result.with_columns([
                (
                    pl.col("saldo_lb")
                    - pl.col("consumo_total").fill_null(0)
                    + pl.col("estoque_lt22").fill_null(0)
                ).alias("quantidade_final")
            ])

            logger.info(f"{len(df_result)} registros preparados para atualização do saldo_lb em pkmc_pk05")

            # Atualiza status das OT usadas
            self.db_manager_knr.update_lt22(df_lt22.select(["num_ot"]))
            logger.debug("Campos num_ot_usado da LT22 marcados como TRUE")

            # Atualiza tabela de estoque
            self.db_manager_knr.update_pkmc_pk05(
                df_result.select(["tacto", "partnumber", "quantidade_final"])
            )
            logger.info("Tabela pkmc_pk05 atualizada com sucesso")
        except Exception as e:
            logger.error(f"Falha durante atualização de PKMC_PK05: {e}", exc_info=True)
            raise


def main():
    try:
        logger.info("Iniciando execução principal de ValuesSaldoLBWithQuery")
        processor = ValuesSaldoLBWithQuery()
        processor.sql_values_join_linha_knr()
        logger.info("Execução principal de ValuesSaldoLBWithQuery concluída com sucesso")
    except Exception as e:
        logger.critical(f"Erro fatal na função main: {e}", exc_info=True)


if __name__ == "__main__":
    main()