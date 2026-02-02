from backend.log import log_files
from .utils import db_helper
import polars as pl


logger = log_files.get_logger('database | db_manager | db_manager_knr', 'db')

class DatabaseManagerKNR:
    def __init__(self):
        self.db_helper = db_helper

    def create_knr_tables(self):
        try:
            logger.debug("Criando schema e tabelas de KNR, se não existirem")
            with self.db_helper.get_connection() as conn:
                conn.execute("CREATE SCHEMA IF NOT EXISTS knr")
                self._create_knr_tables(conn)
                conn.commit()
                logger.info("Tabelas de KNR garantidas com sucesso")
        except Exception as e:
            logger.error(f"Falha ao criar tabelas de KNR: {e}")
            raise

    def _create_knr_tables(self, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.knrs_vivos (
                tma VARCHAR,
                cor VARCHAR,
                tst VARCHAR,
                knr VARCHAR,
                knr_fx4pd VARCHAR,
                tmamg VARCHAR,
                cod_pais VARCHAR,
                pais VARCHAR,
                modelo VARCHAR,
                criado_em DATE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.knrs_mortos (
                tma VARCHAR,
                cor VARCHAR,
                tst VARCHAR,
                knr VARCHAR,
                knr_fx4pd VARCHAR,
                tmamg VARCHAR,
                cod_pais VARCHAR,
                pais VARCHAR,
                modelo VARCHAR,
                criado_em DATE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.knrs_fx4pd (
                knr_fx4pd VARCHAR,
                partnumber VARCHAR,
                quantidade FLOAT,
                quantidade_unidade VARCHAR,
                criado_em DATE DEFAULT CURRENT_DATE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.knrs_comum (
                knr VARCHAR,
                knr_fx4pd VARCHAR,
                cor VARCHAR,
                tmamg VARCHAR,
                cod_pais VARCHAR,
                pais VARCHAR,
                modelo VARCHAR,
                partnumber VARCHAR,
                quantidade FLOAT,
                quantidade_unidade VARCHAR,
                criado_em DATE DEFAULT CURRENT_DATE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.lt22 (
                num_ot VARCHAR,
                partnumber VARCHAR,
                tp_destino VARCHAR,
                posicao_destino VARCHAR,
                quantidade FLOAT,
                unidade_deposito VARCHAR,
                usuario VARCHAR,
                prateleira VARCHAR,
                data_confirmacao DATE,
                hora_confirmacao TIME,
                criado_em DATE DEFAULT CURRENT_DATE,
                num_ot_usado BOOLEAN DEFAULT FALSE
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS knr.pkmc_pk05 (
                partnumber VARCHAR,
                area_abastecimento VARCHAR,
                num_circ_regul_pkmc VARCHAR,
                tipo_deposito_pkmc VARCHAR,
                posicao_deposito_pkmc VARCHAR,
                container_pkmc VARCHAR,
                descricao_partnumber TEXT,
                norma_embalagem_pkmc VARCHAR,
                deposito_pk05 VARCHAR,
                responsavel_pk05 VARCHAR,
                ponto_descarga_pk05 VARCHAR,
                denominacao_pk05 VARCHAR,
                tacto VARCHAR,
                prateleira VARCHAR,
                saldo_lb FLOAT DEFAULT 2000,
                qtd_max_caixas FLOAT,
                qtd_total_teorica FLOAT,
                qtd_por_caixa FLOAT,
                qtd_para_reabastecimento FLOAT,
                criado_em DATE DEFAULT CURRENT_DATE
            );
        """)

        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_knrs_vivos ON knr.knrs_vivos(knr, knr_fx4pd)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_knrs_mortos ON knr.knrs_mortos(knr, knr_fx4pd)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_knrs_fx4pd ON knr.knrs_fx4pd(knr_fx4pd, partnumber)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pkmc_pk05 ON knr.pkmc_pk05(tacto, partnumber)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_lt22 ON knr.lt22(num_ot)")

        logger.debug("Todos os objetos do schema KNR garantidos")

    def insert_knrs_vivos(self, df_vivos: pl.DataFrame = pl.DataFrame()):       
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_vivos) > 0:
                    logger.debug("Inserindo KNRs vivos")
                    conn.register("df_vivos", df_vivos.to_arrow())
                    conn.execute("""
                        INSERT INTO knr.knrs_vivos(tma, cor, tst, knr, knr_fx4pd, tmamg, cod_pais, pais, modelo, criado_em)
                        SELECT tma, cor, tst, knr, knr_fx4pd, tmamg, cod_pais, pais, modelo, criado_em
                        FROM df_vivos
                        ON CONFLICT(knr,knr_fx4pd) 
                        DO UPDATE SET 
                            tma = EXCLUDED.tma, 
                            cor = EXCLUDED.cor, 
                            tst = EXCLUDED.tst,  
                            tmamg = EXCLUDED.tmamg, 
                            cod_pais = EXCLUDED.cod_pais, 
                            pais = EXCLUDED.pais, 
                            modelo = EXCLUDED.modelo,
                            criado_em = EXCLUDED.criado_em
                    """)
                    conn.unregister("df_vivos")
                    conn.commit()
                    logger.info(f"{len(df_vivos)} KNRs vivos inseridos/atualizados")
            except Exception as e:
                logger.error(f"Falha ao inserir KNRs: {e}")
                raise
            
    def insert_knrs_mortos(self, df_mortos: pl.DataFrame = pl.DataFrame()):       
        with self.db_helper.get_connection() as conn:
            try:
                 if len(df_mortos) > 0:
                    logger.debug("Inserindo KNRs mortos")
                    conn.register("df_mortos", df_mortos.to_arrow())
                    conn.execute("""
                        INSERT INTO knr.knrs_mortos(tma, cor, tst, knr, knr_fx4pd, tmamg, cod_pais, pais, modelo, criado_em)
                        SELECT * FROM df_mortos
                        ON CONFLICT(knr,knr_fx4pd) 
                        DO UPDATE SET 
                            tma = EXCLUDED.tma, 
                            cor = EXCLUDED.cor, 
                            tst = EXCLUDED.tst,  
                            tmamg = EXCLUDED.tmamg, 
                            cod_pais = EXCLUDED.cod_pais, 
                            pais = EXCLUDED.pais, 
                            modelo = EXCLUDED.modelo,
                            criado_em = EXCLUDED.criado_em
                    """)
                    conn.unregister("df_mortos")
                    conn.commit()
                    logger.info(f"{len(df_mortos)} KNRs mortos inseridos/atualizados")
            except Exception as e:
                logger.error(f"Falha ao inserir KNRs: {e}")
                raise

    def insert_knrs_fx4pd(self, df_knrs_fx4pd: pl.DataFrame = pl.DataFrame()):
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_knrs_fx4pd) > 0:
                    logger.debug("Inserindo KNRs FX4PD")
                    conn.register("df_knrs_fx4pd", df_knrs_fx4pd.to_arrow())
                    conn.execute("""
                        INSERT INTO knr.knrs_fx4pd(knr_fx4pd, partnumber, quantidade, quantidade_unidade)
                        SELECT PON_Kennnummer, PartCode_Sachnummer, Quantity_Menge, QuantityUnit_Mengeneinheit
                        FROM df_knrs_fx4pd
                        ON CONFLICT (knr_fx4pd, partnumber)
                        DO UPDATE SET
                            quantidade = EXCLUDED.quantidade, 
                            quantidade_unidade = EXCLUDED.quantidade_unidade,
                            criado_em = EXCLUDED.criado_em
                    """)
                    conn.unregister("df_knrs_fx4pd")
                    conn.commit()
                    logger.info(f"{len(df_knrs_fx4pd)} KNRs FX4PD inseridos/atualizados")
            except Exception as e:
                logger.error(f"Falha ao inserir KNRs FX4PD: {e}")
                raise

    def insert_knrs_comuns(self, df_common_concatenated_values: pl.DataFrame):
        try:
            total_rows = len(df_common_concatenated_values)
            logger.info(f"Carregando {total_rows:,} registros em 'knrs_comum' via DataFrame em memória")

            with self.db_helper.get_connection() as conn:
                conn.register("df_common", df_common_concatenated_values.to_arrow())

                conn.execute("""
                    CREATE OR REPLACE TABLE knr.knrs_comum AS
                    SELECT 
                        knr, knr_fx4pd, cor, tmamg, cod_pais, pais,
                        modelo, partnumber, quantidade, quantidade_unidade
                    FROM df_common
                """)

                conn.unregister("df_common")
                conn.commit()
                logger.info("'knrs_comum' recarregada com sucesso via DataFrame em memória")

        except Exception as e:
            logger.error(f"Falha ao inserir em 'knrs_comum': {e}", exc_info=True)
            raise
        
    def insert_pkmc_pk05(self, df_joined: pl.DataFrame = pl.DataFrame()):
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_joined) > 0:
                    logger.debug("Inserindo registros em 'pkmc_pk05'")
                    conn.register("df_joined", df_joined.to_arrow())
                    conn.execute("""
                        INSERT INTO knr.pkmc_pk05(
                            partnumber, area_abastecimento, num_circ_regul_pkmc, tipo_deposito_pkmc, 
                            posicao_deposito_pkmc, container_pkmc, descricao_partnumber, norma_embalagem_pkmc,
                            qtd_por_caixa, qtd_max_caixas, deposito_pk05, responsavel_pk05, ponto_descarga_pk05, 
                            denominacao_pk05, tacto, prateleira, qtd_total_teorica, qtd_para_reabastecimento
                        ) 
                        SELECT * FROM df_joined
                        ON CONFLICT(tacto, partnumber) DO NOTHING
                    """)
                    conn.unregister("df_joined")
                    conn.commit()
                    logger.info(f"{len(df_joined)} registros inseridos em 'pkmc_pk05'")
            except Exception as e:
                logger.error(f"Falha ao inserir em 'pkmc_pk05': {e}")
                raise
                
    def update_pkmc_pk05(self, df_update_pkmc_pk05: pl.DataFrame = pl.DataFrame()):
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_update_pkmc_pk05) > 0:
                    logger.debug("Atualizando saldos em 'pkmc_pk05'")
                    conn.register("df_update_pkmc_pk05", df_update_pkmc_pk05.to_arrow())
                    conn.execute("""
                        UPDATE knr.pkmc_pk05
                        SET saldo_lb = updt.quantidade_final
                        FROM df_update_pkmc_pk05 updt
                        WHERE knr.pkmc_pk05.tacto = updt.tacto 
                        AND knr.pkmc_pk05.partnumber = updt.partnumber
                    """)
                    conn.unregister("df_update_pkmc_pk05")
                    conn.commit()
                    logger.info(f"{len(df_update_pkmc_pk05)} registros atualizados em 'pkmc_pk05'")
            except Exception as e:
                logger.error(f"Falha ao atualizar 'pkmc_pk05': {e}")
                raise

    def insert_lt22(self, df_lt22: pl.DataFrame = pl.DataFrame()):
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_lt22) > 0:
                    logger.debug("Inserindo registros em 'lt22'")
                    conn.register("df_lt22", df_lt22.to_arrow())
                    conn.execute("""
                        INSERT INTO knr.lt22(num_ot, partnumber, tp_destino, posicao_destino, quantidade, unidade_deposito, usuario, prateleira, data_confirmacao, hora_confirmacao)
                        SELECT * FROM df_lt22
                        ON CONFLICT(num_ot)
                        DO UPDATE SET data_confirmacao = EXCLUDED.data_confirmacao,
                                    hora_confirmacao = EXCLUDED.hora_confirmacao
                    """)
                    conn.unregister("df_lt22")
                    conn.commit()
                    logger.info(f"{len(df_lt22)} registros inseridos/atualizados em 'lt22'")
            except Exception as e:
                logger.error(f"Falha ao inserir em 'lt22': {e}")
                raise

    def update_lt22(self, df_update_lt22: pl.DataFrame = pl.DataFrame()):
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_update_lt22) > 0:
                    logger.debug("Atualizando registros em 'lt22' como utilizados")
                    conn.register("df_update_lt22", df_update_lt22.to_arrow())
                    conn.execute("""
                        UPDATE knr.lt22
                        SET num_ot_usado = TRUE
                        WHERE num_ot IN (
                            SELECT num_ot FROM df_update_lt22
                            WHERE num_ot_usado IS FALSE
                        )
                    """)
                    conn.unregister("df_update_lt22")
                    conn.commit()
                    logger.info(f"{len(df_update_lt22)} registros atualizados em 'lt22' como utilizados")
            except Exception as e:
                logger.error(f"Falha ao atualizar 'lt22': {e}")
                raise


db_manager_knr = DatabaseManagerKNR()