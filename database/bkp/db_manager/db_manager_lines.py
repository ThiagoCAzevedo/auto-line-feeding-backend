from backend.log import log_files
from .utils import db_helper
import polars as pl


logger = log_files.get_logger('database | db_manager | db_manager_linha', 'db')

class DatabaseManagerLinhas:
    def __init__(self):
        self.db_helper = db_helper

    def create_linha_tables(self):
        try:
            logger.debug("Verificando e criando schemas no banco de dados, se necessário")
            with self.db_helper.get_connection() as conn:
                conn.execute("CREATE SCHEMA IF NOT EXISTS linha")
                self._create_linhas_tables(conn)
                conn.commit()
                logger.info("Tabelas do schema 'linha' criadas com sucesso")
        except Exception as e:
            logger.error(f"Falha ao criar tabelas do schema 'linha': {e}")
            raise

    def _create_linhas_tables(self, conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS linha.linha_montagem (
                tacto VARCHAR NOT NULL,
                knr VARCHAR,
                lfdnr_sequencia VARCHAR,
                modelo VARCHAR,
                lane TEXT,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS linha.linha_pintura (
                knr VARCHAR,
                sequencia_prevista VARCHAR,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS linha.saldo_lb (
                tacto VARCHAR,
                knr VARCHAR,
                knr_fx4pd VARCHAR,
                partnumber VARCHAR,
                quantidade FLOAT,
                quantidade_unidade VARCHAR,
                atualizado_em DATE DEFAULT CURRENT_DATE
            );
        """)

        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_linha_montagem_knr ON linha.linha_montagem(knr, lfdnr_sequencia)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pintura ON linha.linha_pintura(knr)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_saldo_lb ON linha.saldo_lb(tacto, partnumber)")

        logger.debug("Todos os objetos do schema 'linha' garantidos")

    def insert_update_infos_linha_montagem(self, df_infos_linha_montagem: pl.DataFrame) -> None:
        try:
            logger.debug(f"Inserindo/Atualizando {len(df_infos_linha_montagem)} linhas em 'linha_montagem'")
            with self.db_helper.get_connection() as conn:
                conn.register("df_infos_linha_montagem", df_infos_linha_montagem.to_arrow())
                conn.execute("""
                    INSERT INTO linha.linha_montagem(tacto, knr, lfdnr_sequencia, modelo, lane)
                    SELECT tacto, knr, lfdnr_sequencia, modelo, lane
                    FROM df_infos_linha_montagem
                    ON CONFLICT(knr, lfdnr_sequencia)
                    DO UPDATE SET
                        tacto = EXCLUDED.tacto,
                        lane = EXCLUDED.lane
                """)
                conn.commit()
                conn.unregister("df_infos_linha_montagem")
                logger.info("Dados da linha de montagem inseridos/atualizados com sucesso")
        except Exception as e:
            logger.error(f"Falha ao inserir/atualizar 'linha_montagem': {e}")
            raise

    def insert_update_infos_linha_pintura(self, df_infos_linha_pintura: pl.DataFrame) -> None:
        try:
            logger.debug(f"Inserindo/Atualizando {len(df_infos_linha_pintura)} linhas em 'linha_pintura'")
            with self.db_helper.get_connection() as conn:
                conn.register("df_infos_linha_pintura", df_infos_linha_pintura.to_arrow())
                conn.execute("""
                    INSERT INTO linha.linha_pintura(knr, sequencia_prevista)
                    SELECT knr, sequencia_prevista
                    FROM df_infos_linha_pintura
                    ON CONFLICT(knr)
                    DO UPDATE SET
                        sequencia_prevista = EXCLUDED.sequencia_prevista
                """)
                conn.commit()
                conn.unregister("df_infos_linha_pintura")
                logger.info("Dados da linha de pintura inseridos/atualizados com sucesso")
        except Exception as e:
            logger.error(f"Falha ao inserir/atualizar 'linha_pintura': {e}")
            raise

    def insert_saldo_lb(self, df_consumo_saldo: pl.DataFrame = pl.DataFrame()):
        with self.db_helper.get_connection() as conn:
            try:
                if len(df_consumo_saldo) > 0:
                    logger.debug(f"Inserindo/Atualizando {len(df_consumo_saldo)} linhas em 'saldo_lb'")
                    conn.register("df_consumo_saldo", df_consumo_saldo.to_arrow())
                    conn.execute("""
                        INSERT INTO linha.saldo_lb(tacto, knr, knr_fx4pd, partnumber, quantidade, quantidade_unidade) 
                        SELECT *
                        FROM df_consumo_saldo
                        ON CONFLICT(tacto, partnumber)
                        DO UPDATE SET
                            knr = EXCLUDED.knr,
                            knr_fx4pd = EXCLUDED.knr_fx4pd,
                            partnumber = EXCLUDED.partnumber,
                            quantidade = EXCLUDED.quantidade,
                            quantidade_unidade = EXCLUDED.quantidade_unidade
                    """)
                    conn.unregister("df_consumo_saldo")
                    conn.commit()
                    logger.info("Dados de saldo_lb inseridos/atualizados com sucesso")
            except Exception as e:
                logger.error(f"Falha ao inserir/atualizar 'saldo_lb': {e}")
                raise


db_manager_lines = DatabaseManagerLinhas()