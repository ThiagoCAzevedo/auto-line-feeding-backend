from .db_manager_knr import db_manager_knr
from .db_manager_lines import db_manager_lines
from typing import List, Dict, Any
from .utils import db_helper
from backend.log import log_files
import threading
import duckdb


class DatabaseManagerGeneral:
    _lock = threading.Lock()

    def __init__(self):
        self.db_manager_knr = db_manager_knr
        self.db_manager_lines = db_manager_lines
        self.db_helper = db_helper
        self.logger = log_files.get_logger('database | db_manager | db_manager_general', 'db')
        self._ensure_database_exists()

    def _ensure_database_exists(self):
        try:
            self.logger.debug("Verificando e criando schemas no banco de dados, se necessário")
            with self.db_helper.get_connection() as conn:
                conn.execute("CREATE SCHEMA IF NOT EXISTS linha")
                conn.execute("CREATE SCHEMA IF NOT EXISTS knr")
                conn.commit()
                self.logger.info("Schemas criados no banco de dados")
                self.logger.debug("Criando tabelas de KNR, Sequenciamento e Linha")
                self.db_manager_knr.create_knr_tables()
                self.db_manager_lines.create_linha_tables()
                
                self.logger.info("Estrutura do banco de dados pronta")
        except Exception as e:
            self.logger.error(f"Falha ao configurar a estrutura do banco de dados: {e}")
            raise

    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        try:
            with DatabaseManagerGeneral._lock: 
                self.logger.debug(f"Executando consulta: {query} | Parâmetros: {params}")
                with self.db_helper.get_connection() as conn:
                    result = conn.execute(query, params).fetchall()
                    if result:
                        columns = [desc[0] for desc in conn.description]
                        self.logger.info(f"Consulta retornou {len(result)} linha(s)")
                        return [dict(zip(columns, row)) for row in result]
                    self.logger.info("Consulta não retornou resultados")
                    return []
        except duckdb.TransactionException as e:
            if "write-write conflict" in str(e):
                self.logger.warning(f"Conflito de escrita detectado no execute_query")
            self.logger.error(f"Erro na execução da consulta: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"Erro na execução da consulta: {e}", exc_info=True)
            raise


db_manager_general = DatabaseManagerGeneral()