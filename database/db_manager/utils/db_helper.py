from contextlib import contextmanager
from dotenv import load_dotenv
from backend.log import log_files
from pathlib import Path
import duckdb, os


class DBHelper:
    load_dotenv()
    
    def __init__(self):
        self.db_path = Path(os.getenv("DB_PATH"))
        self._conn = None
        self.already_applied_configs = False
        self.logger = log_files.get_logger('database | db_manager | utils | db_helper', 'db')

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            self.logger.debug("Abrindo conexão com DuckDB...")
            conn = duckdb.connect(str(self.db_path))
            if not self.already_applied_configs:
                self.configs_db(conn)
                self.already_applied_configs = True
            yield conn
        except Exception as e:
            self.logger.error(f"Erro ao abrir conexão com DuckDB: {e}")
            raise
        finally:
            if conn:
                self.logger.debug("Fechando conexão com DuckDB...")
                conn.close()

    def configs_db(self, conn):
        try:
            self.logger.debug("Aplicando configurações no banco DuckDB...")
            conn.execute("SET memory_limit = '10GB'")
            conn.execute("SET max_memory = '20GB'")
            conn.execute("SET threads = 12")
            conn.execute("SET default_block_size = '262144'")

            conn.execute("PRAGMA disable_progress_bar")
            conn.execute("PRAGMA disable_print_progress_bar")
            conn.execute("PRAGMA enable_optimizer")
            conn.execute("PRAGMA enable_object_cache")
            conn.execute("PRAGMA force_checkpoint")

            self.logger.info("Configurações aplicadas com sucesso.")
        except Exception as e:
            self.logger.loggerer.error(f"Erro ao configurar o banco DuckDB: {e}")
            raise


db_helper = DBHelper()