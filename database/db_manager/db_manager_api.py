from typing import Dict, Any, Optional
from datetime import datetime, date
from .utils import db_helper
from dotenv import load_dotenv
from backend.log import log_files
from pathlib import Path
import os


class DatabaseManagerApi:
    load_dotenv()
        
    def __init__(self):
        self.actual_date = date.today()
        self.db_helper = db_helper
        self.logger = log_files.get_logger('database | db_manager | db_manager_api', 'db')
        self.log_knr_path = Path(os.getenv("KNR_LOG_PATH"))
        self.log_linha_path = Path(os.getenv("LINES_LOG_PATH"))

    def read_log_file(self, log_system: str, lines: int = 25):
        log_knr_path = self.log_knr_path
        log_linha_path = self.log_linha_path

        def _call_lines_reader(log_path):
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as file:
                all_lines = file.readlines()
                filtered_lines = [line.strip() for line in all_lines if line.strip()]
                return filtered_lines[-lines:]

        try:
            if log_system == "knr":
                return _call_lines_reader(log_knr_path)
            elif log_system == "linhas":
                return _call_lines_reader(log_linha_path)
            else:
                self.logger.warning("Erro ao ler algum log")
                return []
        except Exception as e:
            self.logger.warning(f"Erro geral na leitura dos logs: {e}")
            return []

    def get_by_tacto(self, tacto: Optional[str] = None) -> Dict[str, Any]:
        try:
            self.logger.debug(f"Fetching shelves grouped by tacto: {tacto if tacto else 'ALL'}")

            with self.db_helper.get_connection() as conn:
                if tacto:
                    query = """
                        SELECT 
                            tacto,
                            prateleira,
                            saldo_lb,
                            partnumber
                        FROM knr.pkmc_pk05
                        WHERE tacto = ?
                        ORDER BY saldo_lb ASC
                    """
                    rows = conn.execute(query, (tacto,)).fetchall()
                else:
                    query = """
                        SELECT 
                            tacto,
                            prateleira,
                            saldo_lb,
                            partnumber
                        FROM knr.pkmc_pk05
                        WHERE tacto IS NOT NULL
                        ORDER BY saldo_lb ASC
                    """
                    rows = conn.execute(query).fetchall()

                tactos_dict = {}
                for row in rows:
                    tacto_key = row[0] or "N/A"
                    if tacto_key not in tactos_dict:
                        tactos_dict[tacto_key] = {
                            "tacto": tacto_key,
                            "prateleiras": []
                        }
                    tactos_dict[tacto_key]["prateleiras"].append({
                        "prateleira": row[1] or "N/A",
                        "saldo_lb": float(row[2]) if row[2] else 0,
                        "partnumber": row[3] or "N/A"
                    })

                return {
                    "tactos": list(tactos_dict.values()),
                    "timestamp": datetime.now().isoformat()
                }

        except Exception as e:
            self.logger.error(f"Erro ao agrupar prateleiras por tacto: {e}")
            return {"tactos": [], "error": str(e)}

    def get_by_prateleira(self, prateleira: Optional[str]) -> Dict[str, Any]:
        try:
            self.logger.debug(f"Fetching details for shelf: {prateleira if prateleira else 'ALL'}")
            
            with self.db_helper.get_connection() as conn:
                if prateleira:
                    query = """
                        SELECT 
                            tacto,
                            saldo_lb,
                            partnumber
                        FROM knr.pkmc_pk05
                        WHERE prateleira = ?
                        ORDER BY saldo_lb
                    """
                    rows = conn.execute(query, (prateleira,)).fetchall()
                else:
                    query = """
                        SELECT 
                            prateleira,
                            tacto,
                            saldo_lb,
                            partnumber
                        FROM knr.pkmc_pk05
                        ORDER BY saldo_lb
                    """
                    rows = conn.execute(query).fetchall()

                if not rows:
                    return {"error": "Nenhuma prateleira encontrada"}

                if prateleira:
                    shelf_data = {
                        "prateleira": prateleira,
                        "tacto": rows[0][0] if rows else "N/A",
                        "parts": []
                    }
                    for row in rows:
                        if row[2]:
                            shelf_data["parts"].append({
                                "partnumber": row[2],
                                "saldo_lb": float(row[1]) if row[1] else 0,
                            })
                    return shelf_data
                else:
                    all_shelves = {}
                    for row in rows:
                        prateleira_nome = row[0] or "N/A"
                        if prateleira_nome not in all_shelves:
                            all_shelves[prateleira_nome] = {
                                "prateleira": prateleira_nome,
                                "tacto": row[1] or "N/A",
                                "parts": []
                            }
                        if row[3]:
                            all_shelves[prateleira_nome]["parts"].append({
                                "partnumber": row[3],
                                "saldo_lb": float(row[2]) if row[2] else 0,
                            })
                    return {
                        "prateleiras": list(all_shelves.values()),
                        "timestamp": datetime.now().isoformat()
                    }

        except Exception as e:
            self.logger.error(f"Failed fetching shelf details: {e}")
            return {"error": str(e)}
        
    def get_lt22_stil_opened(self):
        try:
            with self.db_helper.get_connection() as conn:
                rows = conn.execute("""
                    SELECT 
                        num_ot,
                        partnumber,
                        quantidade,
                        usuario,
                        prateleira
                    FROM knr.lt22
                    WHERE num_ot_usado is FALSE
                """).fetchall()
                
                infos = []
                for row in rows:
                    infos.append({
                        "num_ot": row[0],
                        "partnumber": row[1],
                        "quantidade": row[2],
                        "usuario": row[3],
                        "prateleira": row[4]
                    })

                return {
                    "abertas": infos,
                    "timestamp": datetime.now().isoformat(),
                }
        except Exception as e:
            self.logger.error(f"Failed testing: {e}")
            return {"abertas": [], "error": str(e)}


    # --- JÁ EXISTENTES ---
    def get_dashboard_data(self) -> Dict[str, Any]:
        try:
            # self.logger.debug("Buscando dados para partnumber 2G5827550B")
            with self.db_helper.get_connection() as conn:
                rows = conn.execute("""
                    SELECT 
                        tacto,
                        saldo_lb,
                        partnumber,
                        prateleira
                    FROM knr.pkmc_pk05
                    WHERE prateleira = 'P31B'
                """).fetchall()

                dados_partnumber = []

                for row in rows:
                    dados_partnumber.append({
                        "tacto": row[0],
                        "saldo_lb": row[1] if row[1] is not None else 0,
                        "partnumber": row[2],
                        "prateleira": row[3],
                    })

                # self.logger.info(f"Dados do partnumber 2G5827550B recuperados: {len(dados_partnumber)} registros")
                return {
                    "prateleiras": {
                        "P31B": dados_partnumber
                    },
                    "timestamp": datetime.now().isoformat(),
                }
        except Exception as e:
            self.logger.error(f"Erro ao buscar dados do partnumber 2G5827550B: {e}", exc_info=True)
            return {"prateleiras": "P31B", "dados": [], "error": str(e)}
        

db_manager_api = DatabaseManagerApi()