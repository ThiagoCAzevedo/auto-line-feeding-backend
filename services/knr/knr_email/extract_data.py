from backend.database import db_manager_general, db_manager_knr
from typing import Dict, Optional
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from backend.log import log_files
import polars as pl
import sys, os


logger = log_files.get_logger("knr | infos_knr_email | extract_data", "knr")

class KNRsManager:
    COLUMN_MAPPING = {
        "TMA": "tma",
        "COR": "cor",
        "TST": "tst",
        "KNR": "knr",
        "KNR/FX4": "knr_fx4pd",
        "TMAMG": "tmamg",
        "CÓD.PAÍS": "cod_pais",
        "PAÍS": "pais",
        "MODELO": "modelo",
    }

    FINAL_COLUMNS = [
        "tma", "cor", "tst", "knr", "knr_fx4pd",
        "tmamg", "cod_pais", "pais", "modelo", "criado_em"
    ]

    def __init__(self):
        load_dotenv()
        
        self.db_manager = db_manager_general
        self.db_manager_knr = db_manager_knr
        self.path_file = Path(os.getenv("VALORES_COMPLETOS_EMAIL_PATH"))
        
        self.columns_of_interest = tuple(self.COLUMN_MAPPING.keys())
        
        self._initialize_stats()
        self._initialize_data_structures()
        
        logger.info("Inicializando KNRsManager")
        self._load_db_data()
        logger.info("KNRsManager inicializado com sucesso")

    def _initialize_stats(self):
        self.stats = {
            "sheets_read": 0,
            "records_processed": 0,
            "knrs_found": 0,
            "knrs_new": 0,
            "knrs_to_move": 0,
            "knrs_to_reactivate": 0,
            "errors": 0,
            "start_time": datetime.now(),
        }

    def _initialize_data_structures(self):
        self.df_consolidated = None
        self.actual_knrs = set()
        self.raw_sheets = {}
        
        self.knrs_db_alive = set()
        self.knrs_db_dead = set()
        self.dict_db_alive = {}
        self.dict_db_dead = {}

    def _load_db_data(self):
        try:
            logger.info("Buscando dados de KNR no banco de dados")
            
            query = """
                SELECT tma, cor, tst, knr, knr_fx4pd, tmamg, cod_pais, pais, modelo, 'vivo' as tipo 
                FROM knr.knrs_vivos
                UNION ALL
                SELECT tma, cor, tst, knr, knr_fx4pd, tmamg, cod_pais, pais, modelo, 'morto' as tipo 
                FROM knr.knrs_mortos
            """
            
            records = self.db_manager.execute_query(query) or []
            logger.debug(f"Recuperados {len(records)} registros do banco")

            db_knrs_alive = [r for r in records if r["tipo"] == "vivo"]
            db_knrs_dead = [r for r in records if r["tipo"] == "morto"]

            self.knrs_db_alive = {str(r["knr"]) for r in db_knrs_alive}
            self.knrs_db_dead = {str(r["knr"]) for r in db_knrs_dead}
            self.dict_db_alive = {str(r["knr"]): r for r in db_knrs_alive}
            self.dict_db_dead = {str(r["knr"]): r for r in db_knrs_dead}

            logger.info(
                f"Snapshot do BD: {len(self.knrs_db_alive)} KNRs ativos, "
                f"{len(self.knrs_db_dead)} KNRs inativos"
            )

        except Exception as e:
            logger.error(f"Falha ao carregar KNRs do BD: {e}", exc_info=True)
            self._initialize_data_structures()
            self.stats["errors"] += 1

    def run_pipeline(self):
        try:
            logger.info("Iniciando pipeline")

            if not self._read_excel_file():
                logger.error("Falha ao ler arquivo Excel")
                return

            self._consolidate_sheets()
            self._extract_classify_knrs()
            self._save_data()

            logger.info("Pipeline concluído com sucesso")

        except Exception as e:
            logger.critical(f"Erro fatal no pipeline: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise

    def _read_excel_file(self) -> bool:
        logger.info(f"Lendo arquivo Excel: {self.path_file}")
        
        try:
            for sheet_id in range(2):
                try:
                    df = self._read_sheet_by_id(sheet_id)
                    
                    if df is not None and not df.is_empty():
                        sheet_name = f"Sheet_{sheet_id + 1}"
                        self.raw_sheets[sheet_name] = df
                        self.stats["sheets_read"] += 1
                        logger.info(f"Planilha {sheet_id + 1} lida: {len(df)} linhas")
                    else:
                        logger.warning(f"Planilha {sheet_id + 1} vazia ou inválida")
                        
                except Exception as e:
                    if "sheet" in str(e).lower() or "index" in str(e).lower():
                        logger.debug(f"Planilha {sheet_id + 1} não existe, parando leitura")
                        break
                    logger.error(f"Erro ao ler planilha {sheet_id + 1}: {e}", exc_info=True)
                    self.stats["errors"] += 1

            success = bool(self.raw_sheets)
            
            if success:
                logger.info(f"Leitura concluída: {len(self.raw_sheets)} planilha(s)")
            else:
                logger.warning("Nenhuma planilha válida foi lida")

            return success

        except Exception as e:
            logger.error(f"Falha ao ler arquivo Excel: {e}", exc_info=True)
            self.stats["errors"] += 1
            return False

    def _read_sheet_by_id(self, sheet_id: int) -> Optional[pl.DataFrame]:
        try:
            logger.debug(f"Lendo planilha com ID: {sheet_id}")
            
            result = pl.read_excel(
                self.path_file,
                sheet_id=sheet_id + 1,
                engine="calamine"
            )

            df = next(iter(result.values())) if isinstance(result, dict) else result

            if len(df) < 2:
                logger.debug(f"Planilha {sheet_id} tem poucas linhas ({len(df)})")
                return None

            df = self._process_headers(df)
            logger.debug(f"Planilha {sheet_id} processada: {len(df)} linhas")
            
            return df

        except Exception as e:
            logger.error(f"Erro ao ler planilha {sheet_id}: {e}", exc_info=True)
            raise

    def _process_headers(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Processando cabeçalhos")

        row0 = df.row(0)
        row1 = df.row(1) if df.height > 1 else None

        def is_valid_row(row):
            return any(x and str(x).strip() and str(x).lower() != "null" for x in row)

        headers = row1 if (row1 and is_valid_row(row1)) else row0
        start_from = 2 if headers is row1 else 1

        new_names = [
            str(header).strip() if header and str(header).strip() and str(header).lower() != "null"
            else f"Coluna_{i}"
            for i, header in enumerate(headers)
        ]

        df.columns = new_names
        logger.debug(f"Cabeçalhos processados - Iniciando da linha {start_from}")
        
        return df.slice(start_from)

    def _consolidate_sheets(self):
        if not self.raw_sheets:
            logger.warning("Nenhuma planilha disponível para consolidar")
            return

        logger.info(f"Consolidando {len(self.raw_sheets)} planilha(s)")

        lazy_frames = []
        columns = list(self.columns_of_interest)

        for sheet_name, df in self.raw_sheets.items():
            try:
                existing_columns = [col for col in columns if col in df.columns]

                if not existing_columns:
                    logger.warning(f"Planilha '{sheet_name}' não tem colunas de interesse")
                    continue

                logger.debug(f"Processando '{sheet_name}' com colunas: {existing_columns}")

                lazy_df = df.lazy().select(existing_columns).drop_nulls()
                lazy_frames.append(lazy_df)

            except Exception as e:
                logger.error(f"Erro ao processar planilha '{sheet_name}': {e}", exc_info=True)
                self.stats["errors"] += 1

        if lazy_frames:
            self.df_consolidated = (
                pl.concat(lazy_frames, how="diagonal")
                .unique(subset=["KNR"], keep="first")
                .collect()
            )

            self.stats["records_processed"] = len(self.df_consolidated)
            logger.info(f"Consolidação concluída - {self.stats['records_processed']} registros únicos")
        else:
            self.df_consolidated = pl.DataFrame()
            logger.warning("Nenhum dado para consolidar")

    def _extract_classify_knrs(self):
        if self.df_consolidated is None or self.df_consolidated.is_empty():
            logger.warning("Nenhum dado consolidado para processar")
            return

        if "KNR" not in self.df_consolidated.columns:
            logger.error(f"Coluna KNR não encontrada. Colunas disponíveis: {self.df_consolidated.columns}")
            return

        logger.info("Extraindo e classificando KNRs")

        self.actual_knrs = {
            str(knr)
            for knr in self.df_consolidated["KNR"].drop_nulls().unique().to_list()
            if knr is not None
        }

        self.stats["knrs_found"] = len(self.actual_knrs)
        logger.info(f"Encontrados {self.stats['knrs_found']} KNRs únicos nos dados atuais")

        self.knrs_to_move = self.knrs_db_alive - self.actual_knrs
        self.new_knrs = self.actual_knrs - self.knrs_db_alive - self.knrs_db_dead
        self.knrs_to_reactivate = self.actual_knrs & self.knrs_db_dead

        self.stats["knrs_new"] = len(self.new_knrs)
        self.stats["knrs_to_move"] = len(self.knrs_to_move)
        self.stats["knrs_to_reactivate"] = len(self.knrs_to_reactivate)

        logger.info(f"Classificação concluída:")
        logger.info(f"  - Novos KNRs: {self.stats['knrs_new']}")
        logger.info(f"  - KNRs para desativar: {self.stats['knrs_to_move']}")
        logger.info(f"  - KNRs para reativar: {self.stats['knrs_to_reactivate']}")

    def _save_data(self):
        try:
            logger.info("Preparando dados para inserção no banco")

            df_alive = self._prepare_alive_records()
            df_dead = self._prepare_dead_records()

            logger.info(
                f"Preparados {len(df_alive) if df_alive is not None else 0} vivos e "
                f"{len(df_dead) if df_dead is not None else 0} mortos"
            )

            self._execute_db_transaction(df_alive, df_dead)

            logger.info("Dados salvos com sucesso no banco")

        except Exception as e:
            logger.error(f"Falha ao salvar dados: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise

    def _prepare_alive_records(self) -> Optional[pl.DataFrame]:
        if self.df_consolidated is None or self.df_consolidated.is_empty():
            logger.debug("Nenhum registro vivo para preparar")
            return None

        logger.debug("Preparando registros vivos (novos KNRs)")

        df = self.df_consolidated.filter(
            pl.col("KNR").cast(pl.Utf8).is_in(list(self.new_knrs))
        )

        if df.is_empty():
            logger.debug("Nenhum KNR novo para adicionar")
            return None

        df = self._apply_column_mapping_and_date(df)
        logger.debug(f"Preparados {len(df)} registros vivos (novos)")
        
        return df

    def _prepare_dead_records(self) -> Optional[pl.DataFrame]:
        if not self.knrs_to_move:
            logger.debug("Nenhum registro morto para preparar")
            return None

        logger.debug(f"Preparando {len(self.knrs_to_move)} registros mortos")

        current_date = datetime.now().date()
        dead_records = []

        for knr in self.knrs_to_move:
            if knr in self.dict_db_alive:
                record = self.dict_db_alive[knr].copy()
                record["criado_em"] = current_date
                record.pop("tipo", None)
                dead_records.append(record)
            else:
                logger.warning(f"KNR {knr} está em knrs_to_move mas não em dict_db_alive")

        if not dead_records:
            logger.warning("Nenhum registro morto válido encontrado")
            return None

        df = pl.DataFrame(dead_records)
        existing_columns = [c for c in self.FINAL_COLUMNS if c in df.columns]
        df = df.select(existing_columns)

        logger.debug(f"Preparados {len(df)} registros mortos")
        return df

    def _prepare_reactivate_records(self) -> Optional[pl.DataFrame]:
        if not self.knrs_to_reactivate:
            logger.debug("Nenhum registro para reativar")
            return None

        logger.debug(f"Preparando {len(self.knrs_to_reactivate)} registros para reativação")

        df = self.df_consolidated.filter(
            pl.col("KNR").cast(pl.Utf8).is_in(list(self.knrs_to_reactivate))
        )

        if df.is_empty():
            logger.warning("Nenhum registro para reativar encontrado no arquivo")
            return None

        df = self._apply_column_mapping_and_date(df)
        logger.debug(f"Preparados {len(df)} registros para reativação")
        
        return df

    def _apply_column_mapping_and_date(self, df: pl.DataFrame) -> pl.DataFrame:
        for old, new in self.COLUMN_MAPPING.items():
            if old in df.columns:
                df = df.rename({old: new})

        df = df.with_columns([pl.lit(datetime.now().date()).alias("criado_em")])
        
        existing_columns = [c for c in self.FINAL_COLUMNS if c in df.columns]
        return df.select(existing_columns)

    def _execute_db_transaction(self, df_alive: Optional[pl.DataFrame], df_dead: Optional[pl.DataFrame]):
        try:
            logger.info("Iniciando transação no banco de dados")

            logger.debug(f"df_alive (novos): {len(df_alive) if df_alive is not None else 0} registros")
            logger.debug(f"df_dead: {len(df_dead) if df_dead is not None else 0} registros")
            logger.debug(f"knrs_to_reactivate: {len(self.knrs_to_reactivate)} registros")

            if df_dead is not None and len(df_dead) > 0:
                knrs_to_deactivate = df_dead["knr"].cast(pl.Utf8).to_list()
                logger.info(f"Desativando {len(knrs_to_deactivate)} KNRs")
                
                placeholders = ', '.join(['?'] * len(knrs_to_deactivate))
                delete_query = f"DELETE FROM knr.knrs_vivos WHERE knr IN ({placeholders})"
                
                self.db_manager.execute_query(delete_query, tuple(knrs_to_deactivate))
                logger.info(f"Removidos {len(knrs_to_deactivate)} KNRs da tabela vivos")
                
                self.db_manager_knr.insert_knrs_mortos(df_dead)
                logger.info(f"Inseridos {len(df_dead)} KNRs na tabela mortos")

            if self.knrs_to_reactivate:
                df_reactivate = self._prepare_reactivate_records()
                
                if df_reactivate is not None and len(df_reactivate) > 0:
                    knrs_to_reactivate_list = list(self.knrs_to_reactivate)
                    logger.info(f"Reativando {len(knrs_to_reactivate_list)} KNRs")
                    
                    placeholders = ', '.join(['?'] * len(knrs_to_reactivate_list))
                    delete_query = f"DELETE FROM knr.knrs_mortos WHERE knr IN ({placeholders})"
                    
                    self.db_manager.execute_query(delete_query, tuple(knrs_to_reactivate_list))
                    logger.info(f"Removidos {len(knrs_to_reactivate_list)} KNRs da tabela mortos")
                    
                    df_alive = pl.concat([df_alive, df_reactivate]) if df_alive is not None else df_reactivate

            if df_alive is not None and len(df_alive) > 0:
                logger.info(f"Inserindo {len(df_alive)} registros vivos (novos + reativados)")
                self.db_manager_knr.insert_knrs_vivos(df_alive)
                logger.info("Registros vivos inseridos com sucesso")

            logger.info("Transação concluída com sucesso")

        except Exception as e:
            logger.error(f"Falha na transação do banco: {e}", exc_info=True)
            self.stats["errors"] += 1
            raise

    def get_statistics(self) -> Dict:
        elapsed_time = (datetime.now() - self.stats["start_time"]).total_seconds()
        return {
            **self.stats,
            "execution_time": elapsed_time,
            "success": self.stats["errors"] == 0,
        }


def main():
    try:
        logger.info("Iniciando execução standalone do KNR Manager")
        manager = KNRsManager()
        manager.run_pipeline()

        stats = manager.get_statistics()

        if stats.get("success"):
            logger.info("Processamento de KNR concluído com sucesso")
        else:
            logger.warning(f"Processamento concluído com {stats['errors']} erro(s)")
    except Exception as e:
        logger.critical(f"Erro fatal na execução principal: {e}", exc_info=True)
    finally:
        logger.info("Execução do KNR Manager finalizada")


if __name__ == "__main__":
    main()