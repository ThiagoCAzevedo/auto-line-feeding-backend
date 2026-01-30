from typing import Dict, Any, Optional
from backend.database import db_manager_knr
from dotenv import load_dotenv
import polars as pl, time, os
from datetime import datetime
from threading import Event
from backend.log import log_files
from pathlib import Path


logger = log_files.get_logger('knr | pkmc_pk05 | extract_data', 'knr')

class PKMCProcessor:
    load_dotenv()

    def __init__(self, master_stop_event: Optional[Event] = None):
        self.pkmc_path = Path(os.getenv("PKMC_PATH"))
        self.pk05_path = Path(os.getenv("PK05_PATH"))
        self.db_manager = db_manager_knr
        self.stop_event = master_stop_event or Event()
        self.df_pkmc = pl.DataFrame()
        self.df_pk05 = pl.DataFrame()
        self.df_joined = pl.DataFrame()
        self.max_retries = os.getenv("MAX_RETRIES")
        self.min_retries = os.getenv("MIN_RETRIES")
        self.time_between_retries = os.getenv("BETWEEN_RETRIES")

        logger.info(f"PKMCProcessor inicializado com PKMC={self.pkmc_path.name}, PK05={self.pk05_path.name}")

    def _validate_files(self) -> bool:
        ok = True

        if not self.pkmc_path.exists():
            for attempt in range(1, self.max_retries + 1):
                logger.warning(
                    f"Excel 'PKMC' não encontrado. Tentando novamente em {self.time_between_retries // 60} minutos... "
                    f"Tentativa número {attempt} / {self.max_retries}"
                )
                time.sleep(self.time_between_retries)
                if self.pkmc_path.exists():
                    logger.info(f"Excel 'PKMC' encontrado após {attempt} tentativa(s)")
                    break

            if not self.pkmc_path.exists():
                logger.critical(
                    f"Sistema será parado por completo. Excel 'PKMC' não foi encontrado após {self.max_retries} tentativas"
                )
                self.stop_event.set()
                ok = False

        else:
            logger.info("Arquivo PKMC encontrado")

        if not self.pk05_path.exists():
            for attempt in range(1, self.max_retries + 1):
                logger.warning(
                    f"Excel 'PK05' não encontrado. Tentando novamente em {self.time_between_retries // 60} minutos... "
                    f"Tentativa número {attempt} / {self.max_retries}"
                )
                time.sleep(self.time_between_retries)
                if self.pk05_path.exists():
                    logger.info(f"Excel 'PK05' encontrado após {attempt} tentativa(s)")
                    break

            if not self.pk05_path.exists():
                logger.critical(
                    f"Sistema será parado por completo. Excel 'PK05' não foi encontrado após {self.max_retries} tentativas"
                )
                self.stop_event.set()
                ok = False
        else:
            logger.info("Arquivo PK05 encontrado")

        return ok

    def read_pkmc(self) -> pl.DataFrame:
        logger.info(f"Lendo arquivo PKMC: {self.pkmc_path.name}")
        df = pl.read_excel(self.pkmc_path, engine="calamine")
        logger.info(f"Carregadas {len(df)} linhas do PKMC")

        required_cols = [
            "Material", "Área abastec.prod.", "Nº circ.regul.",
            "Tipo de depósito", "Posição no depósito", "Container",
            "Texto breve de material", "Norma de embalagem", "Quantidade Kanban", "Posição de armazenamento"
        ]

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes no PKMC: {missing}")

        df = (
            df.select(required_cols)
              .filter(pl.col("Tipo de depósito") == "B01")
              .with_columns(
                  pl.col("Material").str.replace_all(" ", "").str.replace_all(r"\.", "")
              )
              .rename({
                  "Material": "partnumber",
                  "Área abastec.prod.": "area_abastecimento",
                  "Nº circ.regul.": "num_circ_regul_pkmc",
                  "Tipo de depósito": "tipo_deposito_pkmc",
                  "Posição no depósito": "posicao_deposito_pkmc",
                  "Container": "container_pkmc",
                  "Texto breve de material": "descricao_partnumber",
                  "Norma de embalagem": "norma_embalagem_pkmc",
                  "Quantidade Kanban": "qtd_por_caixa", 
                  "Posição de armazenamento": "qtd_max_caixas"
              })
        )
        self.df_pkmc = df
        logger.info(f"Processamento do PKMC concluído: {len(df)} linhas")
        return df

    def read_pk05(self) -> pl.DataFrame:
        logger.info(f"Lendo arquivo PK05: {self.pk05_path.name}")
        df = pl.read_excel(self.pk05_path, engine="calamine")
        logger.info(f"Carregadas {len(df)} linhas do PK05")

        required_cols = [
            "Área abastec.prod.", "Depósito", "Responsável",
            "Ponto de descarga", "Denominação SupM"
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes no PK05: {missing}")

        df = (
            df.select(required_cols)
              .filter(pl.col("Depósito") == "LB01")
              .rename({
                  "Área abastec.prod.": "area_abastecimento",
                  "Depósito": "deposito_pk05",
                  "Responsável": "responsavel_pk05",
                  "Ponto de descarga": "ponto_descarga_pk05",
                  "Denominação SupM": "denominacao_pk05",
              })
        )
        self.df_pk05 = df
        logger.info(f"Processamento do PK05 concluído: {len(df)} linhas")
        return df

    def join_and_transform(self) -> pl.DataFrame:
        logger.info("Unindo PKMC e PK05 pela coluna 'area_abastecimento'")
        if self.df_pkmc.is_empty() or self.df_pk05.is_empty():
            raise ValueError("PKMC ou PK05 está vazio")

        df = self.df_pkmc.join(self.df_pk05, on="area_abastecimento", how="inner")
        logger.info(f"Resultado da união: {len(df)} linhas")

        if df.is_empty():
            logger.warning("Resultado da união está vazio, nenhuma chave correspondente encontrada")
            return df

        df = df.with_columns([
            pl.col("denominacao_pk05").str.extract(r"(T\d+)", 1).alias("tacto"),
            pl.col("area_abastecimento").str.extract(r"(P\d+[A-Z]?)", 1).alias("prateleira"),
            
            pl.col("qtd_max_caixas")
            .cast(pl.Utf8)
            .str.extract(r"MAX\s*(\d+)", 1)
            .cast(pl.Float64, strict=False)
            .alias("qtd_max_caixas"),
            
            pl.col("qtd_por_caixa")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.replace_all(r"[^\d.,]", "")
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
            .alias("qtd_por_caixa"),
            
            pl.col("partnumber")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.replace_all(r"\s+", "")
            .str.replace_all(r"\.", "")
            .str.replace_all(r"[^\w-]", "")
            .str.to_uppercase()
        ])

        df = df.with_columns([
            (pl.col("qtd_por_caixa") * pl.col("qtd_max_caixas")).alias("qtd_total_teorica"),
            (pl.col("qtd_por_caixa") * (pl.col("qtd_max_caixas")-1)).alias("qtd_para_reabastecimento")
        ])

        before = len(df)
        df = df.filter(pl.col("tacto").is_not_null() & pl.col("tacto").str.starts_with("T"))
        after = len(df)
        if after < before:
            logger.info(f"Filtradas {before - after} linhas com tacto inválido")

        self.df_joined = df
        logger.info(f"União e transformação concluídas: {after} linhas prontas")
        return df

    def process(self, save_to_db: bool = True) -> Dict[str, Any]:
        try:
            start_time = datetime.now()
            logger.info(f"Processamento completo PKMC/PK05 iniciado às {start_time:%H:%M:%S}")

            if not self._validate_files():
                logger.critical("Arquivos ausentes — sinalizando parada global")
                return {"status": "error", "message": "Arquivos obrigatórios não encontrados"}

            if self.stop_event.is_set():
                return {"status": "error", "message": "Parada global acionada durante validação"}

            self.read_pkmc()
            self.read_pk05()
            self.join_and_transform()

            final_count = len(self.df_joined)
            if final_count == 0:
                return {
                    "status": "warning",
                    "message": "Nenhum dado correspondente encontrado",
                    "final_records": 0,
                }

            if save_to_db:
                self.db_manager.insert_pkmc_pk05(self.df_joined)
                logger.info(f"Salvos {final_count} registros no BD (pkmc_pk05)")

            elapsed = (datetime.now() - start_time).total_seconds()
            summary = {
                "status": "success",
                "pkmc_records": len(self.df_pkmc),
                "pk05_records": len(self.df_pk05),
                "final_records": final_count,
                "unique_tactos": self.df_joined["tacto"].n_unique(),
                "unique_partnumbers": self.df_joined["partnumber"].n_unique(),
                "processing_time": round(elapsed, 2),
                "saved_to_db": save_to_db,
            }

            logger.info(f"Processamento concluído com sucesso em {elapsed:.2f}s")
            return summary

        except Exception as e:
            logger.error(f"Falha no processamento geral PKMC/PK05: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}



# def process_pkmc_pk05(master_stop_event: Optional[Event] = None,
#                       save_to_db: bool = True) -> Dict[str, Any]:
#     try:
#         logger.info("process_pkmc_pk05() iniciado")
#         processor = PKMCProcessor(master_stop_event)
#         result = processor.process(save_to_db)

#         if master_stop_event and master_stop_event.is_set():
#             logger.critical("Parada global acionada pelo PKMC/PK05 — interrompendo sistema")
#         return result

#     except Exception as e:
#         logger.critical(f"Falha na inicialização do PKMCProcessor: {e}", exc_info=True)
#         return {"status": "error", "message": str(e)}


# def main(master_stop_event: Optional[Event] = None):
#     logger.info("Execução principal PKMC/PK05 iniciada")
#     result = process_pkmc_pk05(master_stop_event=master_stop_event)

#     if result["status"] == "success":
#         logger.info(f"Execução finalizada: {result['final_records']} linhas processadas em {result['processing_time']}s")
#     elif result["status"] == "warning":
#         logger.warning(f"Execução finalizada com aviso: {result['message']}")
#     else:
#         logger.error(f"Execução falhou: {result['message']}")

#     logger.info("Execução principal PKMC/PK05 finalizada")


# if __name__ == "__main__":
#     main()