from backend.log import log_files
from typing import List
from backend.database import db_manager_general
import polars as pl, sys


logger = log_files.get_logger('lines | infos_linha_pintura | remove_values', 'lines')

class RemoveValuesProcessor: 
    def __init__(self):        
        self.max_retries = 3
        self.batch_size = 500
        self.db_manager = db_manager_general

        self.df_knrs_linha_montagem: pl.DataFrame = pl.DataFrame()
        self.df_knrs_linha_pintura: pl.DataFrame = pl.DataFrame()
        self.removed_count = 0
        self.error_count = 0
        
        logger.info("RemoveValuesProcessor inicializado")
    
    def pipeline(self) -> None:
        try:
            logger.info("Iniciando pipeline do RemoveValuesProcessor")
            if not self._fetch_data_from_database():
                logger.warning("Nenhum dado obtido do banco de dados. Encerrando pipeline.")
                return

            self._remove_duplicate_values()
            logger.info(f"Pipeline concluído com sucesso - Total removido={self.removed_count}")
        except Exception as e:
            logger.error(f"Erro fatal no pipeline do RemoveValuesProcessor: {e}", exc_info=True)
            self.error_count += 1
            raise
    
    def _fetch_data_from_database(self) -> bool:
        try:
            logger.info("Buscando valores de KNR nas tabelas linha.linha_montagem e linha.linha_pintura")

            knrs_linha_montagem = self.db_manager.execute_query("SELECT knr FROM linha.linha_montagem")
            knrs_linha_pintura = self.db_manager.execute_query("SELECT knr FROM linha.linha_pintura")

            self.df_knrs_linha_montagem = pl.DataFrame(knrs_linha_montagem)
            self.df_knrs_linha_pintura = pl.DataFrame(knrs_linha_pintura)

            logger.info(f"Foram obtidos {len(self.df_knrs_linha_montagem)} KNRs da montagem e "
                        f"{len(self.df_knrs_linha_pintura)} KNRs da pintura")
            return True
        except Exception as e:
            logger.error(f"Erro ao buscar dados de KNR no banco: {e}", exc_info=True)
            self.error_count += 1
            return False
    
    def _remove_duplicate_values(self) -> None:
        try:
            if self.df_knrs_linha_montagem.is_empty():
                logger.info("Tabela de linha de montagem vazia – nada a remover.")
                return
            if self.df_knrs_linha_pintura.is_empty():
                logger.info("Tabela de linha de pintura vazia – nada a remover.")
                return

            logger.info("Buscando KNRs duplicados entre linha_montagem e linha_pintura")
            df_common = self.df_knrs_linha_pintura.join(
                self.df_knrs_linha_montagem, on="knr", how="inner"
            )

            if df_common.is_empty():
                logger.info("Nenhum valor de KNR duplicado encontrado")
                return

            knrs_to_remove = df_common["knr"].to_list()
            total = len(knrs_to_remove)
            logger.info(f"Foram encontrados {total} duplicados para remoção da tabela linha_pintura")

            if total > self.batch_size:
                logger.info(f"Removendo em lotes de {self.batch_size}")
                self._remove_in_batches(knrs_to_remove)
            else:
                self._execute_removal(knrs_to_remove)

            self.removed_count += total
            logger.info(f"Remoção concluída com sucesso - {total} KNR(s) duplicado(s) removido(s) de linha_pintura")
        except Exception as e:
            logger.error(f"Erro durante remoção de duplicados: {e}", exc_info=True)
            self.error_count += 1
            raise
    
    def _remove_in_batches(self, knrs_to_remove: List[str]) -> None:
        total_batches = (len(knrs_to_remove) + self.batch_size - 1) // self.batch_size
        for i in range(0, len(knrs_to_remove), self.batch_size):
            batch = knrs_to_remove[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            logger.debug(f"Removendo lote {batch_num}/{total_batches} contendo {len(batch)} KNRs")
            try:
                self._execute_removal(batch)
            except Exception as e:
                logger.error(f"Falha ao remover lote {batch_num}: {e}", exc_info=True)
                continue
    
    def _execute_removal(self, knrs: List[str]) -> None:
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                placeholders = ", ".join(["?"] * len(knrs))
                query = f"DELETE FROM linha.linha_pintura WHERE knr IN ({placeholders})"
                logger.debug(f"Executando deleção de {len(knrs)} KNR(s)")
                self.db_manager.execute_query(query, knrs)
                logger.debug(f"{len(knrs)} KNR(s) removido(s) de linha_pintura com sucesso")
                return
            except Exception as e:
                retry_count += 1
                if retry_count >= self.max_retries:
                    logger.error(f"Falha na deleção após {self.max_retries} tentativas: {e}", exc_info=True)
                    raise
                else:
                    logger.warning(f"Tentativa de deleção {retry_count} falhou, tentando novamente: {e}")
    
    def get_statistics(self) -> dict:
        return {
            "removidos": self.removed_count,
            "erros": self.error_count,
            "registros_montagem": len(self.df_knrs_linha_montagem),
            "registros_pintura": len(self.df_knrs_linha_pintura)
        }
    
    def reset_counters(self) -> None:
        self.removed_count, self.error_count = 0, 0
        logger.info("Contadores do processador foram redefinidos")


def main():
    processor = RemoveValuesProcessor()
    try:
        logger.info("Principal: iniciando execução do RemoveValuesProcessor")
        processor.pipeline()
        stats = processor.get_statistics()
        logger.info(f"Principal: execução concluída com sucesso - estatísticas={stats}")
    except Exception as e:
        logger.critical(f"Principal: erro fatal {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Principal: execução do RemoveValuesProcessor finalizada")


if __name__ == "__main__":
    main()