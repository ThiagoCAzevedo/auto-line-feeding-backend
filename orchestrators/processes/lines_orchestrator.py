from backend.orchestrators.base import BaseOrchestrator
from backend.orchestrators.utils import db_return_values
from threading import Thread, Lock
from dotenv import load_dotenv
from backend.services.lines import *
from backend.log import log_files
import time, os


logger = log_files.get_logger('orchestrator | processes | lines_orchestrator', 'orchestrator')

class LinhaOrchestrator(BaseOrchestrator):
    load_dotenv()

    def __init__(self):
        super().__init__("lines_orchestrator")
        self.config = {'interval': int(os.getenv("RELOAD_INTERVAL_GENERAL")), 'timeout': int(os.getenv("TIMEOUT"))}
        self.db_helper = db_return_values
        self._lock = Lock()
        logger.info("Orquestrador 'LINHAS' inicializado")
    
    def start(self):
        if self.is_running:
            logger.warning("Orquestrador 'LINHAS' já está em execução")
            return {"status": "já em execução", "running": True}
        
        self._stop_event.clear()
        self._thread = Thread(target=self._run_loop, daemon=True, name="lines_orchestrator")
        self._thread.start()
        self.is_running = True
        
        logger.info("Orquestrador 'LINHAS' iniciado")
        return {"status": "iniciado", "running": True}
    
    def stop(self):
        logger.info("Solicitada parada do orquestrador 'LINHAS'...")
        self._stop_event.set()
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
            if self._thread.is_alive():
                logger.warning("Thread do orquestrador 'LINHAS' não parou corretamente")
        
        self.is_running = False
        logger.info("Orquestrador 'LINHAS' parado")
        return {"status": "parado", "running": False}
    
    def _execute_protected(self, func, name: str, timeout: int):
        if not self._lock.acquire(blocking=False):
            logger.warning(f"[{name}] ignorado — outra tarefa do sistema 'LINHAS' já está em execução")
            return False
        try:
            logger.info(f"[{name}] iniciado (execução exclusiva)")
            result = super()._execute_with_timeout(func, name, timeout)
            if result:
                logger.info(f"[{name}] finalizado com sucesso")
            else:
                logger.warning(f"[{name}] falhou ou excedeu o tempo limite")
            return result
        except Exception as e:
            logger.error(f"[{name}] erro durante execução: {e}", exc_info=True)
            return False
        finally:
            self._lock.release()
            logger.debug(f"[{name}] bloqueio liberado")

    def _execute_lines_once(self):
        try:
            if self._should_stop():
                logger.debug("Parada detectada antes de iniciar o ciclo de linhas")
                return
            
            logger.info("Ciclo de execução do sistema 'LINHAS' iniciado")
            start_time = time.time()
            
            success = self._execute_protected(
                main_extract_data_assembly_line, "assembly_line", self.config['timeout']
            )
            if not success or self._should_stop():
                logger.debug("Interrompendo ciclo após `LINHA MONTAGEM`")
                return
            
            success = self._execute_protected(
                main_extract_data_painting_line, "painting_line", self.config['timeout']
            )
            if not success or self._should_stop():
                logger.debug("Interrompendo ciclo após `LINHA PINTURA`")
                return
            
            if self.db_helper.knrs_comuns_values() is not None and self.db_helper.linha_montagem_values() is not None:
                logger.info("[SaldoLB] iniciando processamento...")
                self._execute_protected(
                    main_define_values_lb_balance, "lb_balance", self.config['timeout']
                )
            else:
                logger.warning("Dados insuficientes para executar `SALDO LB`")

            elapsed = time.time() - start_time
            logger.info(f"Ciclo do sistema 'LINHAS' concluído em {elapsed:.1f}s")
                
        except Exception as e:
            if self._master_stop:
                logger.critical(f"Sistema será parado. Erro no ciclo do sistema 'LINHAS': {e}", exc_info=True)
                self._master_stop.set()
    
    def _run_loop(self):
        logger.info("Laço principal do sistema 'LINHAS' iniciado")
        
        try:
            while not self._should_stop():
                cycle_start = time.time()
                
                self._execute_lines_once()
                
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self.config['interval'] - elapsed)
                
                if sleep_time > 0:
                    logger.debug(f"Sistema 'LINHAS' aguardando próximo ciclo por {sleep_time:.1f}s")
                    for _ in range(int(sleep_time * 2)):  
                        if self._should_stop():
                            logger.debug("Parada detectada durante espera")
                            break
                        time.sleep(0.5)
                    
        except Exception as e:
            if self._master_stop:
                logger.critical(f"Sistema será parado por completo. Erro crítico no laço do sistema 'LINHAS': {e}", exc_info=True)
                self._master_stop.set()
        finally:
            self.is_running = False
            logger.info("Laço principal do sistema 'LINHAS' encerrado com sucesso")


lines_orchestrator = LinhaOrchestrator()