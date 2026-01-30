from backend.services.sap import main_extract_data_lt22, main_auto_request_sap
from backend.orchestrators.base import BaseOrchestrator
from threading import Thread, Lock
from dotenv import load_dotenv
from backend.log import log_files
import time, os


logger = log_files.get_logger("orchestrator | processes | sap_orchestrator", "orchestrator")

class SAPOrchestrator(BaseOrchestrator):
    load_dotenv()

    def __init__(self):
        super().__init__("SAP")
        self.config = {"timeout": int(os.getenv("TIMEOUT")), "interval": int(os.getenv("RELOAD_INTERVAL_SAP"))}
        self._lock = Lock()
        logger.info("Orquestrador 'SAP' inicializado")

    def start(self):
        if self.is_running:
            logger.warning("Orquestrador 'SAP' já está em execução")
            return {"status": "já em execução", "running": True}
        self._stop_event.clear()
        self._thread = Thread(target=self._run_loop, daemon=True, name="sap_orchestrator")
        self._thread.start()
        self.is_running = True
        logger.info("Orquestrador 'SAP' iniciado")
        return {"status": "iniciado", "running": True}

    def stop(self):
        logger.info("Solicitada parada do orquestrador 'SAP'...")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self.is_running = False
        logger.info("Orquestrador 'SAP' foi parado")
        return {"status": "parado", "running": False}

    def _execute_with_timeout(self, func, name: str, timeout: int):
        if not self._lock.acquire(blocking=False):
            logger.warning(f"[{name}] ignorado — outra tarefa do orquestrador 'SAP' já está em execução")
            return None
        try:
            logger.info(f"[{name}] iniciado (execução exclusiva)")
            return super()._execute_with_timeout(func, name, timeout)
        finally:
            self._lock.release()
            logger.info(f"[{name}] finalizado")

    def _run_loop(self):
        logger.info("Laço principal do orquestrador 'SAP' iniciado")
        try:
            while not self._should_stop():
                # logger.debug("Executando extração de LT22 sem controle de timeout")
                # main_extract_data_lt22()
                
                # logger.debug("Executando extração de LM01 sem controle de timeout")
                # main_auto_request_sap()

                for i in range(self.config["interval"]):
                    if self._should_stop():
                        logger.debug("Sinal de parada detectado — encerrando espera antecipadamente")
                        break
                    logger.debug(f"Aguardando próximo ciclo ({i+1}/{self.config['interval']})...")
                    time.sleep(1)
        except Exception as e:
            if self._master_stop:
                logger.critical(f"Sistema será parado por completo. Erro crítico no laço do sistema 'SAP': {e}", exc_info=True)
                self._master_stop.set()
        finally:
            self.is_running = False
            logger.info("Laço principal do orquestrador 'SAP' encerrado")


sap_orchestrator = SAPOrchestrator()