from backend.services.knr import *
from backend.orchestrators.utils import db_return_values
from backend.orchestrators.base import BaseOrchestrator
from datetime import datetime, timedelta
from threading import Thread, Lock
from dotenv import load_dotenv
from backend.log import log_files
import time, os


logger = log_files.get_logger("orchestrator | processes | knr_orchestrator", "orchestrator")

class KNROrchestrator(BaseOrchestrator):
    load_dotenv()

    def __init__(self):
        super().__init__("knr_orchestrator")
        self.config = {"timeout": int(os.getenv("TIMEOUT")), "run_after_hour": int(os.getenv("KNR_SERVICE_RUN_AFTER"))}
        self.db_helper = db_return_values
        self._lock = Lock()
        logger.info("Orquestrador 'KNR' foi inicializado")
        

    def start(self):
        if self.is_running:
            logger.warning("O orquestrador 'KNR' já está em execução")
            return {"status": "já em execução", "running": True}
        self._stop_event.clear()
        self._thread = Thread(target=self._run_loop, daemon=True, name="knr_orchestrator")
        self._thread.start()
        self.is_running = True
        logger.info("Orquestrador 'KNR' iniciado com sucesso")
        return {"status": "iniciado", "running": True}

    def stop(self):
        logger.info("Parada do orquestrador 'KNR' solicitada...")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self.is_running = False
        logger.info("Orquestrador 'KNR' foi finalizado")
        return {"status": "parado", "running": False}

    def _get_last_execution_date(self):
        logger.debug("Buscando última data de execução no banco de dados")
        return self.db_helper.knrs_vivos_values()

    def _execute_with_timeout(self, func, name: str, timeout: int):
        if not self._lock.acquire(blocking=False):
            logger.warning(f"[{name}] ignorado. Outra tarefa do orquestrador 'KNR' já está em execução")
            return None
        try:
            logger.info(f"[{name}] iniciado")
            return super()._execute_with_timeout(func, name, timeout)
        finally:
            self._lock.release()
            logger.info(f"[{name}] finalizado")

    def _process_daily(self):
        process_start = time.time()
        logger.info("Iniciando processamento diário do KNR")

        if not self._should_stop():
            logger.info("Executando 'knr_email'")
            main_knr_email(master_stop_event=self._master_stop)

        if not self._should_stop():
            logger.info("Executando verificação de arquivos")
            main_verify_files()

        if not self._should_stop():
            logger.info("Executando knr_fx4pd")
            main_knr_fx4pd(master_stop_event=self._master_stop)

        if not self._should_stop():
            logger.info("Executando pkmc_pk05")
            self._execute_with_timeout(main_pkmc_pk05(master_stop_event=self._master_stop), "pkmc_pk05", self.config["timeout"])

        if not self._should_stop():
            logger.info("Executando verificação de valores comuns")
            self._execute_with_timeout(main_verify_same_values, "KnrVerify", self.config["timeout"])

        elapsed = time.time() - process_start
        logger.info(f"Processamento diário concluído em {elapsed:.1f}s")
    
    def _run_loop(self):
        logger.info("Loop do KNR iniciado")
        try:
            while not self._should_stop():
                now = datetime.now()
                today = now.date()
                hour = now.hour

                actual_dates = self._get_last_execution_date()
                date_vivos, date_knrs_fx4pd = None, None
                if isinstance(actual_dates, tuple):
                    date_vivos, date_knrs_fx4pd = actual_dates

                if hour < self.config["run_after_hour"]:
                    sleep_until = now.replace(
                        hour=self.config["run_after_hour"],
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                    secs = (sleep_until - now).total_seconds()
                    logger.info(
                        f"Aguardando até {sleep_until.strftime('%H:%M')} para iniciar o processamento diário"
                    )
                    time.sleep(secs)
                    continue

                if (date_vivos == today) and (date_knrs_fx4pd == today):
                    logger.info(
                        f"KNR já foi executado hoje "
                        f"(knrs_vivos: {date_vivos}), pulando execução..."
                    )
                else:
                    logger.info("Executando lote diário do KNR")
                    self._process_daily()

                tomorrow = today + timedelta(days=1)
                next_execution = datetime.combine(tomorrow, datetime.min.time()).replace(
                    hour=self.config["run_after_hour"]
                )
                sleep_secs = int((next_execution - datetime.now()).total_seconds())
                logger.info(
                    f"Próxima execução do KNR agendada para {next_execution.strftime('%Y-%m-%d %H:%M')}"
                )
                time.sleep(max(5, sleep_secs))

        except Exception as e:
            if self._master_stop:
                logger.critical(f"Serviço será interrompido. Erro crítico no loop do serviço 'KNR': {e}", exc_info=True)
                self._master_stop.set()
        finally:
            self.is_running = False
            logger.info("Loop do serviço 'KNR' foi finalizado")


knr_orchestrator = KNROrchestrator()