from threading import Thread, Event
from abc import ABC, abstractmethod
from backend.log import log_files
import time


logger = log_files.get_logger('orchestrator | base | base_orchestrator', 'orchestrator')

class BaseOrchestrator(ABC):   
    def __init__(self, name):
        self.name = name
        self._stop_event = Event()
        self._thread = None
        self.is_running = False
        self._master_stop = None
        logger.info(f"BaseOrchestrator '{self.name}' inicializado")

    def _should_stop(self, specific_event=None):
        if self._master_stop and self._master_stop.is_set():
            logger.debug(f"Sinal de parada mestre detectado para '{self.name}'")
            return True
        if specific_event and specific_event.is_set():
            logger.debug(f"Sinal de parada específico detectado para '{self.name}'")
            return True
        if self._stop_event.is_set():
            logger.debug(f"Sinal de parada local detectado para '{self.name}'")
            return True
        return False
    
    def _execute_with_timeout(self, target_func, name, timeout):
        try:
            logger.debug(f"Iniciando thread '{name}' com timeout de {timeout}s")
            thread = Thread(target=target_func, daemon=True, name=name)
            thread.start()
            
            elapsed = 0
            while thread.is_alive() and elapsed < timeout:
                if self._should_stop():
                    logger.info(f"Parada solicitada durante execução de '{name}' no orquestrador '{self.name}'")
                    return False
                time.sleep(0.5)
                elapsed += 0.5
                logger.debug(f"Aguardando thread '{name}', tempo decorrido: {elapsed:.1f}s")
            
            if thread.is_alive():
                logger.warning(f"Thread '{name}' excedeu o tempo limite de {timeout}s (orquestrador '{self.name}')")
                return False
                
            logger.info(f"Thread '{name}' finalizada com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao executar thread '{name}' no orquestrador '{self.name}': {e}", exc_info=True)
            return False
    
    @abstractmethod
    def start(self):
        logger.debug(f"Chamando método abstrato 'start()' para orquestrador '{self.name}'")
        pass
    
    @abstractmethod
    def stop(self):
        logger.debug(f"Chamando método abstrato 'stop()' para orquestrador '{self.name}'")
        pass
    
    @abstractmethod
    def _run_loop(self):
        logger.debug(f"Chamando método abstrato '_run_loop()' para orquestrador '{self.name}'")
        pass
    
    def get_status(self):
        status = {
            "name": self.name,
            "status": "executando" if self.is_running else "parado",
            "running": self.is_running
        }
        logger.debug(f"Status solicitado para '{self.name}': {status}")
        return status