from threading import Event
from datetime import datetime
from backend.log import log_files
from backend.orchestrators.processes import *


logger = log_files.get_logger('orchestrator | main', 'orchestrator')

class MainOrchestrator:    
    def __init__(self):
        self._master_stop = Event()
        self.lines_orchestrator = lines_orchestrator
        self.knr_orchestrator = knr_orchestrator
        self.sap_orchestrator = sap_orchestrator

        self.lines_orchestrator._master_stop = self._master_stop       
        self.knr_orchestrator._master_stop = self._master_stop
        
        logger.info("MainOrchestrator inicializado com todos os subsistemas")

    def lines_start(self):
        logger.info("Iniciando sistema 'LINHAS'")
        result = self.lines_orchestrator.start()
        logger.debug(f"Resultado da inicialização do sistema 'LINHAS': {result}")
        return result
    
    def knr_start(self):
        logger.info("Iniciando sistema 'KNR'")
        result = self.knr_orchestrator.start()
        logger.debug(f"Resultado da inicialização do sistema 'KNR': {result}")
        return result

    def sap_start(self):
        logger.info("Iniciando sistema 'SAP'")
        result = self.sap_orchestrator.start()
        logger.debug(f"Resultado da inicialização do sistema 'SAP': {result}")
        return result
    
    def lines_stop(self):
        logger.info("Parando sistema 'LINHAS'")
        result = self.lines_orchestrator.stop()
        logger.debug(f"Resultado da parada do sistema 'LINHAS': {result}")
        return result
    
    def knr_stop(self):
        logger.info("Parando sistema 'KNR'")
        result = self.knr_daily.stop()
        logger.debug(f"Resultado da parada do sistema 'KNR': {result}")
        return result
    
    def sap_stop(self):
        logger.info("Parando sistema 'SAP'")
        result = self.sap_orchestrator.stop()
        logger.debug(f"Resultado da parada do sistema 'SAP': {result}")
        return result

    def get_status(self):
        logger.info("Solicitando status dos sistemas")
        status = {
            "systems": {
                "lines": {
                    **self.lines_orchestrator.get_status(),
                    "config": self.lines_orchestrator.config
                },
                "knr": {
                    **self.knr_orchestrator.get_status(),
                    "config": self.knr_orchestrator.config
                },
                "sap":{
                    **self.sap_orchestrator.get_status(),
                    "config": self.sap_orchestrator.config
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        logger.debug(f"Status atual do MainOrchestrator: {status}")
        return status


main_orchestrator = MainOrchestrator()