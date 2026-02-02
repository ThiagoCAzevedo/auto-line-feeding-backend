from typing import Optional
from backend.log import log_files
from dotenv import load_dotenv
import win32com.client, time, pythoncom, os


logger = log_files.get_logger("sap | main_tasks_sap", "sap")

class ConexaoSAP:
    load_dotenv()

    def __init__(self):
        self.session = None
        self.sap_path = r'"C:\Program Files (x86)\SAP\FrontEnd\SAPGUI\saplogon.exe"'
        self.connection_name = "VW Brasil - MAIIS Produção    [P04] Link"
        self.min_retries = int(os.getenv("MIN_RETRIES"))
        self.max_retries = int(os.getenv("MAX_RETRIES"))
        self.time_between_retries = int(os.getenv("BETWEEN_RETRIES"))
        
        self.already_opened = False

    def connect_sap(self) -> Optional[str]:
        pythoncom.CoInitialize()
        logger.info("Tentando conectar ao SAP via sessão existente...")

        try:
            sap_gui_auto = win32com.client.GetObject("SAPGUI")
            application = sap_gui_auto.GetScriptingEngine
            
            if application.Children.Count > 0:
                connection = application.Children(0)
                if connection.Children.Count > 0:
                    self.session = connection.Children(0)
                    logger.info("Sessão SAP já estava aberta e foi reutilizada.")
                    self.already_opened = True
                    return
            logger.warning("SAP GUI ativo, mas sem sessões abertas. Será necessário abrir nova conexão.")
        except Exception as e:
            logger.warning(f"Não foi possível reutilizar sessão existente: {e}. Tentando abrir SAP GUI...")

        for attempts_initialized_sap in range(self.min_retries, self.max_retries + 1):
            try:
                shell = win32com.client.Dispatch("WScript.Shell")

                sap_path_clean = self.sap_path.strip('"')
                shell.Run(f'"{sap_path_clean}"')  
                
                logger.info("SAP GUI iniciado.")
                break
            except Exception as e:
                logger.warning(
                    f"Erro ao iniciar SAP GUI ({e}). "
                    f"Tentativa {attempts_initialized_sap}/{self.max_retries} - aguardando {self.time_between_retries // 60} minutos..."
                )
                time.sleep(self.time_between_retries)

        time.sleep(5)
        try:
            logger.info("Tentando abrir nova conexão SAP...")
            sap_gui_auto = win32com.client.GetObject("SAPGUI")
            application = sap_gui_auto.GetScriptingEngine
            connection = application.OpenConnection(self.connection_name, True)
            self.session = connection.Children(0)

            logger.info("Preenchendo credenciais de login...")
            self.session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = os.getenv("SAP_USER")
            self.session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = os.getenv("SAP_PASSWORD")
            self.session.findById("wnd[0]").sendVKey(0)
            logger.info("Login realizado com sucesso.")
        except Exception as e_conn:
            logger.error(f"Erro ao conectar ao SAP: {e_conn}")
            self.session = None

    def call_specific_transaction_sap(self, transaction="/n", message_path: Optional[str] = None):      
        if not self.session:
            logger.warning("Sessão SAP não encontrada. Tentando conectar...")
            self.connect_sap()
            
        if transaction:
            logger.info(f"Executando transação: {transaction}")
            try:
                self.session.findById("wnd[0]/tbar[0]/okcd").Text = transaction
                self.session.findById("wnd[0]").sendVKey(0)
                logger.info(f"Transação '{transaction}' executada com sucesso.")
                return self.session, self.already_opened
            except Exception as e_trans:
                logger.error(f"Erro ao executar transação '{transaction}': {e_trans}")
        else:
            logger.warning("Nenhuma transação informada.")


main_tasks_sap = ConexaoSAP()