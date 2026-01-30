from dotenv import load_dotenv
from pathlib import Path
from ..main_tasks_sap import main_tasks_sap
from backend.log import log_files
from .clean_data import main as main_clean_data_lt22
from datetime import datetime
import win32com.client, os, time


logger = log_files.get_logger("sap | lt22 | extract_data", "sap")
_already_requested_global = False

class ExtractSAPLT22:
    load_dotenv()

    def __init__(self):
        logger.info("Inicializando extração LT22")
        self.path_lt22 = Path(os.getenv("LT22_PATH")).resolve()
        self.lt22_file_name = Path(os.getenv("LT22_FILE_NAME")).resolve()
        self.infos_extract_lt22 = {}
        self.found_lt22 = False
        self.already_requested = self._load_state()
        logger.info(f"Estado inicial already_requested = {self.already_requested}")

    def _load_state(self) -> bool:
        global _already_requested_global
        return _already_requested_global

    def _save_state(self, value: bool):
        global _already_requested_global
        _already_requested_global = value

    def verify_results_sp02_sap(self):
        logger.info("Verificando resultados SP02")
        session, already_opened_sap_session = main_tasks_sap.call_specific_transaction_sap("/nsp02")
        message = session.findById("wnd[0]/usr/lbl[2,3]").Text

        self.found_lt22 = False
        i = 3

        if message == "":
            while True:
                try:
                    job_name = session.findById(f"wnd[0]/usr/lbl[51,{i}]").Text
                    job_hour = session.findById(f"wnd[0]/usr/lbl[30,{i}]").Text

                    if "lt22" in job_name.lower():
                        self.infos_extract_lt22 = {
                            "nome_job": job_name,
                            "hora_job": job_hour,
                            "index": i
                        }
                        self.found_lt22 = True
                        logger.info(f"Job LT22 encontrado na linha {i}")
                        break
                except Exception:
                    break

        if self.found_lt22:
            self.save_extracted_lt22_sap(self.infos_extract_lt22["index"], session)
        elif not self.already_requested:
            self.request_lt22_sap()

    def save_extracted_lt22_sap(self, index: int, session):
        logger.info(f"Salvando spool LT22 da linha {index}")
        try:
            session.findById(f"wnd[0]/usr/chk[1,{index}]").Selected = False
            session.findById(f"wnd[0]/usr/lbl[14,{index}]").setFocus()
            session.findById(f"wnd[0]/usr/lbl[14,{index}]").caretPosition = 0
            session.findById("wnd[0]").sendVKey(2)
            session.findById("wnd[0]/tbar[1]/btn[48]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/usr/ctxtDY_PATH").Text = str(self.path_lt22)
            session.findById("wnd[1]/usr/ctxtDY_FILENAME").Text = str(self.lt22_file_name)
            session.findById("wnd[1]/tbar[0]/btn[11]").press()

            self.clean_sp02(index)
            main_clean_data_lt22()
            self.request_lt22_sap()

            self.already_requested = False
            self._save_state(False)
            logger.info(f"LT22 salva em {self.path_lt22 / self.lt22_file_name}")
        except Exception as e:
            logger.error(f"Erro ao exportar spool: {e}", exc_info=True)

    def request_lt22_sap(self):
        logger.info("Solicitando nova LT22 via SAP")
        try:
            node_path = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell"
            b01_path = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN002-LOW"
            take_nodes_path = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[0]/shell"

            session, already_opened_sap_session = main_tasks_sap.call_specific_transaction_sap("/nlt22")

            # N° do depósito
            session.findById("wnd[0]/usr/ctxtT3_LGNUM").Text = "ANC"
            session.findById("wnd[0]/tbar[1]/btn[16]").press()
            
            # Navega e seleciona nós
            session.findById(node_path).expandNode("         68")
            session.findById(node_path).selectNode("        108")
            session.findById(node_path).selectNode("        123")
            session.findById(node_path).topNode = "        123"

            # Pressiona botão TAKE
            session.findById(take_nodes_path).pressButton("TAKE")

            # Preenche com "B01"
            session.findById(b01_path).text = "B01"
            session.findById(b01_path).setFocus()
            session.findById(b01_path).caretPosition = 3
            session.findById("wnd[0]").sendVKey(0)

            # Status confirmação
            session.findById("wnd[0]/usr/radT3_OFFTA").select() # "Só itens OT pendentes"

            # Data OT
            data_atual = datetime.now().strftime("%d.%m.%Y")
            session.findById("wnd[0]/usr/ctxtBDATU-LOW").Text = data_atual
            session.findById("wnd[0]/usr/ctxtBDATU-HIGH").Text = data_atual

            # Variante de exibição
            session.findById("wnd[0]/usr/ctxtLISTV").Text = "/sys_knr"

            # Menu para salvar em background
            session.findById("wnd[0]").sendVKey(9)
            session.findById("wnd[1]/usr/ctxtPRI_PARAMS-PDEST").text = "LOCL"
            
            session.findById("wnd[1]/tbar[0]/btn[13]").press()
            session.findById("wnd[1]/usr/btnSOFORT_PUSH").press()
            session.findById("wnd[1]/tbar[0]/btn[11]").press()
            
            self.already_requested = True
            self._save_state(True)
            logger.info("Nova LT22 solicitada com sucesso")

        except Exception as e:
            logger.error(f"Erro ao solicitar LT22: {e}", exc_info=True)

    def clean_sp02(self, index: int):
        logger.info(f"Limpando spool da linha {index}")
        try:
            session, already_opened_sap_session = main_tasks_sap.call_specific_transaction_sap("/nsp02")
            session.findById(f"wnd[0]/usr/chk[1,{index}]").Selected = True
            session.findById("wnd[0]/tbar[1]/btn[14]").press()
            session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()
            logger.info(f"Spool na linha {index} excluído com sucesso")
        except Exception as e:
            logger.error(f"Erro ao excluir spool: {e}", exc_info=True)


def main():
    logger.info("Iniciando execução principal da extração LT22")
    try:
        processor = ExtractSAPLT22()
        processor.verify_results_sp02_sap()
    except Exception as e:
        logger.error(f"Erro na execução principal: {e}", exc_info=True)
    finally:
        logger.info("Execução principal finalizada")


if __name__ == "__main__":
    main()