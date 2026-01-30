from .extract_data import KNRsManager as transform_knr_data
from typing import List, Dict, Union, Optional
from contextlib import suppress
from dotenv import load_dotenv
from datetime import datetime, timedelta
from threading import Event
from backend.log import log_files
from pathlib import Path
import sys, time, os, win32com.client, pythoncom


logger = log_files.get_logger("knr | infos_knr_email | get_attachment", "knr")

class OutlookAttachmentExtractor:
    load_dotenv()

    def __init__(self, master_stop_event: Optional[Event]):
        self.save_dir = Path(os.getenv("VALORES_COMPLETOS_EMAIL_PATH")).resolve()
        self.actual_date = datetime.now().date()
        self.stop_event = master_stop_event
        self.emails_found = 0
        self.attachments_saved = 0
        self.temporary_files: List[str] = []
        self.max_retries = int(os.getenv("MAX_RETRIES"))
        self.min_retries = int(os.getenv("MIN_RETRIES"))
        self.time_between_retries = int(os.getenv("BETWEEN_RETRIES"))
        self.target_outlook_account = os.getenv("TARGET_EMAIL")

        pythoncom.CoInitialize()
        try:
            logger.info("Conectando ao Outlook...")
            self.outlook_app = win32com.client.Dispatch("Outlook.Application")
            self.namespace = self.outlook_app.GetNamespace("MAPI")
            self.namespace.Logon()
            for account in self.outlook_app.Session.Accounts:
                logger.debug(f"Conta detectada: {account.DisplayName}")
                if account.DisplayName.lower() == self.target_outlook_account.lower():
                    self.inbox = account.DeliveryStore.GetDefaultFolder(6).Items
                    break
            else:
                raise ValueError(f"Conta {self.target_outlook_account} não encontrada nas contas do Outlook.")
            
            logger.info(f"Conectado à caixa de entrada da conta {self.target_outlook_account} com sucesso.")

        except Exception as e:
            logger.critical(f"Falha ao conectar ao Outlook: {e}", exc_info=True)
            pythoncom.CoUninitialize()
            raise

    def _sync_mailbox(self, full: bool = False):
        try:
            logger.debug("Sincronizando caixa de entrada...")
            self.namespace.SendAndReceive(False)
            time.sleep(5 if full else 2)
            logger.debug("Sincronização concluída.")
        except Exception as e:
            logger.warning(f"Falha durante sincronização: {e}")

    def _cleanup(self):
        for f in self.temporary_files:
            with suppress(Exception):
                Path(f).unlink(missing_ok=True)
                logger.debug(f"Arquivo temporário removido: {f}")

        with suppress(Exception):
            self.namespace.Logoff()

        self.outlook_app = None
        with suppress(Exception):
            pythoncom.CoUninitialize()

        logger.info("Sessão Outlook encerrada e limpa.")

    def find_emails(self, subject: str) -> List:
        self.emails_found = 0
        logger.info(f"Procurando e-mails com assunto: '{subject}'")

        restriction = (
            f'@SQL="urn:schemas:httpmail:datereceived" >= \'{self.actual_date:%Y-%m-%d}\' '
            f'AND "urn:schemas:httpmail:subject" = \'{subject}\''
        )
        
        try:
            restricted_items = self.inbox.Restrict(restriction)
            results = list(restricted_items)
            self.emails_found = len(results)
            for msg in results:
                logger.debug(f"E-mail: {msg.Subject} | {'Não lido' if msg.UnRead else 'Lido'} | {msg.ReceivedTime}")
            logger.info(f"E-mails encontrados: {self.emails_found}")
            return results
        except Exception as e:
            logger.error(f"Erro ao buscar e-mails: {e}", exc_info=True)
            return []

    def _retry_find_emails(self, subject: str) -> List:
        for attempt in range(self.min_retries, self.max_retries + 1):
            emails = self.find_emails(subject)
            if emails:
                logger.info(f"E-mail encontrado na tentativa {attempt}.")
                return emails

            logger.warning(
                f"Nenhum e-mail encontrado - tentativa {attempt}/{self.max_retries}. "
                f"Aguardando {self.time_between_retries // 60} minutos antes da nova tentativa."
            )
            time.sleep(self.time_between_retries)

            if attempt % 2 == 0:
                self._sync_mailbox(full=True)

        logger.critical(
            f"Nenhum e-mail foi localizado após {self.max_retries} tentativas "
            f"({(self.time_between_retries // 60) * self.max_retries} minutos no total)."
        )
        if self.stop_event:
            self.stop_event.set()
        return []

    def extract_excel_attachments(self, messages: List) -> List[Dict[str, Union[str, datetime]]]:
        self.attachments_saved = 0
        excel_files = []

        for idx, message in enumerate(messages, 1):
            try:
                attachments = list(message.Attachments)
                for attachment in attachments:
                    if attachment.FileName.lower().endswith(".xlsx"):
                        final_path = self.save_dir
                        attachment.SaveAsFile(str(final_path))
                        excel_files.append({
                            "nome": attachment.FileName,
                            "caminho_final": str(final_path),
                            "data_email": message.ReceivedTime,
                            "extensao": ".xlsx",
                        })
                        self.attachments_saved += 1
                        logger.debug(f"Anexo salvo em: {final_path}")

                if message.UnRead:
                    message.UnRead = False
                    message.Save()

            except Exception as e:
                logger.error(f"Erro ao extrair anexos do e-mail {idx}: {e}", exc_info=True)

        logger.info(f"Total de anexos Excel extraídos: {self.attachments_saved}")
        return excel_files

    def process_knr_email(self) -> Dict[str, Union[int, str]]:
        try:
            today_str = self.actual_date.strftime("%Y-%m-%d")
            subject = f"PCP34- Circulante KNR_TST - Envio Automático - {today_str}"

            logger.info(f"Iniciando processamento KNR para assunto: {subject}")

            self._sync_mailbox(full=True)
            emails = self.find_emails(subject) or self._retry_find_emails(subject)

            if not emails:
                logger.critical("Sistema será parado. Não foi encontrado e-mail PCP após todas as tentativas.")
                return {
                    "emails_found": 0,
                    "attachments_saved": 0,
                    "data_processada": today_str,
                    "erro": "Email PCP não encontrado."
                }

            excel_files = self.extract_excel_attachments(emails)
            if excel_files:
                logger.info("Processando arquivos extraídos via pipeline KNR...")
                knrs_manager = transform_knr_data()
                knrs_manager.run_pipeline()
                logger.info("Pipeline KNR executado com sucesso.")
            else:
                logger.warning("Nenhum anexo Excel válido encontrado.")

            return {
                "emails_found": self.emails_found,
                "attachments_saved": self.attachments_saved,
                "data_processada": today_str,
            }

        except Exception as e:
            logger.critical(f"Erro fatal no processamento KNR: {e}", exc_info=True)
            return {"emails_found": 0, "attachments_saved": 0, "erro": str(e)}

        finally:
            self._cleanup()


def main(master_stop_event: Optional[Event] = None) -> int:
    try:
        logger.info("Iniciando extrator Outlook standalone para KNR.")
        extractor = OutlookAttachmentExtractor(master_stop_event)
        stats = extractor.process_knr_email()

        if "erro" in stats:
            logger.warning("Extração KNR terminou com erro.")
            return 1

        logger.info("Extração KNR concluída com sucesso.")
        return 0

    except Exception as e:
        logger.critical(f"Erro fatal no extrator standalone: {e}", exc_info=True)
        return 1

    finally:
        logger.info("Finalizando execução do extrator KNR.")


if __name__ == "__main__":
    main()