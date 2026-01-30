from typing import Any, Optional
from datetime import datetime
import paho.mqtt.client as mqtt
from .remove_values import RemoveValuesProcessor
import polars as pl, json, time, sys, os
from backend.log import log_files
from dotenv import load_dotenv
from backend.database import db_manager_general, db_manager_lines


logger = log_files.get_logger('lines | infos_painting_line | extract_data', 'lines')

class PaintingLineProcessor:
    def __init__(self):
        self.collection_time = int(os.getenv("COLLECTION_TIME"))
        self.max_messages = int(os.getenv("MAX_MESSAGES"))
        self.db_manager = db_manager_general
        self.db_manager_linhas = db_manager_lines
        self.processed_count = 0
        self.error_count = 0
        self.collected_messages = []
        self.knr_to_save = []
        self.actual_knr = None
        self.last_knr = None
        self._mqtt_client: Optional[mqtt.Client] = None
        self._start_time = None
        self._collection_complete = False
        self._connected = False
        logger.info("PaintingLineProcessor inicializado")

    def pipeline(self) -> None:
        try:
            logger.info("Iniciando pipeline da Linha de Pintura")
            self._run_remove_values()
            self._collect_mqtt_messages()

            if self.collected_messages:
                logger.info(f"Processando {len(self.collected_messages)} mensagens coletadas")
                self._process_all_messages()

                if self.knr_to_save:
                    self._save_all_to_database()
                else:
                    logger.warning("Nenhum valor de KNR válido encontrado para salvar")
            else:
                logger.warning("Nenhuma mensagem MQTT foi coletada")

            logger.info(f"Pipeline finalizado - Total de KNR(s) processados: {self.processed_count}")
        except Exception as e:
            logger.critical(f"Erro fatal no pipeline: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()

    def _run_remove_values(self) -> None:
        try:
            logger.info("Executando limpeza com RemoveValuesProcessor")
            remove_values = RemoveValuesProcessor()
            remove_values.pipeline()
            logger.info("Limpeza do RemoveValuesProcessor concluída com sucesso")
        except Exception as e:
            logger.error(f"Erro durante execução do RemoveValuesProcessor: {e}", exc_info=True)

    def _collect_mqtt_messages(self) -> None:
        logger.info(f"Iniciando coleta de mensagens - duração={self.collection_time}s, máximo={self.max_messages}")
        self._start_time = time.time()
        self._mqtt_client = self._create_mqtt_client()

        try:
            logger.info("Conectando ao broker MQTT")
            self._mqtt_client.connect(os.getenv("SYSTEM_HOST_PL"), int(os.getenv("SYSTEM_PORT_PL")), keepalive=int(os.getenv("KEEPALIVE")))

            while not self._collection_complete:
                self._mqtt_client.loop(timeout=0.1)
                elapsed = time.time() - self._start_time

                if elapsed >= self.collection_time:
                    logger.info(f"Tempo máximo de coleta atingido: {self.collection_time}s")
                    self._collection_complete = True

                if len(self.collected_messages) >= self.max_messages:
                    logger.info(f"Limite máximo de mensagens atingido: {self.max_messages}")
                    self._collection_complete = True

                if int(elapsed) % 5 == 0 and int(elapsed) > 0:
                    logger.debug(f"Progresso: {elapsed:.0f}s transcorridos, {len(self.collected_messages)} mensagens coletadas")

            logger.info(f"Coleta de mensagens finalizada - {len(self.collected_messages)} mensagens em {elapsed:.1f}s")
        except Exception as e:
            logger.error(f"Erro durante a coleta de mensagens MQTT: {e}", exc_info=True)
            raise
        finally:
            if self._mqtt_client:
                try:
                    self._mqtt_client.disconnect()
                except:
                    pass

    def _process_all_messages(self) -> None:
        logger.info("Processando mensagens coletadas")
        unique_knrs = set()

        for msg in self.collected_messages:
            try:
                payload = msg.get("payload")
                topic = msg.get("topic")

                if topic != "TablesOut":
                    continue

                texto = payload.decode("utf-8") if isinstance(payload, bytes) else payload
                try:
                    arr = json.loads(texto)
                    for item in arr:
                        if isinstance(item, dict):
                            interno = list(item.values())[0]
                            if isinstance(interno, dict) and "KNR" in interno and self._is_valid_knr(interno["KNR"]):
                                knr_value = interno["KNR"]
                                if knr_value not in unique_knrs and knr_value != self.last_knr:
                                    unique_knrs.add(knr_value)
                                    self.knr_to_save.append(knr_value)
                                    logger.debug(f"KNR identificado: {knr_value}")
                                self.last_knr = knr_value
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON inválido recebido: {e}")
                except Exception as e:
                    logger.error(f"Erro ao processar conteúdo da mensagem: {e}", exc_info=True)
            except Exception as e:
                self.error_count += 1
                logger.error(f"Erro ao processar mensagem MQTT: {e}", exc_info=True)

        logger.info(f"Processamento concluído - {len(self.knr_to_save)} KNR(s) únicos encontrados")

    def _is_valid_knr(self, value: Any) -> bool:
        try:
            val = str(value).strip()
            if val == "0" or val == "" or all(ch == "\u0000" for ch in val) or len(val) < 7 or value is False:
                return False
            return True
        except Exception as e:
            logger.debug(f"Erro ao validar KNR: {e}")
            return False

    def _save_all_to_database(self) -> None:
        logger.info(f"Salvando {len(self.knr_to_save)} novo(s) KNR(s) no banco de dados")
        knr_list, sequencia_list = [], []

        # Determina sequência a ser salva
        result = self.db_manager.execute_query("SELECT COUNT(knr) FROM linha.linha_pintura")
        count_pintura = list(result[0].values())[0] if result else 0

        if count_pintura == 0:
            result_seq = self.db_manager.execute_query("SELECT MAX(lfdnr_sequencia) FROM linha.linha_montagem")
            max_seq = list(result_seq[0].values())[0] if result_seq and list(result_seq[0].values())[0] else 0
            new_seq = int(max_seq) + 1
            logger.debug(f"Iniciando com sequência da montagem: {new_seq}")
        else:
            result_linha = self.db_manager.execute_query("SELECT MAX(sequencia_prevista) FROM linha.linha_pintura")
            last_seq = list(result_linha[0].values())[0] if result_linha else 0
            new_seq = int(last_seq) + 1
            logger.debug(f"Continuando a partir da sequência da pintura: {new_seq}")

        for i, knr in enumerate(self.knr_to_save):
            knr_list.append(knr)
            seq_val = str(new_seq + i)
            sequencia_list.append(seq_val)
            logger.debug(f"Preparado KNR={knr} com sequência={seq_val}")

        df_pintura = pl.DataFrame({"knr": knr_list, "sequencia_prevista": sequencia_list})
        logger.info(f"DataFrame gerado com {len(df_pintura)} registros para linha_pintura")

        self.db_manager_linhas.insert_update_infos_linha_pintura(df_pintura)
        self.processed_count = len(df_pintura)
        logger.info(f"Atualização no banco de dados concluída - {self.processed_count} KNR(s) inserido(s)/atualizado(s)")

    def _create_mqtt_client(self) -> mqtt.Client:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.tls_set()
        client.tls_insecure_set(True)
        return client

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Conexão com o broker MQTT estabelecida")
            client.subscribe("TablesOut")
            logger.info("Inscrito no tópico: TablesOut")
            self._connected = True
        else:
            logger.error(f"Falha na conexão com o MQTT: código={reason_code}")
            self._connected = False

    def _on_message(self, client, userdata, msg):
        try:
            self.collected_messages.append({
                "topic": msg.topic,
                "payload": msg.payload,
                "timestamp": datetime.now().isoformat()
            })
            if len(self.collected_messages) % 100 == 0:
                logger.debug(f"{len(self.collected_messages)} mensagens coletadas até o momento")
        except Exception as e:
            logger.error(f"Erro no processamento de mensagem MQTT: {e}", exc_info=True)

    def _cleanup(self) -> None:
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except:
                pass
        logger.info("Limpeza da sessão MQTT concluída")

    def get_statistics(self) -> dict:
        return {
            "mensagens_coletadas": len(self.collected_messages),
            "knr_processados": len(self.knr_to_save),
            "registros_salvos": self.processed_count,
            "erros": self.error_count,
            "tempo_coleta": self.collection_time
        }


def main():
    processor = PaintingLineProcessor()
    try:
        logger.info("Principal: iniciando execução de PaintingLineProcessor")
        processor.pipeline()
        stats = processor.get_statistics()
        logger.info(f"Principal: execução concluída - Estatísticas={stats}")
        if processor.error_count > 0:
            logger.warning(f"Principal: finalizado com {processor.error_count} erro(s)")
        else:
            logger.info("Principal: concluído com sucesso, sem erros")
    except Exception as e:
        logger.critical(f"Principal: erro fatal {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Principal: execução de PaintingLineProcessor finalizada")


if __name__ == "__main__":
    main()