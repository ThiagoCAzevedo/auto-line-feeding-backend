from typing import Dict, List, Any, Optional
import polars as pl, json, time, os
from backend.database import db_manager_lines
from dotenv import load_dotenv
import paho.mqtt.client as mqtt



class AssemblyLineProcessor:
    load_dotenv()

    def __init__(self):        
        self.db_manager = db_manager_lines
        self.collection_time = int(os.getenv("COLLECTION_TIME"))
        self.max_messages = int(os.getenv("MAX_MESSAGES"))
        self.collected_messages = []
        self.processed_count = 0
        self.error_count = 0
        self._mqtt_client = None
        self._start_time = None
        self._collection_complete = False

    def pipeline(self) -> None:
        try:
            self._collect_mqtt_messages()

            if self.collected_messages:
                self._process_all_messages()
                self._save_to_database()

        except Exception as e:
            raise
        finally:
            self._cleanup()

    def _collect_mqtt_messages(self) -> None:
        self._start_time = time.time()
        self._mqtt_client = self._create_mqtt_client()

        try:
            self._mqtt_client.connect(os.getenv("SYSTEM_HOST_AL"), int(os.getenv("SYSTEM_PORT_AL")), keepalive=int(os.getenv("KEEPALIVE")))

            while not self._collection_complete:
                self._mqtt_client.loop(timeout=0.1)
                elapsed = time.time() - self._start_time

                if elapsed >= self.collection_time:
                    self._collection_complete = True

                if len(self.collected_messages) >= self.max_messages:
                    self._collection_complete = True

        except Exception as e:
            raise
        finally:
            if self._mqtt_client:
                self._mqtt_client.disconnect()

    def _process_all_messages(self) -> None:
        processed_data: List[Dict[str, Any]] = []

        for raw_msg in self.collected_messages:
            try:
                processed_entries = self._process_single_message(raw_msg)
                if processed_entries:
                    processed_data.extend(processed_entries)
            except Exception as e:
                self.error_count += 1

        self.processed_data = processed_data

    def _process_single_message(self, payload: Any) -> List[Dict[str, Any]]:
        results = []
        infos_mqtt = self._normalize_payload(payload)
        for first_info, bloco in infos_mqtt.items():
            if isinstance(bloco, dict):
                for tacto_data in bloco.values():
                    if isinstance(tacto_data, dict):
                        record = self._extract_tacto_info(tacto_data, first_info)
                        if record:
                            results.append(record)
        # except Exception as e:
        #     logger.error(f"Erro ao decodificar/normalizar mensagem: {e}", exc_info=True)
        return results

    def _extract_tacto_info(self, tacto_data: Dict[str, Any], first_info: str) -> Optional[Dict[str, Any]]:
        car = tacto_data.get("CAR")
        if not isinstance(car, dict):
            return None

        tacto = tacto_data.get("TACT")
        knr = car.get("KNR")
        lfdnr = car.get("LFDNR")

        if not tacto or not knr:
            return None

        try:
            lfdnr = int(lfdnr) if lfdnr is not None else None
        except (ValueError, TypeError):
            return None

        return {
            "tacto": str(tacto),
            "lane": tacto_data.get("LANE", first_info),
            "knr": str(knr),
            "lfdnr_sequencia": lfdnr,
            "modelo": car.get("MODELL"),
        }

    def _save_to_database(self) -> None:
        if not hasattr(self, "processed_data") or not self.processed_data:
            return
        try:
            df_infos = pl.DataFrame(self.processed_data).unique(subset=["knr", "lfdnr_sequencia"])
            self.db_manager.insert_update_infos_linha_montagem(df_infos)
            self.processed_count = len(self.processed_data)
        except Exception as e:
            raise

    # def _update_positions(self) -> None:
    #     try:
    #         logger.info("Atualizando posições da linha de montagem...")
    #         logger.info("Posições atualizadas com sucesso")
    #     except Exception as e:
    #         logger.error(f"Falha ao atualizar posições: {e}", exc_info=True)

    def _normalize_payload(self, payload: Any) -> Dict[str, Any]:
        if isinstance(payload, str):
            return json.loads(payload)
        if isinstance(payload, dict):
            content = payload.get("mensagem")
            return json.loads(content) if isinstance(content, str) else (content or payload)
        return {}

    def _create_mqtt_client(self) -> mqtt.Client:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.ws_set_options(path=os.getenv("SYSTEM_WEBSOCKET_PATH_AL"))
        client.tls_set()
        client.tls_insecure_set(True)
        return client

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            client.subscribe("#")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))
            self.collected_messages.append(payload)
        except Exception as e:
            raise

    def _cleanup(self) -> None:
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except:
                pass

    def get_statistics(self) -> dict:
        return {
            "mensagens_coletadas": len(self.collected_messages),
            "registros_processados": self.processed_count,
            "erros": self.error_count
        }


def main():
    processor = AssemblyLineProcessor()
    processor.pipeline()
    stats = processor.get_statistics()


if __name__ == "__main__":
    main()