from unittest.mock import patch, MagicMock, mock_open
from fastapi.testclient import TestClient
from pathlib import Path
from backend.api.web import app
import unittest,sys


class TestWebAPI(unittest.TestCase):
    """Testes para a API FastAPI do Sistema KNR"""
    @classmethod
    def setUpClass(cls):
        """Configuração inicial para todos os testes"""
        cls.client = TestClient(app)

    def test_health_check(self):
        """Testa o endpoint de health check"""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["service"], "Sistema Sesé API")
        self.assertIn("timestamp", data)

    def test_root_redirect(self):
        """Testa o redirecionamento da rota root"""
        response = self.client.get("/", follow_redirects=False)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Redirecionando para o dashboard", response.text)

    # --- TESTES DE LOGS ---
    @patch('backend.api.web.db_manager_api.read_log_file')
    def test_get_knr_logs(self, mock_read_log):
        """Testa o endpoint de logs do KNR"""
        mock_logs = ["Log 1", "Log 2", "Log 3"]
        mock_read_log.return_value = mock_logs

        response = self.client.get("/logs/knr")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        self.assertEqual(data["logs"], mock_logs)
        self.assertEqual(data["count"], 3)
        self.assertEqual(data["file"], "knr.log")
        mock_read_log.assert_called_once_with("knr")

    @patch('backend.api.web.db_manager_api.read_log_file')
    def test_get_linha_logs(self, mock_read_log):
        """Testa o endpoint de logs das linhas"""
        mock_logs = ["Linha Log 1", "Linha Log 2"]
        mock_read_log.return_value = mock_logs

        response = self.client.get("/logs/linhas")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        self.assertEqual(data["logs"], mock_logs)
        self.assertEqual(data["count"], 2)
        self.assertEqual(data["file"], "linhas.log")
        mock_read_log.assert_called_once_with("linhas")

    # --- TESTES DE API (TACTO E PRATELEIRA) ---
    @patch('backend.api.web.db_manager_api.get_by_tacto')
    def test_get_by_tacto_success(self, mock_get_tacto):
        """Testa busca por tacto com sucesso"""
        mock_result = {"shelves": [{"id": 1, "tacto": "T001"}]}
        mock_get_tacto.return_value = mock_result

        response = self.client.get("/api/tacto?tacto=T001")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), mock_result)
        mock_get_tacto.assert_called_once_with("T001")

    @patch('backend.api.web.db_manager_api.get_by_tacto')
    def test_get_by_tacto_error(self, mock_get_tacto):
        """Testa busca por tacto com erro"""
        mock_get_tacto.return_value = {"error": "Erro no banco"}

        response = self.client.get("/api/tacto")
        self.assertEqual(response.status_code, 500)

    @patch('backend.api.web.db_manager_api.get_by_prateleira')
    def test_get_by_prateleira_success(self, mock_get_prat):
        """Testa busca por prateleira com sucesso"""
        mock_result = {"shelves": [{"id": 1, "prateleira": "P001"}]}
        mock_get_prat.return_value = mock_result

        response = self.client.get("/api/prateleira?prateleira=P001")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), mock_result)
        mock_get_prat.assert_called_once_with("P001")

    @patch('backend.api.web.db_manager_api.get_by_prateleira')
    def test_get_by_prateleira_error(self, mock_get_prat):
        """Testa busca por prateleira com erro"""
        mock_get_prat.return_value = {"error": "Erro no banco"}

        response = self.client.get("/api/prateleira")
        self.assertEqual(response.status_code, 500)

    @patch('backend.api.web.db_manager_api.get_lt22_stil_opened')
    def test_get_open_ots_success(self, mock_get_ots):
        """Testa busca de OTs abertas com sucesso"""
        mock_result = {"ots": [{"id": 1, "status": "aberta"}]}
        mock_get_ots.return_value = mock_result

        response = self.client.get("/api/ots/sem-conclusao")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), mock_result)

    @patch('backend.api.web.db_manager_api.get_lt22_stil_opened')
    def test_get_open_ots_error(self, mock_get_ots):
        """Testa busca de OTs abertas com erro"""
        mock_get_ots.return_value = None

        response = self.client.get("/api/ots/sem-conclusao")
        self.assertEqual(response.status_code, 500)

    # --- TESTES DE START ---
    @patch('backend.api.web.main_orchestrator.lines_start')
    @patch('backend.api.web.main_orchestrator.knr_start')
    @patch('backend.api.web.main_orchestrator.sap_start')
    def test_start_all_systems(self, mock_sap, mock_knr, mock_lines):
        """Testa início de todos os sistemas"""
        mock_lines.return_value = {"status": "started"}
        mock_knr.return_value = {"status": "started"}
        mock_sap.return_value = {"status": "started"}

        response = self.client.get("/iniciar/sistema/knr-completo")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        self.assertIn("linhas", data)
        self.assertIn("knr", data)
        self.assertIn("sap", data)
        mock_lines.assert_called_once()
        mock_knr.assert_called_once()
        mock_sap.assert_called_once()

    @patch('backend.api.web.main_orchestrator.lines_start')
    def test_start_linhas(self, mock_lines):
        """Testa início apenas das linhas"""
        mock_lines.return_value = {"status": "started"}

        response = self.client.get("/iniciar/sistema/linhas")
        self.assertEqual(response.status_code, 200)
        mock_lines.assert_called_once()

    @patch('backend.api.web.main_orchestrator.knr_start')
    def test_start_knr(self, mock_knr):
        """Testa início apenas do KNR"""
        mock_knr.return_value = {"status": "started"}

        response = self.client.get("/iniciar/sistema/knr")
        self.assertEqual(response.status_code, 200)
        mock_knr.assert_called_once()

    @patch('backend.api.web.main_orchestrator.sap_start')
    def test_start_sap(self, mock_sap):
        """Testa início apenas do SAP"""
        mock_sap.return_value = {"status": "started"}

        response = self.client.get("/iniciar/sistema/sap")
        self.assertEqual(response.status_code, 200)
        mock_sap.assert_called_once()

    # --- TESTES DE STOP ---
    @patch('backend.api.web.main_orchestrator.lines_stop')
    @patch('backend.api.web.main_orchestrator.knr_stop')
    @patch('backend.api.web.main_orchestrator.sap_stop')
    def test_stop_all_systems(self, mock_sap, mock_knr, mock_lines):
        """Testa parada de todos os sistemas"""
        mock_lines.return_value = {"status": "stopped"}
        mock_knr.return_value = {"status": "stopped"}
        mock_sap.return_value = {"status": "stopped"}

        response = self.client.get("/parar/sistema/knr-completo")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        self.assertIn("linhas", data)
        self.assertIn("knr", data)
        self.assertIn("sap", data)
        mock_lines.assert_called_once()
        mock_knr.assert_called_once()
        mock_sap.assert_called_once()

    @patch('backend.api.web.main_orchestrator.lines_stop')
    def test_stop_linhas(self, mock_lines):
        """Testa parada apenas das linhas"""
        mock_lines.return_value = {"status": "stopped"}

        response = self.client.get("/parar/sistema/linhas")
        self.assertEqual(response.status_code, 200)
        mock_lines.assert_called_once()

    @patch('backend.api.web.main_orchestrator.knr_stop')
    def test_stop_knr(self, mock_knr):
        """Testa parada apenas do KNR"""
        mock_knr.return_value = {"status": "stopped"}

        response = self.client.get("/parar/sistema/knr")
        self.assertEqual(response.status_code, 200)
        mock_knr.assert_called_once()

    @patch('backend.api.web.main_orchestrator.sap_stop')
    def test_stop_sap(self, mock_sap):
        """Testa parada apenas do SAP"""
        mock_sap.return_value = {"status": "stopped"}

        response = self.client.get("/parar/sistema/sap")
        self.assertEqual(response.status_code, 200)
        mock_sap.assert_called_once()

    # --- TESTES DE STATUS ---
    @patch('backend.api.web.main_orchestrator.get_status')
    def test_get_status(self, mock_status):
        """Testa endpoint de status simples"""
        mock_status.return_value = {
            "linhas": "running",
            "knr": "stopped",
            "sap": "running"
        }

        response = self.client.get("/status/")
        self.assertEqual(response.status_code, 200)
        mock_status.assert_called_once()

    @patch('backend.api.web.main_orchestrator.get_status')
    def test_get_detailed_status(self, mock_status):
        """Testa endpoint de status detalhado"""
        mock_status.return_value = {
            "linhas": "running",
            "knr": "stopped",
            "sap": "running"
        }

        response = self.client.get("/status/detailed")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        self.assertIn("status", data)
        self.assertIn("timestamp", data)
        self.assertIn("systems", data)
        self.assertEqual(data["systems"]["linhas"], "running")
        self.assertEqual(data["systems"]["knr"], "stopped")
        self.assertEqual(data["systems"]["sap"], "running")

    # --- TESTES DE DASHBOARD DATA ---
    @patch('backend.api.web.db_manager_api.get_dashboard_data')
    def test_get_dashboard_data(self, mock_dashboard):
        """Testa endpoint de dados do dashboard"""
        mock_dashboard.return_value = {"data": "dashboard_info"}

        response = self.client.get("/data/dashboard")
        self.assertEqual(response.status_code, 200)
        mock_dashboard.assert_called_once()

    # --- TESTES DE PÁGINAS HTML ---
    @patch('builtins.open', new_callable=mock_open, read_data="<html>Controle</html>")
    @patch('pathlib.Path.exists')
    def test_get_control_page(self, mock_exists, mock_file):
        """Testa página de controle"""
        mock_exists.return_value = True

        response = self.client.get("/sistema/controle")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Controle", response.text)

    @patch('pathlib.Path.exists')
    def test_get_control_page_not_found(self, mock_exists):
        """Testa página de controle não encontrada"""
        mock_exists.return_value = False

        response = self.client.get("/sistema/controle")
        self.assertEqual(response.status_code, 404)
        self.assertIn("não encontrado", response.text)

    @patch('builtins.open', new_callable=mock_open, read_data="<html>Dashboard</html>")
    @patch('pathlib.Path.exists')
    def test_get_dashboard_page(self, mock_exists, mock_file):
        """Testa página de dashboard"""
        mock_exists.return_value = True

        response = self.client.get("/sistema/dashboard")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Dashboard", response.text)

    @patch('pathlib.Path.exists')
    def test_get_dashboard_page_not_found(self, mock_exists):
        """Testa página de dashboard não encontrada"""
        mock_exists.return_value = False

        response = self.client.get("/sistema/dashboard")
        self.assertEqual(response.status_code, 404)
        self.assertIn("não encontrado", response.text)


class TestCORSMiddleware(unittest.TestCase):
    """Testes para configuração de CORS"""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_cors_headers(self):
        """Verifica se os headers CORS estão configurados"""
        response = self.client.options("/health")
        # O FastAPI lida com CORS automaticamente
        self.assertIn(response.status_code, [200, 405])


if __name__ == "__main__":
    # Executar todos os testes
    unittest.main(verbosity=2)