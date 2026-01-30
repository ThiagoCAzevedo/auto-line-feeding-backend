import unittest
from unittest.mock import patch, MagicMock, mock_open, Mock
from pathlib import Path
from datetime import datetime, date
import tempfile
import os
from backend.database import DatabaseManagerApi, db_manager_api

class TestDatabaseManagerApi(unittest.TestCase):
    """Testes para DatabaseManagerApi"""

    def setUp(self):
        """Configuração antes de cada teste"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_knr_log = Path(self.temp_dir) / "knr.log"
        self.mock_linha_log = Path(self.temp_dir) / "linhas.log"
        
        self.mock_knr_log.touch()
        self.mock_linha_log.touch()

    def tearDown(self):
        """Limpeza após cada teste"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch.dict(os.environ, {
        'KNR_LOG_PATH': '/logs/knr.log',
        'LINES_LOG_PATH': '/logs/linhas.log'
    })
    @patch('backend.database.db_manager.db_manager_api.log_files')
    def test_init(self, mock_log_files):
        """Testa inicialização da classe"""
        api = DatabaseManagerApi()
        
        self.assertIsInstance(api.actual_date, date)
        self.assertIsNotNone(api.db_helper)
        self.assertIsInstance(api.log_knr_path, Path)
        self.assertIsInstance(api.log_linha_path, Path)

    def test_read_log_file_knr(self):
        """Testa leitura do arquivo de log KNR"""
        log_content = "\n".join([f"Log line {i}" for i in range(30)])
        self.mock_knr_log.write_text(log_content)
        
        api = DatabaseManagerApi()
        api.log_knr_path = self.mock_knr_log
        
        result = api.read_log_file("knr", lines=10)
        
        self.assertEqual(len(result), 10)
        self.assertIn("Log line 29", result[-1])

    def test_read_log_file_linhas(self):
        """Testa leitura do arquivo de log de linhas"""
        log_content = "\n".join([f"Linha log {i}" for i in range(20)])
        self.mock_linha_log.write_text(log_content)
        
        api = DatabaseManagerApi()
        api.log_linha_path = self.mock_linha_log
        
        result = api.read_log_file("linhas", lines=5)
        
        self.assertEqual(len(result), 5)
        self.assertIn("Linha log 19", result[-1])

    def test_read_log_file_invalid_system(self):
        """Testa leitura com sistema inválido"""
        api = DatabaseManagerApi()
        result = api.read_log_file("invalid_system")
        
        self.assertEqual(result, [])

    def test_read_log_file_exception(self):
        """Testa comportamento quando há exceção"""
        api = DatabaseManagerApi()
        api.log_knr_path = Path("/path/that/does/not/exist.log")
        
        result = api.read_log_file("knr")
        
        self.assertEqual(result, [])

    def test_read_log_file_empty_lines(self):
        """Testa que linhas vazias são filtradas"""
        log_content = "Line 1\n\n\nLine 2\n\n"
        self.mock_knr_log.write_text(log_content)
        
        api = DatabaseManagerApi()
        api.log_knr_path = self.mock_knr_log
        
        result = api.read_log_file("knr")
        
        self.assertEqual(len(result), 2)
        self.assertIn("Line 1", result)
        self.assertIn("Line 2", result)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_tacto_with_specific_tacto(self, mock_db_helper):
        """Testa busca por tacto específico"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("T01", "P01", 100.5, "PN01"),
            ("T01", "P02", 200.0, "PN02")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_by_tacto("T01")
        
        self.assertIn("tactos", result)
        self.assertEqual(len(result["tactos"]), 1)
        self.assertEqual(result["tactos"][0]["tacto"], "T01")
        self.assertEqual(len(result["tactos"][0]["prateleiras"]), 2)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_tacto_all_tactos(self, mock_db_helper):
        """Testa busca de todos os tactos"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("T01", "P01", 100.5, "PN01"),
            ("T02", "P03", 150.0, "PN03")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_by_tacto(None)
        
        self.assertIn("tactos", result)
        self.assertEqual(len(result["tactos"]), 2)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_tacto_error(self, mock_db_helper):
        """Testa comportamento com erro na busca por tacto"""
        mock_db_helper.get_connection.side_effect = Exception("Database error")
        
        api = DatabaseManagerApi()
        result = api.get_by_tacto("T01")
        
        self.assertIn("error", result)
        self.assertEqual(result["tactos"], [])

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_prateleira_specific(self, mock_db_helper):
        """Testa busca por prateleira específica"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("T01", 100.5, "PN01"),
            ("T01", 200.0, "PN02")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_by_prateleira("P01")
        
        self.assertEqual(result["prateleira"], "P01")
        self.assertEqual(result["tacto"], "T01")
        self.assertEqual(len(result["parts"]), 2)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_prateleira_all(self, mock_db_helper):
        """Testa busca de todas as prateleiras"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("P01", "T01", 100.5, "PN01"),
            ("P02", "T02", 200.0, "PN02")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_by_prateleira(None)
        
        self.assertIn("prateleiras", result)
        self.assertEqual(len(result["prateleiras"]), 2)

    @patch('backend.database.db_manager_api.db_helper')
    def test_get_by_prateleira_not_found(self, mock_db_helper):
        """Testa busca de prateleira não encontrada"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []
        
        api = DatabaseManagerApi()
        result = api.get_by_prateleira("P999")
        
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Nenhuma prateleira encontrada")

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_prateleira_error(self, mock_db_helper):
        """Testa comportamento com erro na busca por prateleira"""
        mock_db_helper.get_connection.side_effect = Exception("Database error")
        
        api = DatabaseManagerApi()
        result = api.get_by_prateleira("P01")
        
        self.assertIn("error", result)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_lt22_stil_opened_success(self, mock_db_helper):
        """Testa busca de OTs abertas com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("OT01", "PN01", 10, "user1", "P01"),
            ("OT02", "PN02", 20, "user2", "P02")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_lt22_stil_opened()
        
        self.assertIn("abertas", result)
        self.assertEqual(len(result["abertas"]), 2)
        self.assertEqual(result["abertas"][0]["num_ot"], "OT01")
        self.assertEqual(result["abertas"][1]["num_ot"], "OT02")

    @patch('backend.database.db_manager_api.db_helper')
    def test_get_lt22_stil_opened_empty(self, mock_db_helper):
        """Testa busca de OTs abertas quando não há registros"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []
        
        api = DatabaseManagerApi()
        result = api.get_lt22_stil_opened()
        
        self.assertEqual(result["abertas"], [])
        self.assertIn("timestamp", result)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_lt22_stil_opened_error(self, mock_db_helper):
        """Testa comportamento com erro na busca de OTs"""
        mock_db_helper.get_connection.side_effect = Exception("Database error")
        
        api = DatabaseManagerApi()
        result = api.get_lt22_stil_opened()
        
        self.assertIn("error", result)
        self.assertEqual(result["abertas"], [])

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_dashboard_data_success(self, mock_db_helper):
        """Testa busca de dados do dashboard com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("T01", 100.5, "2G5827550B", "P01"),
            ("T02", 200.0, "2G5827550B", "P02")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_dashboard_data()
        
        self.assertIn("prateleiras", result)
        self.assertIn("P11", result["prateleiras"])
        self.assertEqual(len(result["prateleiras"]["P11"]), 2)

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_dashboard_data_error(self, mock_db_helper):
        """Testa comportamento com erro na busca de dashboard"""
        mock_db_helper.get_connection.side_effect = Exception("Database error")
        
        api = DatabaseManagerApi()
        result = api.get_dashboard_data()
        
        self.assertIn("error", result)
        self.assertEqual(result["partnumber"], "2G5827550B")

    def test_singleton_instance_exists(self):
        """Testa se a instância singleton foi criada"""
        self.assertIsInstance(db_manager_api, DatabaseManagerApi)


class TestDatabaseManagerApiEdgeCases(unittest.TestCase):
    """Testes de casos extremos"""

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_tacto_with_null_values(self, mock_db_helper):
        """Testa busca com valores NULL"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            (None, None, None, None),
            ("T01", "P01", 100.5, "PN01")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_by_tacto(None)
        
        self.assertEqual(len(result["tactos"]), 2)
        self.assertIn("N/A", [t["tacto"] for t in result["tactos"]])

    @patch('backend.database.db_manager.db_manager_api.db_helper')
    def test_get_by_prateleira_with_none_partnumber(self, mock_db_helper):
        """Testa busca quando partnumber é None"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_rows = [
            ("T01", 100.5, None),
            ("T01", 200.0, "PN02")
        ]
        mock_conn.execute.return_value.fetchall.return_value = mock_rows
        
        api = DatabaseManagerApi()
        result = api.get_by_prateleira("P01")
        
        self.assertEqual(len(result["parts"]), 1)

    @patch('backend.database.db_manager_api.db_helper')
    def test_timestamp_format(self, mock_db_helper):
        """Testa se timestamp está em formato ISO"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []
        
        api = DatabaseManagerApi()
        result = api.get_lt22_stil_opened()
        
        self.assertIn("timestamp", result)
        datetime.fromisoformat(result["timestamp"])


if __name__ == "__main__":
    unittest.main(verbosity=2)