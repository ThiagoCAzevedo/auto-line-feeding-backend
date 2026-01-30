from unittest.mock import patch, MagicMock, call
import unittest
import duckdb
from backend.database import DatabaseManagerGeneral, db_manager_general


class TestDatabaseManagerGeneral(unittest.TestCase):
    """Testes para o DatabaseManagerGeneral"""

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def setUp(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Configuração inicial para cada teste"""
        # Mock da conexão para evitar criação real do banco
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        self.db_manager = DatabaseManagerGeneral()
        self.mock_logger = mock_logger
        self.mock_db_helper = mock_db_helper
        self.mock_knr = mock_knr
        self.mock_lines = mock_lines

    # --- TESTES DE INICIALIZAÇÃO ---

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_init_success(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa inicialização com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        db_manager = DatabaseManagerGeneral()

        # Verifica atribuições
        self.assertEqual(db_manager.knr_manager, mock_knr)
        self.assertEqual(db_manager.line_manager, mock_lines)
        self.assertEqual(db_manager.db_helper, mock_db_helper)

        # Verifica logs
        mock_logger.debug.assert_any_call("Inicializando DatabaseManagerGeneral")

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_init_calls_ensure_database(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa se __init__ chama _ensure_database_exists"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        db_manager = DatabaseManagerGeneral()

        # Verifica criação de schemas
        mock_conn.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS linha")
        mock_conn.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS knr")
        mock_conn.commit.assert_called()

    # --- TESTES DE ENSURE DATABASE EXISTS ---

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_ensure_database_exists_success(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa criação de estrutura do banco com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        db_manager = DatabaseManagerGeneral()

        # Verifica criação de schemas
        calls = mock_conn.execute.call_args_list
        executed_queries = [str(call[0][0]) for call in calls]
        
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS linha" in query for query in executed_queries))
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS knr" in query for query in executed_queries))

        # Verifica chamadas aos managers
        mock_knr.create_knr_tables.assert_called_once()
        mock_lines.create_linha_tables.assert_called_once()

        # Verifica commit
        mock_conn.commit.assert_called()

        # Verifica logs
        mock_logger.info.assert_any_call("Schemas criados no banco de dados")
        mock_logger.info.assert_any_call("Estrutura do banco de dados pronta")

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_ensure_database_exists_error(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa erro na criação de estrutura do banco"""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Erro ao criar schema")
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        with self.assertRaises(Exception):
            db_manager = DatabaseManagerGeneral()

        mock_logger.error.assert_called()
        self.assertIn("Falha ao configurar a estrutura do banco de dados", 
                     str(mock_logger.error.call_args))

    # --- TESTES DE EXECUTE QUERY ---

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_execute_query_success_with_results(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa execução de query com resultados"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("valor1", "valor2"),
            ("valor3", "valor4")
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.description = [("col1",), ("col2",)]
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "SELECT * FROM test"
        params = ("param1",)
        
        result = db_manager.execute_query(query, params)

        # Verifica execução da query
        mock_conn.execute.assert_called_once_with(query, params)
        
        # Verifica resultado
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {"col1": "valor1", "col2": "valor2"})
        self.assertEqual(result[1], {"col1": "valor3", "col2": "valor4"})

        # Verifica logs
        mock_logger.debug.assert_called_with(f"Executando consulta: {query} | Parâmetros: {params}")
        mock_logger.info.assert_called_with(f"Consulta retornou 2 linha(s)")

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_execute_query_success_no_results(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa execução de query sem resultados"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "SELECT * FROM empty_table"
        
        result = db_manager.execute_query(query)

        # Verifica resultado vazio
        self.assertEqual(result, [])

        # Verifica logs
        mock_logger.info.assert_called_with("Consulta não retornou resultados")

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_execute_query_empty_params(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa execução de query sem parâmetros"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "SELECT * FROM test"
        
        db_manager.execute_query(query)

        # Verifica que foi chamado com tupla vazia
        mock_conn.execute.assert_called_once_with(query, ())

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_execute_query_transaction_exception(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa erro de TransactionException com write-write conflict"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = duckdb.TransactionException("write-write conflict detected")
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "UPDATE test SET col1 = 'value'"

        with self.assertRaises(duckdb.TransactionException):
            db_manager.execute_query(query)

        # Verifica logs de warning e error
        mock_logger.warning.assert_called_with("Conflito de escrita detectado no execute_query")
        mock_logger.error.assert_called()

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_execute_query_transaction_exception_no_conflict(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa erro de TransactionException sem write-write conflict"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = duckdb.TransactionException("outro erro de transação")
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "UPDATE test SET col1 = 'value'"

        with self.assertRaises(duckdb.TransactionException):
            db_manager.execute_query(query)

        # Não deve chamar warning
        mock_logger.warning.assert_not_called()
        # Mas deve chamar error
        mock_logger.error.assert_called()

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_execute_query_generic_exception(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa erro genérico na execução da query"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Erro genérico")
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "SELECT * FROM test"

        with self.assertRaises(Exception):
            db_manager.execute_query(query)

        # Verifica log de erro
        mock_logger.error.assert_called()
        self.assertIn("Erro na execução da consulta", str(mock_logger.error.call_args))

    # --- TESTES DE THREAD SAFETY ---

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    @patch('backend.database.db_manager.db_manager_general.DatabaseManagerGeneral._lock')
    def test_execute_query_uses_lock(self, mock_lock, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa se execute_query usa thread lock"""
        # Setup inicial
        mock_conn_init = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn_init
        db_manager = DatabaseManagerGeneral()

        # Setup para execute_query
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        query = "SELECT * FROM test"
        
        db_manager.execute_query(query)

        # Verifica se o lock foi utilizado
        mock_lock.__enter__.assert_called()
        mock_lock.__exit__.assert_called()

    # --- TESTES DE INSTÂNCIA GLOBAL ---

    def test_db_manager_general_instance(self):
        """Testa se a instância global foi criada"""
        self.assertIsInstance(db_manager_general, DatabaseManagerGeneral)

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    def test_db_manager_has_managers(self, mock_db_helper, mock_knr, mock_lines):
        """Testa se o db_manager tem os managers"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        db_manager = DatabaseManagerGeneral()
        
        self.assertIsNotNone(db_manager.knr_manager)
        self.assertIsNotNone(db_manager.line_manager)
        self.assertIsNotNone(db_manager.db_helper)

    # --- TESTES DE INTEGRAÇÃO ---

    @patch('backend.database.db_manager.db_manager_general.db_manager_lines')
    @patch('backend.database.db_manager.db_manager_general.db_manager_knr')
    @patch('backend.database.db_manager.db_manager_general.db_helper')
    @patch('backend.database.db_manager.db_manager_general.logger')
    def test_full_initialization_flow(self, mock_logger, mock_db_helper, mock_knr, mock_lines):
        """Testa fluxo completo de inicialização"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn

        db_manager = DatabaseManagerGeneral()

        # Verifica ordem de execução
        self.assertTrue(mock_logger.debug.call_count >= 2)
        self.assertTrue(mock_conn.execute.call_count >= 2)
        mock_conn.commit.assert_called()
        mock_knr.create_knr_tables.assert_called_once()
        mock_lines.create_linha_tables.assert_called_once()
        mock_logger.info.assert_any_call("Estrutura do banco de dados pronta")


if __name__ == "__main__":
    unittest.main(verbosity=2)