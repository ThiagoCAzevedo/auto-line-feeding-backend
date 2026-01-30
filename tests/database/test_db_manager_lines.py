from unittest.mock import patch, MagicMock, call
import unittest
import polars as pl
from backend.database import DatabaseManagerLinhas, db_manager_lines


class TestDatabaseManagerLinhas(unittest.TestCase):
    """Testes para o DatabaseManagerLinhas"""

    def setUp(self):
        """Configuração inicial para cada teste"""
        self.db_manager = DatabaseManagerLinhas()

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_create_linha_tables_success(self, mock_logger, mock_get_conn):
        """Testa criação de tabelas com sucesso"""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        self.db_manager.create_linha_tables()

        # Verifica se o schema foi criado
        mock_conn.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS linha")
        mock_conn.commit.assert_called_once()
        
        # Verifica logs
        mock_logger.debug.assert_any_call("Verificando e criando schemas no banco de dados, se necessário")

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_create_linha_tables_error(self, mock_logger, mock_get_conn):
        """Testa criação de tabelas com erro"""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Erro no banco")
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        with self.assertRaises(Exception):
            self.db_manager.create_linha_tables()

        mock_logger.error.assert_called()
        self.assertIn("Falha ao criar tabelas", str(mock_logger.error.call_args))

    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_create_linhas_tables_executes_all_queries(self, mock_logger):
        """Testa se todas as queries de criação são executadas"""
        mock_conn = MagicMock()

        self.db_manager._create_linhas_tables(mock_conn)

        # Verifica se todas as tabelas foram criadas
        calls = mock_conn.execute.call_args_list
        executed_queries = [str(call[0][0]) for call in calls]

        self.assertTrue(any("linha_montagem" in query for query in executed_queries))
        self.assertTrue(any("linha_pintura" in query for query in executed_queries))
        self.assertTrue(any("saldo_lb" in query for query in executed_queries))
        
        # Verifica índices
        self.assertTrue(any("idx_linha_montagem_knr" in query for query in executed_queries))
        self.assertTrue(any("idx_pintura" in query for query in executed_queries))
        self.assertTrue(any("idx_saldo_lb" in query for query in executed_queries))

        mock_logger.debug.assert_called_with("Todos os objetos do schema 'linha' garantidos")

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_update_linha_montagem_success(self, mock_logger, mock_get_conn):
        """Testa inserção/atualização de linha de montagem com sucesso"""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_test = pl.DataFrame({
            "tacto": ["T001", "T002"],
            "knr": ["K001", "K002"],
            "lfdnr_sequencia": ["001", "002"],
            "modelo": ["M1", "M2"],
            "lane": ["L1", "L2"]
        })

        self.db_manager.insert_update_infos_linha_montagem(df_test)

        # Verifica registro do dataframe
        mock_conn.register.assert_called_once()
        self.assertEqual(mock_conn.register.call_args[0][0], "df_infos_linha_montagem")

        # Verifica execução da query
        mock_conn.execute.assert_called_once()
        executed_query = mock_conn.execute.call_args[0][0]
        self.assertIn("INSERT INTO linha.linha_montagem", executed_query)
        self.assertIn("ON CONFLICT", executed_query)

        # Verifica commit e unregister
        mock_conn.commit.assert_called_once()
        mock_conn.unregister.assert_called_once_with("df_infos_linha_montagem")

        # Verifica logs
        mock_logger.debug.assert_called()
        mock_logger.info.assert_called_with("Dados da linha de montagem inseridos/atualizados com sucesso")

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_update_linha_montagem_error(self, mock_logger, mock_get_conn):
        """Testa inserção/atualização de linha de montagem com erro"""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Erro no insert")
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_test = pl.DataFrame({
            "tacto": ["T001"],
            "knr": ["K001"],
            "lfdnr_sequencia": ["001"],
            "modelo": ["M1"],
            "lane": ["L1"]
        })

        with self.assertRaises(Exception):
            self.db_manager.insert_update_infos_linha_montagem(df_test)

        mock_logger.error.assert_called()
        self.assertIn("Falha ao inserir/atualizar 'linha_montagem'", str(mock_logger.error.call_args))

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_update_linha_pintura_success(self, mock_logger, mock_get_conn):
        """Testa inserção/atualização de linha de pintura com sucesso"""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_test = pl.DataFrame({
            "knr": ["K001", "K002"],
            "sequencia_prevista": ["001", "002"]
        })

        self.db_manager.insert_update_infos_linha_pintura(df_test)

        # Verifica registro do dataframe
        mock_conn.register.assert_called_once()
        self.assertEqual(mock_conn.register.call_args[0][0], "df_infos_linha_pintura")

        # Verifica execução da query
        mock_conn.execute.assert_called_once()
        executed_query = mock_conn.execute.call_args[0][0]
        self.assertIn("INSERT INTO linha.linha_pintura", executed_query)
        self.assertIn("ON CONFLICT", executed_query)

        # Verifica commit e unregister
        mock_conn.commit.assert_called_once()
        mock_conn.unregister.assert_called_once_with("df_infos_linha_pintura")

        # Verifica logs
        mock_logger.debug.assert_called()
        mock_logger.info.assert_called_with("Dados da linha de pintura inseridos/atualizados com sucesso")

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_update_linha_pintura_error(self, mock_logger, mock_get_conn):
        """Testa inserção/atualização de linha de pintura com erro"""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Erro no insert")
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_test = pl.DataFrame({
            "knr": ["K001"],
            "sequencia_prevista": ["001"]
        })

        with self.assertRaises(Exception):
            self.db_manager.insert_update_infos_linha_pintura(df_test)

        mock_logger.error.assert_called()
        self.assertIn("Falha ao inserir/atualizar 'linha_pintura'", str(mock_logger.error.call_args))

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_saldo_lb_success(self, mock_logger, mock_get_conn):
        """Testa inserção de saldo LB com sucesso"""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_test = pl.DataFrame({
            "tacto": ["T001", "T002"],
            "knr": ["K001", "K002"],
            "knr_fx4pd": ["FX001", "FX002"],
            "partnumber": ["P001", "P002"],
            "quantidade": [10.5, 20.3],
            "quantidade_unidade": ["UN", "KG"]
        })

        self.db_manager.insert_saldo_lb(df_test)

        # Verifica registro do dataframe
        mock_conn.register.assert_called_once()
        self.assertEqual(mock_conn.register.call_args[0][0], "df_consumo_saldo")

        # Verifica execução da query
        mock_conn.execute.assert_called_once()
        executed_query = mock_conn.execute.call_args[0][0]
        self.assertIn("INSERT INTO linha.saldo_lb", executed_query)
        self.assertIn("ON CONFLICT", executed_query)

        # Verifica commit e unregister
        mock_conn.commit.assert_called_once()
        mock_conn.unregister.assert_called_once_with("df_consumo_saldo")

        # Verifica logs
        mock_logger.debug.assert_called()
        mock_logger.info.assert_called_with("Dados de saldo_lb inseridos/atualizados com sucesso")

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_saldo_lb_empty_dataframe(self, mock_logger, mock_get_conn):
        """Testa inserção de saldo LB com DataFrame vazio"""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_empty = pl.DataFrame()

        self.db_manager.insert_saldo_lb(df_empty)

        # Não deve executar queries se DataFrame está vazio
        mock_conn.register.assert_not_called()
        mock_conn.execute.assert_not_called()
        mock_conn.commit.assert_not_called()
        mock_logger.debug.assert_not_called()

    @patch('backend.database.db_manager.db_manager_lines.db_helper.get_connection')
    @patch('backend.database.db_manager.db_manager_lines.logger')
    def test_insert_saldo_lb_error(self, mock_logger, mock_get_conn):
        """Testa inserção de saldo LB com erro"""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Erro no insert")
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        df_test = pl.DataFrame({
            "tacto": ["T001"],
            "knr": ["K001"],
            "knr_fx4pd": ["FX001"],
            "partnumber": ["P001"],
            "quantidade": [10.5],
            "quantidade_unidade": ["UN"]
        })

        with self.assertRaises(Exception):
            self.db_manager.insert_saldo_lb(df_test)

        mock_logger.error.assert_called()
        self.assertIn("Falha ao inserir/atualizar 'saldo_lb'", str(mock_logger.error.call_args))

    def test_db_manager_lines_instance(self):
        """Testa se a instância global foi criada"""
        self.assertIsInstance(db_manager_lines, DatabaseManagerLinhas)

    def test_db_manager_has_db_helper(self):
        """Testa se o db_manager tem db_helper"""
        self.assertIsNotNone(self.db_manager.db_helper)


if __name__ == "__main__":
    unittest.main(verbosity=2)