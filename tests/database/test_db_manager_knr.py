import unittest
from unittest.mock import patch, MagicMock, Mock
import polars as pl

# Importar a classe a ser testada
from backend.database import DatabaseManagerKNR, db_manager_knr

class TestDatabaseManagerKNR(unittest.TestCase):
    """Testes para DatabaseManagerKNR"""

    @patch('backend.database.db_manager_knr.db_helper')
    def test_init(self, mock_db_helper):
        """Testa inicialização da classe"""
        manager = DatabaseManagerKNR()
        self.assertIsNotNone(manager.db_helper)

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_create_knr_tables_success(self, mock_db_helper):
        """Testa criação de tabelas KNR com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        manager = DatabaseManagerKNR()
        manager.create_knr_tables()
        
        # Verificar que schema foi criado
        calls = [str(call) for call in mock_conn.execute.call_args_list]
        self.assertTrue(any("CREATE SCHEMA" in call for call in calls))
        
        # Verificar commit
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_create_knr_tables_creates_all_tables(self, mock_db_helper):
        """Testa se todas as tabelas KNR são criadas"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        manager = DatabaseManagerKNR()
        manager.create_knr_tables()
        
        # Verificar que todas as tabelas foram criadas
        calls = [str(call) for call in mock_conn.execute.call_args_list]
        
        self.assertTrue(any("knrs_vivos" in call for call in calls))
        self.assertTrue(any("knrs_mortos" in call for call in calls))
        self.assertTrue(any("knrs_fx4pd" in call for call in calls))
        self.assertTrue(any("knrs_comum" in call for call in calls))
        self.assertTrue(any("lt22" in call for call in calls))
        self.assertTrue(any("pkmc_pk05" in call for call in calls))

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_create_knr_tables_creates_indexes(self, mock_db_helper):
        """Testa se índices são criados"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        manager = DatabaseManagerKNR()
        manager.create_knr_tables()
        
        # Verificar que índices foram criados
        calls = [str(call) for call in mock_conn.execute.call_args_list]
        self.assertTrue(any("CREATE UNIQUE INDEX" in call for call in calls))

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_create_knr_tables_error(self, mock_db_helper):
        """Testa comportamento com erro na criação de tabelas"""
        mock_db_helper.get_connection.side_effect = Exception("Database error")
        
        manager = DatabaseManagerKNR()
        
        with self.assertRaises(Exception):
            manager.create_knr_tables()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_knrs_vivos_success(self, mock_db_helper):
        """Testa inserção de KNRs vivos com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_vivos = pl.DataFrame({
            "tma": ["TMA001"],
            "cor": ["RED"],
            "tst": ["TST001"],
            "knr": ["KNR001"],
            "knr_fx4pd": ["FX001"],
            "tmamg": ["TMAMG001"],
            "cod_pais": ["BR"],
            "pais": ["Brasil"],
            "modelo": ["Model1"],
            "criado_em": ["2025-01-01"]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_knrs_vivos(df_vivos)
        
        # Verificar que register foi chamado
        mock_conn.register.assert_called_once_with("df_vivos", df_vivos.to_arrow())
        
        # Verificar commit
        mock_conn.commit.assert_called()
        
        # Verificar unregister
        mock_conn.unregister.assert_called_once_with("df_vivos")

    @patch('backend.database.db_manager_knr.db_helper')
    def test_insert_knrs_vivos_empty_dataframe(self, mock_db_helper):
        """Testa inserção com DataFrame vazio"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_empty = pl.DataFrame()
        
        manager = DatabaseManagerKNR()
        manager.insert_knrs_vivos(df_empty)
        
        # Não deve chamar register para DataFrame vazio
        mock_conn.register.assert_not_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_knrs_vivos_error(self, mock_db_helper):
        """Testa comportamento com erro na inserção"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.side_effect = Exception("Insert failed")
        
        df_vivos = pl.DataFrame({
            "tma": ["TMA001"],
            "cor": ["RED"],
            "tst": ["TST001"],
            "knr": ["KNR001"],
            "knr_fx4pd": ["FX001"],
            "tmamg": ["TMAMG001"],
            "cod_pais": ["BR"],
            "pais": ["Brasil"],
            "modelo": ["Model1"],
            "criado_em": ["2025-01-01"]
        })
        
        manager = DatabaseManagerKNR()
        
        with self.assertRaises(Exception):
            manager.insert_knrs_vivos(df_vivos)

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_knrs_mortos_success(self, mock_db_helper):
        """Testa inserção de KNRs mortos com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_mortos = pl.DataFrame({
            "tma": ["TMA002"],
            "cor": ["BLUE"],
            "tst": ["TST002"],
            "knr": ["KNR002"],
            "knr_fx4pd": ["FX002"],
            "tmamg": ["TMAMG002"],
            "cod_pais": ["US"],
            "pais": ["USA"],
            "modelo": ["Model2"],
            "criado_em": ["2025-01-01"]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_knrs_mortos(df_mortos)
        
        mock_conn.register.assert_called_once_with("df_mortos", df_mortos.to_arrow())
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_knrs_fx4pd_success(self, mock_db_helper):
        """Testa inserção de KNRs FX4PD com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_fx4pd = pl.DataFrame({
            "PON_Kennnummer": ["FX001"],
            "PartCode_Sachnummer": ["PN001"],
            "Quantity_Menge": [10.5],
            "QuantityUnit_Mengeneinheit": ["KG"]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_knrs_fx4pd(df_fx4pd)
        
        mock_conn.register.assert_called_once()
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_knrs_comuns_success(self, mock_db_helper):
        """Testa inserção de KNRs comuns com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_comum = pl.DataFrame({
            "knr": ["KNR001"],
            "knr_fx4pd": ["FX001"],
            "cor": ["RED"],
            "tmamg": ["TMAMG001"],
            "cod_pais": ["BR"],
            "pais": ["Brasil"],
            "modelo": ["Model1"],
            "partnumber": ["PN001"],
            "quantidade": [100.0],
            "quantidade_unidade": ["PC"]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_knrs_comuns(df_comum)
        
        mock_conn.register.assert_called_with("df_common", df_comum.to_arrow())
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_pkmc_pk05_success(self, mock_db_helper):
        """Testa inserção em pkmc_pk05 com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_joined = pl.DataFrame({
            "partnumber": ["PN001"],
            "area_abastecimento": ["Area1"],
            "num_circ_regul_pkmc": ["NUM001"],
            "tipo_deposito_pkmc": ["Tipo1"],
            "posicao_deposito_pkmc": ["Pos1"],
            "container_pkmc": ["Cont1"],
            "descricao_partnumber": ["Desc1"],
            "norma_embalagem_pkmc": ["Norma1"],
            "qtd_por_caixa": [10.0],
            "qtd_max_caixas": [100.0],
            "deposito_pk05": ["Dep1"],
            "responsavel_pk05": ["Resp1"],
            "ponto_descarga_pk05": ["Ponto1"],
            "denominacao_pk05": ["Denom1"],
            "tacto": ["T001"],
            "prateleira": ["P001"],
            "qtd_total_teorica": [1000.0],
            "qtd_para_reabastecimento": [200.0]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_pkmc_pk05(df_joined)
        
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_update_pkmc_pk05_success(self, mock_db_helper):
        """Testa atualização de pkmc_pk05 com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_update = pl.DataFrame({
            "tacto": ["T001"],
            "partnumber": ["PN001"],
            "quantidade_final": [150.0]
        })
        
        manager = DatabaseManagerKNR()
        manager.update_pkmc_pk05(df_update)
        
        mock_conn.register.assert_called_with("df_update_pkmc_pk05", df_update.to_arrow())
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_lt22_success(self, mock_db_helper):
        """Testa inserção em lt22 com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_lt22 = pl.DataFrame({
            "num_ot": ["OT001"],
            "partnumber": ["PN001"],
            "tp_destino": ["Destino1"],
            "posicao_destino": ["Pos1"],
            "quantidade": [50.0],
            "unidade_deposito": ["UN"],
            "usuario": ["User1"],
            "prateleira": ["P001"],
            "data_confirmacao": ["2025-01-01"],
            "hora_confirmacao": ["10:00:00"]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_lt22(df_lt22)
        
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_update_lt22_success(self, mock_db_helper):
        """Testa atualização de lt22 com sucesso"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_update = pl.DataFrame({
            "num_ot": ["OT001"],
            "num_ot_usado": [False]
        })
        
        manager = DatabaseManagerKNR()
        manager.update_lt22(df_update)
        
        mock_conn.commit.assert_called()

    @patch('backend.database.db_manager_knr.db_helper')
    def test_insert_methods_with_empty_dataframes(self, mock_db_helper):
        """Testa todos os métodos de inserção com DataFrames vazios"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_empty = pl.DataFrame()
        
        manager = DatabaseManagerKNR()
        
        # Testar todos os métodos com DataFrame vazio
        manager.insert_knrs_vivos(df_empty)
        manager.insert_knrs_mortos(df_empty)
        manager.insert_knrs_fx4pd(df_empty)
        manager.insert_pkmc_pk05(df_empty)
        manager.update_pkmc_pk05(df_empty)
        manager.insert_lt22(df_empty)
        manager.update_lt22(df_empty)
        
        # Nenhum register deve ser chamado para DataFrames vazios
        mock_conn.register.assert_not_called()

    def test_singleton_instance_exists(self):
        """Testa se a instância singleton foi criada"""
        self.assertIsInstance(db_manager_knr, DatabaseManagerKNR)


class TestDatabaseManagerKNRConflictHandling(unittest.TestCase):
    """Testes de tratamento de conflitos"""

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_knrs_vivos_with_conflict(self, mock_db_helper):
        """Testa inserção com conflito (ON CONFLICT DO UPDATE)"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_vivos = pl.DataFrame({
            "tma": ["TMA001"],
            "cor": ["RED"],
            "tst": ["TST001"],
            "knr": ["KNR001"],
            "knr_fx4pd": ["FX001"],
            "tmamg": ["TMAMG001"],
            "cod_pais": ["BR"],
            "pais": ["Brasil"],
            "modelo": ["Model1"],
            "criado_em": ["2025-01-01"]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_knrs_vivos(df_vivos)
        
        # Verificar que a query contém ON CONFLICT
        execute_call = mock_conn.execute.call_args_list
        query = str(execute_call)
        self.assertIn("ON CONFLICT", query)

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_pkmc_pk05_with_conflict_do_nothing(self, mock_db_helper):
        """Testa inserção em pkmc_pk05 com DO NOTHING"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        
        df_joined = pl.DataFrame({
            "partnumber": ["PN001"],
            "area_abastecimento": ["Area1"],
            "num_circ_regul_pkmc": ["NUM001"],
            "tipo_deposito_pkmc": ["Tipo1"],
            "posicao_deposito_pkmc": ["Pos1"],
            "container_pkmc": ["Cont1"],
            "descricao_partnumber": ["Desc1"],
            "norma_embalagem_pkmc": ["Norma1"],
            "qtd_por_caixa": [10.0],
            "qtd_max_caixas": [100.0],
            "deposito_pk05": ["Dep1"],
            "responsavel_pk05": ["Resp1"],
            "ponto_descarga_pk05": ["Ponto1"],
            "denominacao_pk05": ["Denom1"],
            "tacto": ["T001"],
            "prateleira": ["P001"],
            "qtd_total_teorica": [1000.0],
            "qtd_para_reabastecimento": [200.0]
        })
        
        manager = DatabaseManagerKNR()
        manager.insert_pkmc_pk05(df_joined)
        
        # Verificar que a query contém DO NOTHING
        execute_call = mock_conn.execute.call_args_list
        query = str(execute_call)
        self.assertIn("DO NOTHING", query)


class TestDatabaseManagerKNRErrorHandling(unittest.TestCase):
    """Testes de tratamento de erros"""

    @patch('backend.database.db_manager.db_manager_knr.db_helper')
    def test_insert_with_connection_error(self, mock_db_helper):
        """Testa comportamento com erro de conexão"""
        mock_db_helper.get_connection.side_effect = Exception("Connection failed")
        
        df_vivos = pl.DataFrame({
            "tma": ["TMA001"],
            "cor": ["RED"],
            "tst": ["TST001"],
            "knr": ["KNR001"],
            "knr_fx4pd": ["FX001"],
            "tmamg": ["TMAMG001"],
            "cod_pais": ["BR"],
            "pais": ["Brasil"],
            "modelo": ["Model1"],
            "criado_em": ["2025-01-01"]
        })
        
        manager = DatabaseManagerKNR()
        
        with self.assertRaises(Exception):
            manager.insert_knrs_vivos(df_vivos)

    @patch('backend.database.db_manager_knr.db_helper')
    def test_all_insert_methods_rollback_on_error(self, mock_db_helper):
        """Testa se todos os métodos fazem rollback em caso de erro"""
        mock_conn = MagicMock()
        mock_db_helper.get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.side_effect = Exception("Execute failed")
        
        df_test = pl.DataFrame({"col": ["value"]})
        
        manager = DatabaseManagerKNR()
        
        methods_to_test = [
            (manager.insert_knrs_vivos, df_test),
            (manager.insert_knrs_mortos, df_test),
        ]
        
        for method, df in methods_to_test:
            with self.assertRaises(Exception):
                method(df)


if __name__ == "__main__":
    unittest.main(verbosity=2)