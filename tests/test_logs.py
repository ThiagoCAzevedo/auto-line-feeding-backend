import unittest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import logging
import os
import tempfile
import shutil
from backend.log import LogFilesProcessor, log_files

class TestLogFilesProcessor(unittest.TestCase):
    """Testes para a classe LogFilesProcessor"""

    def setUp(self):
        """Configuração antes de cada teste"""
        # Limpar handlers de loggers anteriores
        for logger_name in ['test_logger', 'lines_logger', 'knr_logger']:
            logger = logging.getLogger(logger_name)
            logger.handlers.clear()
            logger.setLevel(logging.NOTSET)

        # Criar diretório temporário para logs
        self.temp_dir = tempfile.mkdtemp()
        self.test_log_path = Path(self.temp_dir) / "test.log"

    def tearDown(self):
        """Limpeza após cada teste"""

        # Fechar handlers de todos os loggers criados
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)

        # Fechar handlers do root logger (por segurança)
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)

        # Remover diretório temporário
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch('backend.log.log.load_dotenv')
    def test_init_loads_env_variables(self, mock_load_dotenv):
        """Testa se __init__ carrega as variáveis de ambiente corretamente"""
        processor = LogFilesProcessor()
        
        # Verificar se os paths foram configurados
        self.assertIn("lines", processor._logger_paths)
        self.assertIn("knr", processor._logger_paths)
        self.assertIn("orchestrator", processor._logger_paths)
        self.assertIn("db", processor._logger_paths)
        self.assertIn("sap", processor._logger_paths)
        
        # Verificar se são objetos Path
        self.assertIsInstance(processor._logger_paths["lines"], Path)
        self.assertIsInstance(processor._logger_paths["knr"], Path)

    def test_logger_paths_are_resolved(self):
        """Testa se os paths são resolvidos corretamente"""
        processor = LogFilesProcessor()
        
        for key, path in processor._logger_paths.items():
            self.assertTrue(path.is_absolute(), 
                          f"Path '{key}' deveria ser absoluto: {path}")

    def test_create_logger_basic(self):
        """Testa criação básica de logger"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        # Verificar que o logger foi criado
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "test_logger")
        self.assertEqual(logger.level, logging.INFO)
        self.assertFalse(logger.propagate)

    def test_create_logger_has_handler(self):
        """Testa se o logger criado tem um handler configurado"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        # Verificar handler
        self.assertEqual(len(logger.handlers), 1)
        handler = logger.handlers[0]
        self.assertIsInstance(handler, logging.FileHandler)

    def test_create_logger_formatter(self):
        """Testa se o formatter está configurado corretamente"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        handler = logger.handlers[0]
        formatter = handler.formatter
        
        # Verificar formato
        self.assertIsNotNone(formatter)
        self.assertIn('%(levelname)s', formatter._fmt)
        self.assertIn('%(name)s', formatter._fmt)
        self.assertIn('%(asctime)s', formatter._fmt)
        self.assertIn('%(message)s', formatter._fmt)
        self.assertEqual(formatter.datefmt, '%Y-%m-%d %H:%M:%S')

    def test_create_logger_no_duplicate_handlers(self):
        """Testa se não cria handlers duplicados"""
        processor = LogFilesProcessor()
        
        # Criar logger duas vezes
        logger1 = processor._create_logger("test_logger", self.test_log_path)
        logger2 = processor._create_logger("test_logger", self.test_log_path)
        
        # Deve ser o mesmo logger e ter apenas 1 handler
        self.assertIs(logger1, logger2)
        self.assertEqual(len(logger2.handlers), 1)

    def test_create_logger_writes_to_file(self):
        """Testa se o logger realmente escreve no arquivo"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        # Escrever uma mensagem
        test_message = "Test log message"
        logger.info(test_message)
        
        # Forçar flush
        for handler in logger.handlers:
            handler.flush()
        
        # Verificar se o arquivo foi criado e contém a mensagem
        self.assertTrue(self.test_log_path.exists())
        with open(self.test_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertIn(test_message, content)
            self.assertIn('[INFO', content)
            self.assertIn('test_logger', content)

    def test_get_logger_with_valid_system(self):
        """Testa get_logger com sistema válido"""
        processor = LogFilesProcessor()
        
        with patch.object(processor, '_create_logger') as mock_create:
            mock_logger = MagicMock()
            mock_create.return_value = mock_logger
            
            logger = processor.get_logger("test_name", "lines")
            
            # Verificar que _create_logger foi chamado
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            self.assertEqual(call_args[0][0], "test_name")
            self.assertIsInstance(call_args[0][1], Path)

    def test_get_logger_with_invalid_system(self):
        """Testa get_logger com sistema inválido"""
        processor = LogFilesProcessor()
        logger = processor.get_logger("test_name", "invalid_system")
        
        # Deve retornar um logger padrão
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "test_name")

    def test_get_logger_all_systems(self):
        """Testa get_logger para todos os sistemas disponíveis"""
        processor = LogFilesProcessor()
        systems = ["lines", "knr", "orchestrator", "db", "sap"]
        
        for system in systems:
            with patch.object(processor, '_create_logger') as mock_create:
                mock_create.return_value = MagicMock()
                logger = processor.get_logger(f"{system}_logger", system)
                mock_create.assert_called_once()

    def test_logger_encoding_utf8(self):
        """Testa se o logger suporta UTF-8"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        # Mensagem com caracteres especiais
        test_message = "Teste com acentuação: á é í ó ú ã õ ç"
        logger.info(test_message)
        
        # Forçar flush
        for handler in logger.handlers:
            handler.flush()
        
        # Verificar se foi escrito corretamente
        with open(self.test_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertIn(test_message, content)

    def test_logger_multiple_messages(self):
        """Testa múltiplas mensagens de log"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        messages = ["Message 1", "Message 2", "Message 3"]
        for msg in messages:
            logger.info(msg)
        
        # Forçar flush
        for handler in logger.handlers:
            handler.flush()
        
        # Verificar todas as mensagens
        with open(self.test_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
            for msg in messages:
                self.assertIn(msg, content)

    def test_logger_different_levels(self):
        """Testa diferentes níveis de log"""
        processor = LogFilesProcessor()
        logger = processor._create_logger("test_logger", self.test_log_path)
        
        logger.debug("Debug message")  # Não deve aparecer (nível INFO)
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        # Forçar flush
        for handler in logger.handlers:
            handler.flush()
        
        with open(self.test_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
            self.assertNotIn("Debug message", content)
            self.assertIn("Info message", content)
            self.assertIn("Warning message", content)
            self.assertIn("Error message", content)

    def test_log_files_singleton_instance(self):
        """Testa se a instância global log_files está disponível"""
        # Verificar que a instância foi criada
        self.assertIsInstance(log_files, LogFilesProcessor)
        self.assertIsNotNone(log_files._logger_paths)


class TestLogFilesProcessorEdgeCases(unittest.TestCase):
    """Testes de casos extremos e erros"""

    @patch.dict(os.environ, {}, clear=True)
    @patch('backend.log.log.load_dotenv')
    def test_init_without_env_variables(self, mock_load_dotenv):
        """Testa inicialização sem variáveis de ambiente"""
        # Isso pode gerar erro dependendo da implementação
        # Ajuste conforme necessário
        try:
            processor = LogFilesProcessor()
            # Se não gerar erro, verificar paths
            for path in processor._logger_paths.values():
                self.assertIsInstance(path, Path)
        except (TypeError, AttributeError):
            # Se espera erro quando env vars não existem
            pass

    def test_create_logger_with_invalid_path(self):
        """Testa criação de logger com path inválido"""
        processor = LogFilesProcessor()
        invalid_path = Path("/invalid/path/that/does/not/exist/test.log")
        
        # Dependendo da implementação, pode gerar erro
        # ou criar o logger sem erro até tentar escrever
        try:
            logger = processor._create_logger("test", invalid_path)
            self.assertIsInstance(logger, logging.Logger)
        except (OSError, PermissionError):
            # Erro esperado em alguns casos
            pass


class TestLogFilesProcessorIntegration(unittest.TestCase):
    """Testes de integração"""
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Limpeza após testes"""
        # self.env_patch.stop()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_full_workflow(self):
        """Testa fluxo completo de criação e uso de loggers"""
        processor = LogFilesProcessor()
        
        # Criar loggers para diferentes sistemas
        lines_logger, lines_path = processor.get_logger("tests | lines", "lines", True)
        knr_logger, knr_path = processor.get_logger("tests | knr", "knr", True)
        
        # Escrever mensagens
        lines_logger.info("Criação de logs para sistema de linhas testado e funcionando")
        knr_logger.error("Criação de logs para sistema de knr testado e funcionando")

        # Forçar flush
        for logger in [lines_logger, knr_logger]:
            for handler in logger.handlers:
                handler.flush()
        
        # Verificar arquivos
        self.assertTrue(lines_path.exists())
        self.assertTrue(knr_path.exists())
        
        # Verificar conteúdo
        with open(lines_path, 'r', encoding='utf-8') as f:
            self.assertIn("Criação de logs para sistema de linhas testado e funcionando", f.read())
        
        with open(knr_path, 'r', encoding='utf-8') as f:
            self.assertIn("Criação de logs para sistema de knr testado e funcionando", f.read())


if __name__ == "__main__":
    # Executar todos os testes
    unittest.main(verbosity=2)