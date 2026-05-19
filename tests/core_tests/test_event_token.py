import unittest
from threading import Thread

# Предполагается, что ваш код лежит в файле token_module.py
# Скорректируйте импорт под вашу структуру проекта:
from smarteventbus import (
    CannotComplete,
    FlatDict,
    RePublishing,
    UnReadToken,
)
from smarteventbus.core.eventparent import EventToken, TokenState


class TestEventTokenLifecycle(unittest.TestCase):
    def test_happy_path(self):
        """Проверка успешного жизненного цикла токена (Зеленый путь на схеме)"""
        token = EventToken()

        # 1. Initialized
        self.assertEqual(token.state_num, TokenState.INITIALIZED)
        self.assertEqual(
            token.state,
            "Initialized",
        )
        self.assertIsNone(token._content)

        # 2. Write content -> In Queue
        test_data = FlatDict(event_id=42, payload="data")
        token.write_content(test_data)
        self.assertEqual(token.state_num, TokenState.IN_QUEUE)
        self.assertEqual(token._content, test_data)

        # 3. Read content -> In Work
        read_data = token.read_content()
        self.assertEqual(token.state_num, TokenState.IN_WORK)
        self.assertEqual(read_data, test_data)
        self.assertIsNone(token._content)  # СХЕМА: Контент должен занулиться!

        # 4. Complete -> Successfully Completed
        token.complete()
        self.assertEqual(token.state_num, TokenState.SUCCESSFULLY_COMPLETED)
        self.assertIsNone(token._content)

    def test_devalided_after_write_trying(self):
        """Проверка повторной записи (Re-publishing attempt -> 101)"""
        token = EventToken()
        token.write_content(FlatDict(key="first"))

        # Пытаемся записать второй раз в токен, который уже в очереди
        with self.assertRaises(RePublishing):
            token.write_content(FlatDict(key="second"))

        self.assertEqual(token.state_num, TokenState.DEVALIDED_AFTER_WRITE_TRYING)
        self.assertIsNone(token._content)  # СХЕМА: Clear Content

    def test_devalided_after_read_trying(self):
        """Проверка повторного/несвоевременного чтения (Re-read attempt -> 102)"""
        token = EventToken()
        # Токен в состоянии INITIALIZED, читать еще нельзя
        with self.assertRaises(UnReadToken):
            token.read_content()

        self.assertEqual(token.state_num, TokenState.DEVALIDED_AFTER_READ_TRYING)
        self.assertIsNone(token._content)  # СХЕМА: Clear Content

    def test_devalided_after_handler_error(self):
        """Проверка фиксации ошибки воркера (Execution error -> 103)"""
        token = EventToken()
        token.write_content(FlatDict(key="data"))
        token.read_content()  # Перевели в IN_WORK

        # Симулируем ошибку внутри обработчика
        token.error()

        self.assertEqual(token.state_num, TokenState.DEVALIDED_AFTER_HANDLER_ERROR)
        self.assertIsNone(token._content)  # СХЕМА: Clear Content

    def test_devalided_after_finish_trying(self):
        """Проверка вызова complete() / error() не вовремя (-> 104)"""
        token = EventToken()
        # Токен только создан, завершать работу еще нельзя
        with self.assertRaises(CannotComplete):
            token.complete()

        self.assertEqual(token.state_num, TokenState.DEVALIDED_AFTER_FINISH_TRYING)
        self.assertIsNone(token._content)

    def test_manually_devalided(self):
        """Проверка принудительного вызова devalid() -> 1000"""
        token = EventToken()
        token.write_content(FlatDict(key="secret"))

        token.devalid()

        self.assertEqual(token.state_num, TokenState.MANUALLY_DEVALIDED)
        self.assertIsNone(token._content)  # СХЕМА: Clear Content

    def test_thread_safety_basic(self):
        """Базовая проверка потокобезопасности при одновременном чтении"""
        token = EventToken()
        token.write_content(FlatDict(secure="payload"))

        results = []

        def worker():
            try:
                res = token.read_content()
                results.append(("success", res))
            except UnReadToken:
                results.append(("error", None))

        # Запускаем два потока, которые одновременно пытаются прочитать токен
        t1 = Thread(target=worker)
        t2 = Thread(target=worker)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Один поток должен был успешно забрать данные, а второй — уйти в ошибку и инвалидировать токен
        success_count = sum(1 for r in results if r[0] == "success")
        error_count = sum(1 for r in results if r[0] == "error")

        self.assertEqual(success_count, 1)
        self.assertEqual(error_count, 1)
        # Итоговый статус токена должен быть "Ошибка чтения", так как второй поток опоздал
        self.assertEqual(token.state_num, TokenState.DEVALIDED_AFTER_READ_TRYING)


if __name__ == "__main__":
    unittest.main()
