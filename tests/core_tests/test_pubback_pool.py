import time
import unittest
from threading import Thread

from smarteventbus import PotentialLoop, PubType
from smarteventbus.core.publishback import PubBackPool


class MockToken:
    def __init__(self):
        self.content = None
        self.type = None
        self.history = tuple()

    def write_content(self, type, content, history):
        self.type = type
        self.content = content
        self.history = history


class MockEvent:
    def __init__(self, pubback_ttl=6):
        self.token = MockToken()
        self.id = 203480394
        self.pubback_ttl = pubback_ttl


class TestPubBackPool(unittest.TestCase):
    def setUp(self):
        """Инициализация чистого пула перед каждым тестом"""
        self.pool = PubBackPool()

    def test_successful_pubback_and_extract(self):
        """Проверка штатной работы: добавление события и очистка пула при extract"""
        event = MockEvent()
        # Имитируем контекст старого токена (например, уровень вложенности 6)
        old_context = {
            "type": PubType.PUBBACK,
            "content": {"ttl": 6},
            "history": (0, 1098093480293),
        }

        # Публикуем
        self.pool.pubback(event, old_context)

        # Проверяем размер через property info
        self.assertEqual(self.pool.info["qsize"], 1)
        # Проверяем, что TTL уменьшился на 1
        self.assertEqual(event.token.content["ttl"], 5)

        # Извлекаем данные
        extracted_events = self.pool.extract()
        self.assertEqual(len(extracted_events), 1)
        self.assertIn(event, extracted_events)

        # Проверяем, что после extract пул кристально чист
        self.assertEqual(self.pool.info["qsize"], 0)

    def test_potential_loop_exception(self):
        """Проверка защиты от бесконечного цикла (TTL <= 0)"""
        event = MockEvent()

        # Ситуация: хэндлер вернул событие, но у родительского токена TTL уже равен 0.
        # Следующий шаг должен сделать его -1 и выбросить исключение.
        bad_context = {
            "type": PubType.PUBBACK,
            "content": {"ttl": 0},
            "history": (0, 1098093480293),
        }

        with self.assertRaises(PotentialLoop) as context:
            self.pool.pubback(event, bad_context)

        # Проверяем, что в ошибку улетело именно твое сообщение
        self.assertIn("Event reverse publication TTL expired", str(context.exception))
        # Проверяем, что событие НЕ попало в пул
        self.assertEqual(self.pool.info["qsize"], 0)

    def test_concurrent_pubback(self):
        """Стресс-тест: проверка потокобезопасности при одновременной записи из разных потоков"""
        thread_count = 50
        events_per_thread = 10
        old_context = {
            "type": PubType.PUBBACK,
            "content": {"ttl": 6},
            "history": (0, 1098093480293),
        }

        def worker():
            for _ in range(events_per_thread):
                self.pool.pubback(MockEvent(), old_context)

        # Запускаем пачку потоков
        threads = [Thread(target=worker) for _ in range(thread_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Проверяем, что ни одно событие не потерялось из-за гонки потоков
        expected_total = thread_count * events_per_thread
        self.assertEqual(self.pool.info["qsize"], expected_total)


if __name__ == "__main__":
    unittest.main()
