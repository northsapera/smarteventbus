import asyncio
import threading
import time
import unittest
from unittest.mock import Mock

from smarteventbus import (
    BusNetwork,
    Event,
    Handler,
    SubscribeType,
    ThreadType,
    TyEv,
    bus,
    eventbus,
)


class TestAsyncErrorWrapper(unittest.TestCase, BusNetwork):
    def setUp(self):
        # Инициализируем шину перед каждым тестом
        self.bus = super().bus
        self.bus.start()

    def tearDown(self):
        # Обязательно гасим шину и очищаем потоки после теста
        self.bus.stop()

    def test_async_handler_error_captured_by_bus(self):
        """Тест проверяет, что ошибка внутри асинхронного хэндлера успешно отлавливается оберткой и превращается в BUS_ERROR."""
        # Ссылки для сбора результатов внутри колбэков
        error_received_event = threading.Event()
        captured_error_kwargs = {}

        # 1. Создаем асинхронный хэндлер, который гарантированно падает
        async def failing_async_func():
            await asyncio.sleep(0.01)  # Имитируем асинхронную работу
            raise ValueError("Упс! Что-то пошло не так в async коде!")

        # Обернем его в твой класс Handler (если это необходимо по архитектуре)
        async_handler = Handler(
            func=failing_async_func,
            context=ThreadType.POOL,
        )

        # 2. Создаем синхронный хэндлер-перехватчик для события BUS_ERROR
        def bus_error_sub(*args, **kwargs):
            nonlocal captured_error_kwargs
            captured_error_kwargs = kwargs
            error_received_event.set()  # Сигнализируем тесту: ошибка поймана шиной!

        # Подписываемся на системную ошибку шины
        # (Используем имя или ID в зависимости от того, как настроен TyEv.BUS_ERROR)
        self.bus.subscribe(
            event_data=TyEv.BUS_ERROR,  # или TyEv.BUS_ERROR.name / ID
            handlers=bus_error_sub,
        )

        # Подписываем падающий асинхронный хэндлер на тестовое событие
        self.bus.subscribe(
            event_data="trigger_async_fail",
            handlers=async_handler,
        )

        # 3. Публикуем триггер-событие
        trigger_event = Event(name="trigger_async_fail")
        self.bus.publish(trigger_event)

        # 4. Ожидаем реакцию шины (таймаут 2 секунды, чтобы тест не завис в случае бага)
        is_success = error_received_event.wait(timeout=2.0)

        # Проверяем утверждения (Asserts)
        self.assertTrue(
            is_success, "Шина не сгенерировала BUS_ERROR в течение таймаута!"
        )

        # Проверяем, что в тексте ошибки есть упоминание нашего ValueError и имени хэндлера
        error_text = captured_error_kwargs.get("txt", "")
        self.assertIn("ValueError", error_text)
        self.assertIn("Упс! Что-то пошло не так в async коде!", error_text)
        self.assertIn("failing_async_func", error_text)


if __name__ == "__main__":
    unittest.main()
