import asyncio
import sys

import pytest

from smarteventbus import (
    CallTimeoutError,
    Event,
    EventBus,
    QueueFull,
    UniqType,
    WaitTimeoutError,
    register,
    subscribe_to,
)

# Реальные объекты событий
event_publish = Event(name="test_publish", kwargs={"txt": "Pub Успешно!"})
event_call = Event(name="test_call", kwargs={"txt": "Call Успешно!"})
event_async_pub = Event(name="test_async_pub", kwargs={"txt": "Async Pub Успешно!"})
event_async_call = Event(name="test_async_call", kwargs={"txt": "Async Call Успешно!"})


class TestEventBusFullCycle:
    @pytest.fixture(scope="class", autouse=True)
    def event_loop(self):
        """Создает единый loop для всего класса (совместимо со всеми версиями)."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        yield loop
        loop.close()

    @pytest.fixture(scope="class", autouse=True)
    def bus_lifecycle(self, event_loop):
        """Фикстура запускает шину один раз для всех тестов класса."""
        bus = EventBus()

        @subscribe_to(bus, event_publish)
        @register()
        def on_publish(txt: str):
            print(txt)

        @subscribe_to(bus, event_call)
        @register()
        def on_call(txt: str) -> str:
            return f"Result: {txt}"

        @subscribe_to(bus, event_async_pub)
        @register()
        def on_async_publish(txt: str):
            print(txt)

        @subscribe_to(bus, event_async_call)
        @register()
        def on_async_call(txt: str) -> str:
            return f"Async Result: {txt}"

        bus.start()
        yield bus
        bus.stop()

    @pytest.mark.asyncio
    async def test_1_synchronous_publish(self, bus_lifecycle, capsys):
        bus = bus_lifecycle
        bus.publish(event_publish)
        await asyncio.sleep(0.05)

        captured = capsys.readouterr()
        assert "Pub Успешно!" in captured.out

    @pytest.mark.asyncio
    async def test_2_synchronous_call(self, bus_lifecycle):
        bus = bus_lifecycle
        result = bus.call(event_call)
        assert result == ("Result: Call Успешно!",)

    @pytest.mark.asyncio
    async def test_3_asynchronous_publish(self, bus_lifecycle, capsys):
        bus = bus_lifecycle
        await bus.async_publish(event_async_pub)
        await asyncio.sleep(0.05)

        captured = capsys.readouterr()
        assert "Async Pub Успешно!" in captured.out

    @pytest.mark.asyncio
    async def test_4_asynchronous_call(self, bus_lifecycle):
        bus = bus_lifecycle
        result = await bus.async_call(event_async_call)
        assert result == ("Async Result: Async Call Успешно!",)

    # =========================================================================
    # ЧАСТЬ 2: ТЕСТИРОВАНИЕ ОШИБОК И ТАЙМАУТОВ (ЧЕРЕЗ ПАУЗУ ШИНЫ)
    # =========================================================================

    @pytest.mark.asyncio
    async def test_5_wait_logic_timeout_error(self):
        """Проверка генерации WaitTimeoutError при uniq_type=UniqType.WAIT.

        Используем паузу шины, чтобы гарантированно заблокировать обработку.
        """
        # Создаем изолированную шину в режиме паузы
        bus = EventBus(paused=True)
        bus.start()

        # Первое событие ложится в очередь, но не обрабатывается из-за паузы
        base_event = Event(name="freeze_event")
        bus.publish(base_event)

        # Второе событие с логикой WAIT и коротким таймаутом
        wait_event = Event(name="freeze_event", uniq_type=UniqType.WAIT, timeout=0.05)

        # Так как шина на паузе, время ожидания обработки (0.05с) мгновенно истечет
        with pytest.raises(WaitTimeoutError):
            bus.publish(wait_event)

        # Снимаем с паузы, чтобы правильно закончить через stop()
        bus.resume()
        bus.stop()

    @pytest.mark.asyncio
    async def test_6_call_timeout_error_synchronous(self):
        """Проверка генерации CallTimeoutError в синхронном методе call().

        Шина на паузе -> ответ гарантированно не запишется в Future вовремя.
        """
        bus = EventBus(paused=True)
        bus.start()

        # Регистрируем любой хэндлер (он даже не успеет вызваться из-за паузы)
        @subscribe_to(bus, Event(name="call_timeout_event"))
        @register()
        def some_handler():
            return "Fast OK"

        timeout_event = Event(name="call_timeout_event", timeout=0.05)

        # Вызов должен упасть по таймауту, так как шина заморожена
        with pytest.raises(CallTimeoutError):
            bus.call(timeout_event)

        bus.resume()
        bus.stop()

    @pytest.mark.asyncio
    async def test_7_call_timeout_error_asynchronous(self):
        """Проверка генерации CallTimeoutError в асинхронном методе async_call()."""
        bus = EventBus(paused=True)
        bus.start()

        timeout_event = Event(name="call_timeout_event", timeout=0.05)

        # Асинхронный вызов точно так же обязан отдать CallTimeoutError
        with pytest.raises(CallTimeoutError):
            await bus.async_call(timeout_event)

        bus.resume()
        bus.stop()

    @pytest.mark.asyncio
    async def test_8_queue_full_error_via_saturation(self):
        """Проверка генерации QueueFull при реальном физическом заполнении очереди.

        Замораживаем шину и забиваем её до лимита, проверяя block=False.
        """

        max_size = 5

        bus = EventBus(paused=True, maxsize=max_size)

        bus.start()

        # Забиваем очередь до предела
        for i in range(max_size):
            try:
                bus.publish(Event(name="fill_event", block=False))
            except QueueFull:
                break  # Если лимит заполнился раньше

        # Следующий пуш с block=False гарантированно обязан выкинуть QueueFull
        overflow_event = Event(name="fill_event", block=False)

        with pytest.raises(QueueFull):
            bus.publish(overflow_event)

        bus.resume()
        bus.stop()


if __name__ == "__main__":
    # Позволяет запускать файл напрямую через python.exe
    sys.exit(pytest.main([__file__, "-v", "-s"]))
