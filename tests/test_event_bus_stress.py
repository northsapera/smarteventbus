import asyncio
import json
import random
import threading
import time
import unittest

from smarteventbus import (
    BusNetwork,
    Event,
    EventBus,
    Handler,
    SubscribeType,
    ThreadType,
    TyEv,
    UniqType,
    debug_mode,
)

# debug_mode.set()


def test_stress_run():
    bus = EventBus(maxsize=6000, paused=False)
    bus.start()

    # Счётчики для проверки
    processed_sync = 0
    processed_async = 0
    errors_captured = 0

    # Блокировки для потокобезопасного инкремента счётчиков в тестах
    lock = threading.Lock()

    # 1. Синхронный хэндлер (имитирует работу с диском/БД)
    def sync_worker(task_id):
        nonlocal processed_sync
        time.sleep(random.uniform(0.001, 0.005))  # Микро-задержка
        with lock:
            processed_sync += 1

    # 2. Асинхронный хэндлер (имитирует сетевые запросы)
    async def async_worker(task_id):
        nonlocal processed_async
        await asyncio.sleep(random.uniform(0.001, 0.005))
        # Рандомно симулируем падение каждого 10-го асинхронного хэндлера
        if task_id % 10 == 0:
            raise RuntimeError(f"Рандомный баг в асинхронном таске {task_id}")
        with lock:
            processed_async += 1

    # 3. Перехватчик системных ошибок шины
    def error_catcher(*args, **kwargs):
        nonlocal errors_captured
        with lock:
            errors_captured += 1

    # Регистрируем подписчиков
    bus.subscribe(TyEv.BUS_ERROR, error_catcher)

    # Обернем в Handler
    bus.subscribe("heavy_load", Handler(func=sync_worker, strict_order=False))
    bus.subscribe(
        "heavy_load",
        Handler(
            func=async_worker,
            context=ThreadType.POOL,
            strict_order=False,
        ),
    )

    print("🚀 Начинаем стресс-тест. Генерируем 200 конвергентных событий...")

    # Массово шлем события из разных потоков (имитируем работу реального GUI или сети)
    def producer(start_idx, count):
        for i in range(start_idx, start_idx + count):
            # Передаем task_id через args
            ev = Event(name="heavy_load", args=(i,), uniq_type=UniqType.NONE)
            bus.publish(ev)

    threads = []
    # Создаем n параллельных потоков-продюсеров, которые одновременно забьют шину
    n = 100
    for num in range(n):
        t = threading.Thread(target=producer, args=(num * 50, 50))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("📥 Все события отправлены в очередь. Ждем обработки...")

    # Даем шине время разгрести завалы
    time.sleep(8)

    # Проверяем отчет самого оркестратора
    report = bus.report()

    print(json.dumps(report, indent=4))

    print("\n--- Результаты стресс-теста ---")
    print(f"Всего должно быть обработано задач: {100 * n}")
    print(f"Успешно обработано синхронных: {processed_sync} / {round(50 * n)}")

    # Из 200 задач с ID от 0 до 199 ровно 20 делятся на 10 без остатка (0, 10, 20... 190)
    # Значит 20 должны упасть, а 180 — успешно выполниться.
    print(f"Успешно обработано асинхронных: {processed_async} / {round(50 * n * 0.9)}")
    print(
        f"Ошибок поймано шиной (BUS_ERROR): {errors_captured} / {round(50 * n * 0.1)}"
    )

    print(
        f"Остаток задач в очереди шины: {report['queue_info'].get('qsize', 'unknown')}"
    )

    bus.stop()
    print("🛑 Шина успешно остановлена.")

    def test_processed_sync_comparison():
        assert processed_sync == round(50 * n)

    def test_processed_async_comparison():
        assert processed_async == round(50 * n * 0.9)

    def test_errors_captured_comparison():
        assert errors_captured == round(50 * n * 0.1)

    test_processed_sync_comparison()
    test_processed_async_comparison()
    test_errors_captured_comparison()


if __name__ == "__main__":
    test_stress_run()
