import time
import unittest
from concurrent.futures import Future

from smarteventbus import CallTimeoutError, Event, EventBus, Handler


class TestEventBusCallManager(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        pass

    def test_call_manager_success(self):
        """1. Проверяем успешный сбор результатов от нескольких подписчиков"""
        call_future = Future()
        f1, f2 = Future(), Future()

        # Симулируем, что подписчики успешно отработали
        f1.set_result("Result 1")
        f2.set_result("Result 2")

        # Вызываем менеджер (в реальном тесте через self.bus._call_manager)
        self.bus._threadorch._call_manager(call_future, [f1, f2], call_timeout=2.0)

        # Проверяем, что паблишер получил правильный кортеж
        self.assertTrue(call_future.done())
        self.assertEqual(call_future.result(), ("Result 1", "Result 2"))

    def test_call_manager_timeout(self):
        """2. Проверяем, что внутренний таймер корректно генерирует CallTimeoutError"""
        call_future = Future()
        f1 = Future()  # Этот фьючер никогда не завершится

        start = time.perf_counter()
        # Задаем очень маленький таймаут для быстроты тестов
        self.bus._threadorch._call_manager(call_future, [f1], call_timeout=0.1)
        end = time.perf_counter()

        self.assertTrue(call_future.done())
        # Проверяем, что выброшено именно наше кастомное исключение
        with self.assertRaises(CallTimeoutError):
            call_future.result()

        # Проверяем, что мы не висели дольше положенного (с учетом зазора +1 сек)
        self.assertLess(end - start, 1.5)

    def test_call_manager_future_timeout_fallback(self):
        """3. Проверяем перехват FutureTimeoutError, если он вылетит из самого фьючера"""
        call_future = Future()

        # Ломаем метод result фьючера, чтобы он имитировал внезапный FutureTimeoutError
        bad_future = Future()
        from concurrent.futures import TimeoutError as FutureTimeoutError

        bad_future.result = lambda timeout: raise_helper(FutureTimeoutError())

        self.bus._threadorch._call_manager(call_future, [bad_future], call_timeout=2.0)

        self.assertTrue(call_future.done())
        with self.assertRaises(CallTimeoutError):
            call_future.result()

    def test_call_method_happy_result(self):
        def responser_str():
            return "This line will be in the response!"

        def responser_error():
            raise RuntimeError("This exception will be in the response!")

        def responser_int():
            return 10000

        bus = EventBus()
        bus.start()

        bus.subscribe("calling", [responser_str, responser_error, responser_int])

        response = bus.call(Event(name="calling"))

        print(response)

        bus.stop()

        self.assertEqual(len(response), 3)

        # 2. Распаковываем кортеж
        msg, error_obj, number = response

        # 3. Проверяем каждый элемент по отдельности
        self.assertEqual(msg, "This line will be in the response!")
        self.assertEqual(number, 10000)

        # Проверка исключения: проверяем класс и его сообщение
        self.assertIsInstance(error_obj, RuntimeError)
        self.assertEqual(str(error_obj), "This exception will be in the response!")

    def test_call_method_wrong_event_response(self):
        def responser_str():
            return "This line will not be in the response!"

        bus = EventBus()
        bus.start()

        bus.subscribe("calling", responser_str)

        # При неправильном событии возвращается пустой кортеж
        response = bus.call(Event(name="wrong_calling"))
        self.assertFalse(response, msg="Кортеж должен быть пустым!")

        bus.stop()

    def test_call_method_timeout_error(self):
        def responser_str():
            return "This line will not be in the response because the timeout!"

        bus = EventBus()
        bus.start()

        bus.subscribe("calling", responser_str)

        with self.assertRaises(CallTimeoutError):
            response = bus.call(Event(name="calling", timeout=0))

        bus.stop()


def raise_helper(exc):
    raise exc


if __name__ == "__main__":
    unittest.main()
