import time
import unittest
import warnings

from smarteventbus import (
    Event,
    EventBus,
    Handler,
    HandlerWarning,
    PotentialLoopWarning,
    TyEv,
)


class TestHappyPubback(unittest.TestCase):
    def setUp(self):
        self.sync_pubback_complete = False
        self.async_pubback_complete = False

    def happy_sub_sync(self) -> Event:
        event_a = Event(name="A")
        return event_a

    def happy_sub_a(self):
        self.sync_pubback_complete = True

    async def happy_sub_async(self) -> Event:
        event_b = Event(name="B")
        return event_b

    async def happy_sub_b(self):
        self.async_pubback_complete = True

    def test_happy_pubback(self):
        bus = EventBus()

        bus.start()

        init_event = Event(name="init")

        happy_sync_handler_init = Handler(func=self.happy_sub_sync)
        happy_async_handler_init = Handler(func=self.happy_sub_async)
        happy_sync_handler_a = Handler(func=self.happy_sub_a)
        happy_async_handler_b = Handler(func=self.happy_sub_b)

        bus.subscribe(init_event, [happy_sync_handler_init, happy_async_handler_init])
        bus.subscribe("A", happy_sync_handler_a)
        bus.subscribe("B", happy_async_handler_b)

        bus.publish(init_event)

        time.sleep(0.1)

        bus.stop()

        self.assertTrue(self.sync_pubback_complete)
        self.assertTrue(self.async_pubback_complete)


class TestErrorByHistory(unittest.TestCase):
    def setUp(self) -> None:
        self.captured_ttl_errors = 0
        self.captured_error_txt = ""

    def error_sync_sub_1(self) -> Event:
        event_s2 = Event(name="s2")
        return event_s2

    def error_sync_sub_2(self):
        event_s1 = Event(name="s1")
        return event_s1

    async def error_async_sub_1(self) -> Event:
        event_a2 = Event(name="a2")
        return event_a2

    async def error_async_sub_2(self):
        event_a1 = Event(name="a1")
        return event_a1

    def capture_ttl_error(self, txt):
        self.captured_ttl_errors += 1
        self.captured_error_txt = txt

    def test_error_by_ttl_pubback(self):
        bus = EventBus()

        bus.start()

        init_event = Event(name="init")

        error_sync_handler_1 = Handler(func=self.error_sync_sub_1)
        error_sync_handler_2 = Handler(func=self.error_sync_sub_2)
        error_async_handler_1 = Handler(func=self.error_async_sub_1)
        error_async_handler_2 = Handler(func=self.error_async_sub_2)
        capture_ttl_error_handler = Handler(func=self.capture_ttl_error)

        bus.subscribe(init_event, [error_sync_handler_1, error_async_handler_1])
        bus.subscribe("s1", error_sync_handler_1)
        bus.subscribe("s2", error_sync_handler_2)
        bus.subscribe("a1", error_async_handler_1)
        bus.subscribe("a2", error_async_handler_2)
        bus.subscribe(TyEv.BUS_ERROR, capture_ttl_error_handler)

        with self.assertWarns(PotentialLoopWarning):
            bus.publish(init_event)

            time.sleep(0.5)

        bus.stop()

        self.assertEqual(self.captured_ttl_errors, 2)
        self.assertIn("PotentialLoop", self.captured_error_txt)
        self.assertIn("PotentialLoop", self.captured_error_txt)


class TestErrorByHistoryFromError(unittest.TestCase):
    def setUp(self) -> None:
        self.captured_ttl_errors = 0
        self.captured_error_txt = ""

    def error_sync_sub_capture_error(self) -> Event:
        event_s2 = Event(name="to_error")
        return event_s2

    def error_sync_sub_to_error(self):
        raise RuntimeError()

    def test_error_by_ttl_pubback(self):
        bus = EventBus()

        bus.start()

        init_event = Event(name="init")

        error_sync_handler_capture_error = Handler(
            func=self.error_sync_sub_capture_error
        )
        error_sync_handler_to_error = Handler(func=self.error_sync_sub_to_error)

        bus.subscribe(init_event, error_sync_handler_to_error)
        bus.subscribe("to_error", error_sync_handler_to_error)
        bus.subscribe(TyEv.BUS_ERROR, error_sync_handler_capture_error)

        with self.assertWarns(PotentialLoopWarning):
            bus.publish(init_event)

            time.sleep(0.5)

        bus.stop()


if __name__ == "__main__":
    unittest.main()
