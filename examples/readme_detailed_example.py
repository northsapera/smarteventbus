import threading
import time

from SmartEventBus import DEBUG_MODE, BusNetwork, Event, Handler, TyEv, UniqType, bus
from SmartEventBus import SubscribeType as SubType

# Установка флага отладки
DEBUG_MODE.set()

# Запуск шины
bus.start()


# Создание классов с подключением к шине (в случае необходимости публикации событий)
class Logic(BusNetwork):
    def __init__(self):
        self.bus = super().bus

    def calc(self, num1: int, num2: int, end_event: Event):
        result = num1 + num2

        # Создание события (имя события, передаваемые данные, приоритет, логика уникальности)
        got_result = Event(
            name="calc complete",
            kwargs={"result": result},
            priority=10,
            uniq_type=UniqType.WAIT,
        )

        # Публикация результата
        self.bus.publish(got_result)
        self.bus.publish(end_event)

    def calc_to_thread(self, num1: int, num2: int, end_event: Event):
        """Запуск расчетов в отдельном потоке"""
        self.calc_thread = threading.Thread(
            target=self.calc,
            kwargs={"num1": num1, "num2": num2, "end_event": end_event},
            daemon=True,
        )

        self.calc_thread.start()


logic = Logic()

""" - В основном потоке: - """


def start_print(**kwargs):
    print("Start!")


def print_result(result: int, **kwargs):
    print(result)


def print_txt(**kwargs):
    print(kwargs)
    print("No problems.")


def end_calc():
    print("Calculations complete!")


# Создание подписчиков
calc_handler = Handler(func=logic.calc_to_thread, default_kwargs={"num1": 5})
end_handler = Handler(func=end_calc)

# Подписка
# TyEv.START - типовое событие, имеет значения по умолчанию, допускает их изменение тем же образом, что и при создании нового события
bus.subscribe(
    TyEv.START, [start_print, calc_handler]
)  # Указание нескольких подписчиков списком гарантирует порядок их активации

# Если подписчик - обычная функция, рекомендуется в ее аргументах указывать **kwargs, так как шина пробрасывает события "как есть" со всеми переданными и внутренними (при DEBUG_MODE) аргументами
bus.subscribe("calc complete", print_result)

# Подписка по ID не принимает строку
bus.subscribe(Event(name="calc complete"), print_txt, SubType.ID)

# Подписка по NUMBER принимает конкретный объект или номер
end_event = TyEv.END()
bus.subscribe(end_event, end_handler, SubType.NUMBER)

# Публикация
bus.publish(TyEv.START(kwargs={"num2": 3, "end_event": end_event}))
# Активируется подписчик start_print
# --- Вывод: Start!

# Активируется подписчик calc_handler, благодаря default_kwargs в хэндлере не падает с ошибкой от отсутствия num1
# Публикация события got_result ("calc complete")
# К этому событию привязаны 2 подписчика: print_result по NAME и print_txt по ID
# Первым активируется print_txt, так как порядок NUMBER->ID->NAME (от более точного к менее)

# Активируется подписчик print_txt, благодаря регистрации в Handler не падает от лишнего аргумента result. Благодаря файлу отладки добавляется техническая информация
# --- Вывод: {'result': 8, '_func_name': 'calc', '_signal_name': 'calc complete'}
# --- Вывод: No problems.

# Активируется подписчик print_result
# --- Вывод: 8

# Публикация события end_event

# Активируется подписчик end_handler
# --- Вывод: Calculations complete!

# Небольшое ожидание, чтобы поток calc_thread успел создаться (в обычной ситуации не требуется)
time.sleep(1)

# Остановка потока расчетов
logic.calc_thread.join()

# Остановка шины
bus.stop()
