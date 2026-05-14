#    Copyright 2026 Matvey Aleksandrovich Grigoryev

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Event queue based on queue.PriorityQueue with complex event interaction logic."""

import queue
import threading
import warnings
from dataclasses import dataclass, field

from .config import STACKLEVEL, debug_mode
from .easycounter import EasyCounter
from .eventclasses import Event, TyEv
from .exceptions import (
    UnknownExitType,
    UnknownSearchType,
    UnknownUniqType,
    WaitTimeoutError,
)
from .logictypes import ExitType, SearchType, UniqType
from .warnings import (
    NonValidEventWarning,
    PuttingFailedWarning,
    WaitTimeoutWarning,
)


# Ядро (очередь)
class UniquePriorityQueue:
    """Специальная очередь событий, приоритеризирована и имеет проверку на уникальность событий"""

    @dataclass
    class Inspection:
        """Самоинспекция"""

        wait_warnings_amount: EasyCounter = field(default_factory=EasyCounter)
        wait_errors_amount: EasyCounter = field(default_factory=EasyCounter)
        nonvalid_events_gotten: EasyCounter = field(default_factory=EasyCounter)
        putting_failed: EasyCounter = field(default_factory=EasyCounter)
        events_cleaned: EasyCounter = field(default_factory=EasyCounter)

        def snapshot(self) -> dict:
            """Превращает в словарь."""
            return {
                k: int(v) if isinstance(v, EasyCounter) else v
                for k, v in self.__dict__.items()
            }

    def __init__(self, maxsize=0) -> None:
        self.maxsize = maxsize

        self.queue = queue.PriorityQueue(maxsize=self.maxsize)
        self._ids: dict[int, dict[int, Event]] = {}
        """Сопровождающий словарь, сгруппирован по id, формат: [event.id: {event.num: event}]"""

        self._condition = threading.Condition()
        """Замок состояния"""

        self._inspection = self.Inspection()

    # Взаимодействия с очередью
    def put(self, event: Event) -> None:
        with self._condition:
            if event.uniq_type == UniqType.NONE:
                self._bare_put(event)

            elif event.uniq_type == UniqType.WAIT:
                self._waiting_put(event)

            elif event.uniq_type == UniqType.REPLACE:
                self._replacing_put(event)

            else:
                raise UnknownUniqType("Unknown unique logic type received!")

    def _bare_put(self, event: Event) -> None:
        self._add_to_queue(event)

    def _waiting_put(self, event: Event) -> None:
        with self._condition:
            success = self._condition.wait_for(
                lambda: len(self._ids.get(event.id, {})) < event.uniq_counter,
                timeout=event.wait_timeout,
            )

            if not success:
                if event.wait_timeout_exit == ExitType.REJECT:
                    self._inspection.wait_errors_amount()
                    raise WaitTimeoutError(
                        f"The WAIT logic event (name='{event.name}', id={event.id}, num={event.num}) was rejected by timeout!"
                    )
                elif event.wait_timeout_exit == ExitType.PUT:
                    self._inspection.wait_warnings_amount()
                    warnings.warn(
                        f"Event(name='{event.name}', id={event.id}, num={event.num}) WAIT timeout exceeded. Forcing PUT into queue. Total WAIT timeout warnings amount: {int(self._inspection.wait_warnings_amount)}",
                        WaitTimeoutWarning,
                        stacklevel=STACKLEVEL,
                    )
                else:
                    raise UnknownExitType("Unknown exit logic type received!")

        self._add_to_queue(event)

    def _replacing_put(self, event: Event) -> None:
        with self._condition:
            _events_list = sorted(
                (e for e in self._ids.get(event.id, {}).values() if e.is_valid)
            )

            if _events_list:
                idx = min(event.uniq_counter - 1, len(_events_list) - 1)

                _old_event = _events_list[idx]

                _old_event.make_nonvalid()

                event.priority_counter = _old_event.priority_counter

        self._add_to_queue(event)

    def _add_to_queue(self, event: Event) -> None:
        with self._condition:
            id_group = self._ids.setdefault(event.id, {})
            id_group[event.num] = event

        try:
            self.queue.put(event, block=event.put_block, timeout=event.put_timeout)
        except (queue.Full, Exception) as e:
            with self._condition:
                self._remove_from_sattelite(event)
                self._condition.notify_all()

                self._inspection.putting_failed()
                warnings.warn(
                    f"Event(name='{event.name}', id={event.id}, num={event.num}) did not added to the queue. Total failed puttings amount: {int(self._inspection.putting_failed)}. Error: {e}",
                    PuttingFailedWarning,
                    stacklevel=STACKLEVEL,
                )

                raise

        with self._condition:
            self._condition.notify_all()

    def get(self, block: bool = True, timeout: float | None = None) -> Event:
        while True:
            event: Event = self.queue.get(block=block, timeout=timeout)

            with self._condition:
                self._remove_from_sattelite(event)
                self._condition.notify_all()

                if not event.is_valid:
                    self._inspection.nonvalid_events_gotten()
                    if debug_mode.is_set():
                        warnings.warn(
                            f"Event(name='{event.name}', id={event.id}, num={event.num}) was gotten as nonvalid. Total nonvalid events amount: {int(self._inspection.nonvalid_events_gotten)}",
                            NonValidEventWarning,
                            stacklevel=STACKLEVEL,
                        )

                    continue

                return event

    def _remove_from_sattelite(self, event: Event) -> None:
        id_group = self._ids.get(event.id)
        if id_group:
            id_group.pop(event.num, None)
            if not id_group:
                self._ids.pop(event.id)

    # Работа с обособленными событиями
    def search_events(
        self,
        search_type: SearchType,
        event_name: str = "",
        event_meta: dict | None = None,
        event_type: TyEv | type[Event] = Event,  # FIXME: Перевести на любой Enum
        event_num: int = -1,
    ) -> list[Event]:
        """Ищет событие (события) в очереди. Возвращает список найденных событий в порядке приоритета или пустой список, если событий не найдено.

        Args:
            search_type (SearchType): Вид поиска.
            event_name (str, optional): Имя события. Defaults to "".
            event_meta (dict, optional): Метаданные события (полное совпадение). Defaults to None.
            event_type (TyEv | type[Event], optional): Класс типа типового или нетипового события. Defaults to Event.
            event_num (int, optional): Порядковый номер события. Defaults to -1.

        Raises:
            UnknownSearchType: Если передан неизвестный вид поиска.
            UnknownEventDataType: Если передан неизвестный класс вместо события.
            EventError: Если у именнованного события не определено имя.

        Returns:
            list[Event]: Список объектов событий, отсортированных по приоритету.

        Notes:
            - Поиск по `NAME` требует *обязательной* передачи **имени**.

            -> Возвращает *все* события *любых типов* с переданным **именем**.

            - Поиск по `ID` требует передачи **типа** (default=`Event`) и **метаданных** (default=`None` -> `{}`) события. Если для типа события установлено значение имени *(в случае типовых событий или заранее созданных именованных типов)*, не требует, *но допускает*, передачу **имени**, в ином случае требует *обязательной* передачи.

            -> Возвращает события, *полностью совпадающие* по **имени**, **метаданным** и **типу**.

            - Поиск по `NUMBER` требует *обязательной* передачи порядкового **номера**.

            -> В стандартной ситуации возвращает список из *одного* события.

        Examples:
            Подготовка: Создаем очередь и добавляем данные
            >>> class SLEEP(Event):
            ...     name: str = "SLEEP"

            >>> _queue = UniquePriorityQueue()
            >>> e = Event(name="click", priority=5)
            >>> _queue.put(Event(name="click", priority=10))
            >>> _queue.put(e)
            >>> _queue.put(SLEEP())
            >>> _queue.put(TyEv.CANCEL(meta={"window": "main"}, kwargs={'button': 1}))
            >>> _queue.put(TyEv.CANCEL(name="CANCEL", meta={"window": "main"}, kwargs={'button': 2}))

            Для поиска по имени:
            >>> _queue.search_events(SearchType.NAME, event_name="click") # doctest: +ELLIPSIS
            [Event(name='click', ...), Event(name='click', ...)]

            Для поиска по id (тип, имя, метаданные):
            >>> _queue.search_events(SearchType.ID, event_type=SLEEP, event_name="SLEEP") # doctest: +ELLIPSIS
            [SLEEP(name='SLEEP', ...)]

            >>> _queue.search_events(SearchType.ID, event_type=TyEv.CANCEL, event_name="CANCEL", event_meta={"window": "main"}) # doctest: +ELLIPSIS
            [...]
            >>> _queue.search_events(SearchType.ID, event_type=TyEv.CANCEL, event_meta={"window": "main"}) # doctest: +ELLIPSIS
            [CANCEL_EVENT(name='CANCEL', meta={'window': 'main'}, ...kwargs={'button': 1...}, ...), CANCEL_EVENT(name='CANCEL', meta={'window': 'main'}, ...kwargs={'button': 2...}, ...)]

            Для поиска по номеру:
            >>> found = _queue.search_events(SearchType.NUMBER, event_num=e.num)

            Проверка, что найдено 1 событие
            >>> len(found)
            1

            Проверка совпадения номеров
            >>> found[0]._event_number == e.num
            True
        """
        searched_events: list[Event] = []

        with self._condition:
            if search_type == SearchType.NAME:
                for id_group in self._ids.values():
                    for event in id_group.values():
                        if event.name == event_name:
                            searched_events.append(event)

            elif search_type == SearchType.ID:
                default_data = Event.get_default_data(event_type)
                e_type, default_name, default_meta = default_data

                name = event_name if event_name else default_name
                meta = event_meta if event_meta is not None else default_meta

                event_id = Event.get_id(e_type, name, meta)

                for event in self._ids.get(event_id, {}).values():
                    searched_events.append(event)

            elif search_type == SearchType.NUMBER:
                for id_group in self._ids.values():
                    if event_num in id_group:
                        searched_events.append(id_group[event_num])
                        break

            else:
                raise UnknownSearchType("Unknown search logic type received!")

            if not searched_events:
                return []

            return sorted(searched_events)

    def replace_event(self, new_event: Event, old_event: Event) -> None:
        """Заменяет одно событие в очереди другим, ставя новое событие хронологически на то же место. Все поля, включая название сигнала, id, приоритет и метаданные, обновляются.

        Args:
            new_event (Event): Замещяющее событие.
            old_event (Event): Замещаемое событие.

        Notes:
            Событие кладется в очередь через метод .put(), что позволяет использовать для него любую логику уникальности.
        """
        with self._condition:
            old_event.make_nonvalid()

            new_event.priority_counter = old_event.priority_counter

        self.put(new_event)

    def devalid_event(self, event: Event) -> None:
        """Девалидизация заданного события, используется только для ручной девалидизации.

        Args:
            event (Event): Событие.
        """
        with self._condition:
            event.make_nonvalid()

    def valid_event(self, event: Event) -> None:
        """Ревалидизация заданного события, используется только для ручной ревалидизации.

        Args:
            event (Event): Событие.
        """
        with self._condition:
            event.make_valid()

    def clean_queue(self) -> None:
        """Очистка очереди."""
        with self._condition:
            while True:
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()

                    self._inspection.events_cleaned()

                except queue.Empty:
                    break

            self._ids.clear()

            self._condition.notify_all()

    # Счетчики
    def task_done(self) -> None:
        self.queue.task_done()

    def join(self) -> None:
        self.queue.join()

    # Самоинспекция
    def qsize(self) -> int:
        with self._condition:
            return self.queue.qsize()

    def info(self) -> dict:
        """Информация о текущем состоянии очереди. Возвращает отчет, содержащий информацию о размере очереди, количестве id групп, {количестве полученных ошибок и предупреждений}, содержании словаря-спутика {id1: {количество событий, тип, имя, метаданные, [порядковые номера]}, id2:...}.

        Returns:
            dict: Отчет о текущем состоянии.

        Examples:
            Подготовка: Создаем очередь и добавляем данные
            >>> _queue = UniquePriorityQueue(10)
            >>> _queue.put(Event(name="click", priority=10))
            >>> _queue.put(TyEv.CANCEL(meta={"window": "main"}, kwargs={'button': 1}))
            >>> e = TyEv.CANCEL(name="CANCEL", meta={"window": "main"}, kwargs={'button': 2})
            >>> _queue.put(e)
            >>> e.make_nonvalid()

            Получение отчета:
            >>> _queue.info() # doctest: +ELLIPSIS
            {'qsize': 3, 'maxsize': 10, 'ids_amount': 2, 'inspection': {'wait_warnings_amount': ..., 'wait_errors_amount': ..., 'nonvalid_events_gotten': ..., 'putting_failed': ...}, 'satellite': {339514816366116273679071597463465878107: {'events_amount': 1, 'type': 'Event', 'name': 'click', 'meta': {}, 'nums': [(..., True)]}, 315106522019672546056486932092520271199: {'events_amount': 2, 'type': 'CANCEL_EVENT', 'name': 'CANCEL', 'meta': {'window': 'main'}, 'nums': [(..., True), (..., False)]}}}
        """
        with self._condition:
            report: dict = {
                "qsize": self.qsize(),
                "maxsize": self.maxsize,
                "ids_amount": len(self._ids),
                "inspection": self._inspection.snapshot(),
            }

            report["satellite"] = {}

            for event_id, id_group in self._ids.items():
                nums_amount = len(id_group)

                data_type = ""
                data_name = ""
                data_meta = {}
                nums = []

                if nums_amount:
                    first_event = next(iter(id_group.values()))

                    if first_event:
                        data_type = first_event.type
                        data_name = first_event.name
                        data_meta = first_event.meta

                        nums = [(k, e.is_valid) for k, e in id_group.items()]

                report["satellite"][event_id] = {
                    "events_amount": nums_amount,
                    "type": data_type,
                    "name": data_name,
                    "meta": data_meta.copy(),
                    "nums": nums,
                }

            return report
