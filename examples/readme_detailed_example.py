import threading

from smarteventbus import BusNetwork, Event, Handler, TyEv, UniqType, bus, debug_mode
from smarteventbus import SubscribeType as SubType

# Enable debug mode
debug_mode.set()

# Start the event bus
bus.start()


# Create classes connected to the bus (needed if publishing events from within the class)
class Logic(BusNetwork):
    def __init__(self):
        self.bus = super().bus

    def calc(self, num1: int, num2: int, end_event: Event):
        result = num1 + num2

        # Create an event (event name, payload data, priority, uniqueness logic)
        got_result = Event(
            name="calc complete",
            kwargs={"result": result},
            priority=10,
            uniq_type=UniqType.WAIT,
        )

        # Publish the result
        self.bus.publish(got_result)
        self.bus.publish(end_event)

    def calc_to_thread(self, num1: int, num2: int, end_event: Event):
        """Run calculations in a separate thread"""
        self.calc_thread = threading.Thread(
            target=self.calc,
            kwargs={"num1": num1, "num2": num2, "end_event": end_event},
            daemon=True,
        )

        self.calc_thread.start()


logic = Logic()

""" - Inside the main thread: - """


def start_print(**kwargs):
    print("Start!")


def print_result(result: int, **kwargs):
    print(result)


def print_txt(**kwargs):
    print(kwargs)
    print("No problems.")


def end_calc():
    print("Calculations complete!")


# Create handlers (subscribers)
calc_handler = Handler(func=logic.calc_to_thread, default_kwargs={"num1": 5})
end_handler = Handler(func=end_calc)

# Subscribe to events
# TyEv.START is a built-in event type with default values; its parameters can be customized just like a regular event
bus.subscribe(
    TyEv.START, [start_print, calc_handler]
)  # Providing multiple subscribers in a list guarantees their execution order

# If a subscriber is a standard function, it's recommended to include **kwargs in its arguments.
# The bus passes events "as-is" with all payload and internal parameters (when debug_mode is active).
bus.subscribe("calc complete", print_result)

# Subscription by ID does not accept strings
bus.subscribe(Event(name="calc complete"), print_txt, SubType.ID)

# Subscription by NUMBER accepts a specific event object or index/number
end_event = TyEv.END()
bus.subscribe(end_event, end_handler, SubType.NUMBER)

# Publish
bus.publish(TyEv.START(kwargs={"num2": 3, "end_event": end_event}))
# 1. The start_print subscriber is triggered.
# --- Output: Start!

# 2. The calc_handler subscriber is triggered. Thanks to default_kwargs in the Handler, it doesn't fail due to a missing num1 argument.
# 3. The got_result ("calc complete") event is published.
# Two subscribers are attached to this event: print_result by NAME and print_txt by ID.
# print_txt is triggered first because the resolution priority is NUMBER -> ID -> NAME (from most specific to least specific).

# 4. The print_txt subscriber is triggered.
# --- Output: {'result': 8, '_func_name': 'calc', '_signal_name': 'calc complete'}
# --- Output: No problems.

# 5. The print_result subscriber is triggered. Thanks to the Handler registration, it doesn't crash from the extra technical metadata arguments. Debug mode adds technical metadata.
# --- Output: 8

# 6. The end_event is published.

# 7. The end_handler subscriber is triggered.
# --- Output: Calculations complete!


# Stop the event bus
bus.stop()

# Stop the calculation thread
logic.calc_thread.join()
