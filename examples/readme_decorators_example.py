from smarteventbus import Event, EventBus, register, subscribe_to

bus = EventBus()

bus.start()

# Define an event with dynamic context
example_event = Event(
    name="Init event",
    kwargs={"txt": "This line will appear after the event is published!"},
)


@subscribe_to(bus, example_event)
@register(default_kwargs={"txt": "This line will be in the output!"})
def func_print(txt: str, printing: bool = True) -> None:
    """This docstring is visible!"""
    if printing:
        print(txt)


# 1. Direct call: invalid argument won't crash the code since it is registered as a Handler!
func_print(
    invalid_argument="This line won't crash the code thanks to the Handler!",
)
# Output: This line will be in the output!

# 2. Event-driven execution: triggering the function via the event bus
bus.publish(example_event)
# Output: This line will appear after the event is published!

# Gracefully stops the bus, waiting for all task_done signals (timeout: 10s)
bus.stop()
