import asyncio
import time

from smarteventbus import Event, EventBus, register, subscribe_to

# Initializing the frozen bus
bus = EventBus(paused=True)

# Define an event with dynamic context
example_event_publish = Event(
    name="Publish_event",
    kwargs={"txt": "This line will appear after the event is published!"},
)

example_event_call = Event(
    name="Call_event",
    kwargs={"txt": "This line will be returned after the event is called!"},
)


# Registering functions as Handlers and subscribing to events
@subscribe_to(bus, example_event_publish)
@register(default_kwargs={"txt": "This line will be in the output!"})
def func_print(txt: str) -> None:
    print(txt)


@subscribe_to(bus, example_event_call)
@register(default_kwargs={"txt": "This line will be in the output!"})
def func_return(txt: str) -> str:
    return f"Received: {txt}"


# 1. Demo asynchronous wait
async def independent_waititng(_time):
    start_time = time.perf_counter()

    await asyncio.sleep(_time)

    end_time = time.perf_counter()
    waiting_time = end_time - start_time

    print(f"Independent waiting is over ({waiting_time} sec)...")
    # Output: Independent waiting is over ({waiting_time}.0... sec)...


# 2. Event-driven execution: triggering the function via the event bus
async def pub_publish():
    await bus.async_publish(example_event_publish)
    # Output: This line will appear after the event is published!


# 3. Event-driven execution: calling the function via the event bus
async def pub_call():
    result = await bus.async_call(example_event_call)
    print(result)
    # Output: ('Received: This line will be returned after the event is called!',)


async def publication():
    """Asynchronous group function call with asynchronous publish, call and await."""
    await asyncio.gather(independent_waititng(1), pub_publish(), pub_call())


async def preparing_waiting(preparing_time):
    """Asynchronous wait before bus defrosting."""
    await asyncio.sleep(preparing_time)
    print("--- The preparatory wait is over, the bus is defrosting... ---")
    bus.resume()
    # Output: --- The preparatory wait is over, the bus is defrosting... ---


async def main(preparing_time):
    print("=== START ===")
    await asyncio.gather(publication(), preparing_waiting(preparing_time))
    print("=== END ===")


if __name__ == "__main__":
    bus.start()

    asyncio.run(main(2))
    # Output: === START ===
    # Output: Independent waiting is over...
    # Output: --- The preparatory wait is over...
    # Output: This line will appear after...
    # Output: ('Received: This line will be returned after...
    # Output: === END ===

    # Gracefully stops the bus, waiting for all task_done signals (timeout: 10s)
    bus.stop()
