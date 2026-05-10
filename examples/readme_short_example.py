import time

from SmartEventBus import Event, Handler, bus

bus.start()


def hello(greet, target):
    print(f"{greet}, {target}!")


hello_handler = Handler(func=hello, default_kwargs={"greet": "Hello"})
hello_event = Event(name="greetings", kwargs={"target": "World"})

bus.subscribe(hello_event, hello_handler)
bus.publish(hello_event)
# Hello, World!

time.sleep(1)
bus.stop()
