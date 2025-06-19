import json

from flink_agents.api.event import InputEvent, OutputEvent

# python/flink_agents/api/test_action.py

def user_defined_action(input: InputEvent) -> OutputEvent:
    return OutputEvent(output=input.input + 1)

def framework_wrapped_action(input: str) -> str:
    print("input: " + str(input))
    input_event = InputEvent(input=int(input))
    output_event = user_defined_action(input_event)
    event_type = f"{output_event.__class__.__module__}.{output_event.__class__.__name__}"
    returned = json.dumps({"eventType": event_type, "payload": output_event.model_dump_json()})
    print("output: " + str(returned))
    return returned
