import json

from flink_agents.api.event import InputEvent, OutputEvent, Event, TempEvent
from flink_agents.api.workflow_runner_context import WorkflowRunnerContext

# python/flink_agents/api/test_action.py

def user_defined_action(input: InputEvent) -> OutputEvent:
    return OutputEvent(output=input.input + 1)

def user_defined_action1(input: InputEvent) -> TempEvent:
    return TempEvent(input=input.input + 1)

def user_defined_action2(input: TempEvent) -> OutputEvent:
    print("input: " + str(input.input))
    return OutputEvent(output=input.input + 1)

# def framework_wrapped_action(input: str) -> str:
#     print("input: " + str(input))
#     input_event = InputEvent(input=int(input))
#     output_event = user_defined_action(input_event)
#     event_type = f"{output_event.__class__.__module__}.{output_event.__class__.__name__}"
#     returned = json.dumps({"eventType": event_type, "payload": output_event.model_dump_json()})
#     print("output: " + str(returned))
#     return returned

def framework_wrapped_action(input: str, ctx: WorkflowRunnerContext):
    try:
        input_event = InputEvent(input=int(input))
        output_event = user_defined_action(input_event)
        ctx.send_event(output_event)
    except Exception as e:
        print(f"Error adding event: {e}")

def framework_wrapped_action1(input_event: InputEvent, ctx: WorkflowRunnerContext):
    try:
        output_event = user_defined_action1(input_event)
        ctx.send_event(output_event)
    except Exception as e:
        print(f"Error adding event: {e}")

def framework_wrapped_action2(input_event: Event, ctx: WorkflowRunnerContext):
    try:
        output_event = user_defined_action2(input_event)
        ctx.send_event(output_event)
    except Exception as e:
        print(f"Error adding event: {e}")