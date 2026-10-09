"""Small execution adapters; no framework dependencies loaded by importing this."""
import inspect
from .client import Agent


def json_value(value):
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "model_dump"):
        return json_value(value.model_dump(mode="json"))
    raise TypeError("result contains a non-JSON value; provide a result_mapper")


def result_dict(result):
    if isinstance(result, dict):
        return json_value(result)
    if getattr(result, "json_dict", None) is not None:
        return json_value(result.json_dict)
    if getattr(result, "pydantic", None) is not None:
        return json_value(result.pydantic.model_dump(mode="json"))
    if hasattr(result, "model_dump"):
        return json_value(result.model_dump(mode="json"))
    if hasattr(result, "raw"):
        return {"text": result.raw}
    if isinstance(result, str):
        return {"text": result}
    raise TypeError("provide a result_mapper that returns a JSON-serializable dictionary")


class CallableAdapter:
    def __init__(self, invoke, *, input_mapper=None, result_mapper=result_dict):
        self.invoke = invoke
        self.input_mapper = input_mapper or (lambda payload, message: payload)
        self.result_mapper = result_mapper

    async def __call__(self, payload, message):
        result = self.invoke(self.input_mapper(payload, message))
        if not inspect.isawaitable(result):
            raise TypeError("callable adapter requires an async entry point")
        return self.result_mapper(await result)

    async def serve(self, endpoint, *, concurrency=1):
        await Agent(endpoint).serve(self, concurrency=concurrency)


class LangGraphAdapter(CallableAdapter):
    """Pass per-session checkpoint/config mapping explicitly when needed."""
    def __init__(self, graph, *, input_mapper=None, result_mapper=result_dict, config_factory=None):
        super().__init__(graph.ainvoke, input_mapper=input_mapper, result_mapper=result_mapper)
        self.config_factory = config_factory

    async def __call__(self, payload, message):
        data = self.input_mapper(payload, message)
        kwargs = {"config": self.config_factory(payload, message)} if self.config_factory else {}
        result = await self.invoke(data, **kwargs)
        return self.result_mapper(result)


class CrewAIAdapter(CallableAdapter):
    """A factory can create a fresh crew per invocation for state isolation."""
    def __init__(self, crew_or_factory, *, input_mapper=None, result_mapper=result_dict):
        self.crew_or_factory = crew_or_factory
        super().__init__(None, input_mapper=input_mapper, result_mapper=result_mapper)

    async def __call__(self, payload, message):
        crew = self.crew_or_factory(payload, message) if callable(self.crew_or_factory) else self.crew_or_factory
        if not hasattr(crew, "akickoff"):
            raise TypeError("CrewAI adapter requires native async akickoff(); use CallableAdapter for an explicit fallback")
        result = await crew.akickoff(inputs=self.input_mapper(payload, message))
        return self.result_mapper(result)


class OpenAIAgentsAdapter(CallableAdapter):
    """Wrap Agents SDK Runner.run; isolate sessions with an options_factory.

    Agent runs interrupted for approval are returned as errors to this RPC layer;
    resolve/resume them in the framework before returning a final result.
    A custom async runner can be injected for applications and offline tests.
    """
    def __init__(self, agent, *, runner=None, input_mapper=None, result_mapper=result_dict, options_factory=None):
        if runner is None:
            from agents import Runner
            runner = Runner.run
        super().__init__(runner, input_mapper=input_mapper or (lambda payload, message: payload["input"]), result_mapper=result_mapper)
        self.agent, self.options_factory = agent, options_factory

    async def __call__(self, payload, message):
        options = self.options_factory(payload, message) if self.options_factory else {}
        result = await self.invoke(self.agent, self.input_mapper(payload, message), **options)
        if getattr(result, "interruptions", None):
            raise RuntimeError("agent run needs approval; resolve and resume it in the framework")
        return self.result_mapper(result.final_output)
