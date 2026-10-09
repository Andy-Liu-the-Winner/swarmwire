"""The same tools exposed to OpenAI, Anthropic and MCP."""
import asyncio
from .client import Agent, encode


def schema(properties, required=()):
    return {"type": "object", "properties": properties, "required": list(required), "additionalProperties": False}


TEXT = {"type": "string"}
SEND_FIELDS = {"to": TEXT, "payload": {"type": "object"}, "kind": {"type": "string", "enum": ["event", "task", "reply", "error"]}, "correlation_id": TEXT}
DEFINITIONS = [
    ("relay_send", "Send a compact message directly to a registered agent. For replies, use kind=reply and correlation_id=the received message id. Acknowledgement means queued, not processed.", schema(SEND_FIELDS, ["to", "payload"])),
    ("relay_send_batch", "Send independent messages concurrently. Each item returns its own queued/error status; a batch is not atomic.", schema({"messages": {"type": "array", "items": schema(SEND_FIELDS, ["to", "payload"]), "minItems": 1, "maxItems": 128}}, ["messages"])),
    ("relay_receive", "Receive up to 128 messages. timeout=0 checks once; a positive timeout waits for an event, with no polling loop. Reply using sender and id.", schema({"timeout": {"type": "number", "minimum": 0, "maximum": 30}, "limit": {"type": "integer", "minimum": 1, "maximum": 128}})),
    ("relay_store", "Store up to 1 MiB of UTF-8 text once and send its SHA-256 reference instead of repeating large text. The bounded store is ephemeral and may evict old data.", schema({"text": TEXT}, ["text"])),
    ("relay_load", "Fetch stored UTF-8 text by SHA-256 reference. Fails if missing or evicted.", schema({"reference": TEXT}, ["reference"])),
    ("relay_stats", "Inspect registered agents, routed/rejected messages and bounded queue/store memory.", schema({})),
]


class Tools:
    def __init__(self, agent: Agent):
        self.agent = agent

    def openai(self):
        return [{"type": "function", "name": name, "description": description, "parameters": params, "strict": False} for name, description, params in DEFINITIONS]

    def anthropic(self):
        return [{"name": name, "description": description, "input_schema": params} for name, description, params in DEFINITIONS]

    async def call(self, name: str, arguments: dict):
        definitions = {n: p for n, _, p in DEFINITIONS}
        if name not in definitions or not isinstance(arguments, dict):
            raise ValueError("unknown tool or invalid arguments")
        params = definitions[name]
        if set(arguments) - set(params["properties"]) or any(k not in arguments for k in params["required"]):
            raise ValueError("unexpected or missing argument")
        if name == "relay_send":
            if not isinstance(arguments["to"], str) or not isinstance(arguments["payload"], dict):
                raise ValueError("to must be a string and payload an object")
            if arguments.get("kind", "event") not in ("event", "task", "reply", "error"):
                raise ValueError("invalid message kind")
            if "correlation_id" in arguments and not isinstance(arguments["correlation_id"], str):
                raise ValueError("correlation_id must be a string")
            return {"queued": True, "id": await self.agent.send(**arguments)}
        if name == "relay_send_batch":
            messages = arguments["messages"]
            if not isinstance(messages, list) or not 1 <= len(messages) <= 128:
                raise ValueError("batch must contain 1..128 messages")
            async def send(item):
                try:
                    return await self.call("relay_send", item)
                except Exception as exc:
                    return {"queued": False, "error": str(exc)}
            return {"results": await asyncio.gather(*(send(item) for item in messages))}
        if name == "relay_receive":
            timeout, limit = arguments.get("timeout", 0), arguments.get("limit", 32)
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 <= timeout <= 30:
                raise ValueError("timeout must be 0..30 seconds")
            if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 128:
                raise ValueError("limit must be 1..128")
            messages = []
            if timeout > 0:
                try:
                    messages.append(await self.agent.receive(timeout=timeout))
                except asyncio.TimeoutError:
                    return {"messages": []}
            while len(messages) < limit and not self.agent.client.inbox.empty():
                messages.append(await self.agent.receive())
            return {"messages": messages}
        if name == "relay_store":
            if not isinstance(arguments["text"], str):
                raise ValueError("text must be a string")
            data = arguments["text"].encode()
            if len(data) > 1024 * 1024:
                raise ValueError("text exceeds 1 MiB")
            return {"reference": await self.agent.client.store(data), "bytes": len(data)}
        if name == "relay_load":
            reference = arguments["reference"]
            if not isinstance(reference, str) or len(reference) != 64:
                raise ValueError("reference must be a SHA-256 hex string")
            return {"text": (await self.agent.client.load(reference)).decode()}
        return await self.agent.client.stats()

    async def execute_openai(self, output_items):
        """Execute Responses API function_call objects concurrently."""
        async def execute(item):
            try:
                import json
                result = await self.call(item.name, json.loads(item.arguments))
            except Exception as exc:
                result = {"error": str(exc)}
            return {"type": "function_call_output", "call_id": item.call_id, "output": encode(result).decode()}
        return await asyncio.gather(*(execute(item) for item in output_items if item.type == "function_call"))

    async def execute_anthropic(self, content_blocks):
        """Execute Anthropic Messages API tool_use objects concurrently."""
        async def execute(item):
            failed = False
            try:
                result = await self.call(item.name, item.input)
            except Exception as exc:
                result, failed = {"error": str(exc)}, True
            return {"type": "tool_result", "tool_use_id": item.id, "content": encode(result).decode(), "is_error": failed}
        return await asyncio.gather(*(execute(item) for item in content_blocks if item.type == "tool_use"))
