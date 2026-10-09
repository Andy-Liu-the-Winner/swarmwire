"""Pass in your existing async provider client and explicit model.

These functions are not run by tests and do not read API keys themselves.
Each provider tool call still incurs its normal model/API latency.
"""
from swarmwire import Tools


async def openai_turn(client, model, prompt, agent, max_rounds=8):
    tools = Tools(agent)
    response = await client.responses.create(model=model, input=prompt, tools=tools.openai())
    for _ in range(max_rounds):
        results = await tools.execute_openai(response.output)
        if not results:
            return response.output_text
        response = await client.responses.create(model=model, previous_response_id=response.id, input=results, tools=tools.openai())
    raise RuntimeError("OpenAI tool-round budget exhausted")


async def anthropic_turn(client, model, prompt, agent, max_rounds=8):
    tools = Tools(agent)
    messages = [{"role": "user", "content": prompt}]
    for _ in range(max_rounds):
        response = await client.messages.create(model=model, max_tokens=1024, messages=messages, tools=tools.anthropic())
        results = await tools.execute_anthropic(response.content)
        if not results:
            return "".join(block.text for block in response.content if block.type == "text")
        messages.extend([{"role": "assistant", "content": response.content}, {"role": "user", "content": results}])
    raise RuntimeError("Anthropic tool-round budget exhausted")
