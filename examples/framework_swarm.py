"""Real LangGraph plus a generic async agent, with no model/API calls."""
import argparse
import asyncio
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from swarmwire import Swarm, LangGraphAdapter, AdaptiveTransport


class State(TypedDict):
    question: str
    answer: str


def answer(state):
    return {"answer": state["question"].upper()}


async def main(args):
    graph = StateGraph(State).add_node("answer", answer).add_edge(START, "answer").add_edge("answer", END).compile()
    adapter = LangGraphAdapter(graph, result_mapper=lambda state: {"answer": state["answer"]})
    async def generic(payload, message):
        return {"answer": payload["question"][::-1]}
    async with Swarm(AdaptiveTransport(unix=args.unix) if args.unix else None) as swarm:
        await swarm.register("langgraph", adapter)
        await swarm.register("generic", generic)
        async for name, result in swarm.explore({"langgraph": {"question": "explore"}, "generic": {"question": "explore"}}):
            print(name, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--unix', help='use adaptive transport with an existing native broker')
    asyncio.run(main(parser.parse_args()))
