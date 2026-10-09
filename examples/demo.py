"""One coordinator and three callable agents; no API keys required."""
import asyncio
import argparse
from swarmwire import Agent, Client


async def main(port=8765, unix=None):
    clients = [await Client.connect(name, port=port, unix=unix) for name in ("coordinator", "research", "code", "review")]
    agents = [Agent(client) for client in clients]

    async def worker(payload, message):
        return {"agent": message["payload"]["role"], "result": payload["value"] ** 2}

    tasks = [asyncio.create_task(agent.serve(worker)) for agent in agents[1:]]
    try:
        requests = [asyncio.create_task(agents[0].request(name, {"role": name, "value": i + 2})) for i, name in enumerate(("research", "code", "review"))]
        for ready in asyncio.as_completed(requests):
            print((await ready)["payload"])
        print("Router:", await clients[0].stats())
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(client.close() for client in clients))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--unix")
    parser.add_argument("--fast", action="store_true")
    args = parser.parse_args()
    if args.fast:
        import uvloop
        uvloop.install()
    asyncio.run(main(args.port, args.unix))
