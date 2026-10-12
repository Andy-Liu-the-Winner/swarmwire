"""Existing async agent + configuration-selected transport. No model/API calls."""
import asyncio
from swarmwire import CallableAdapter, Swarm

async def existing_agent(payload):
    return {'answer': payload['question'].upper()}

async def main():
    async with Swarm() as swarm:
        await swarm.register('researcher', CallableAdapter(existing_agent))
        print(await swarm.request('researcher', {'question': 'explore'}))
        print(swarm.status())

if __name__ == '__main__':
    asyncio.run(main())
