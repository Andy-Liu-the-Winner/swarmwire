"""Binary RPC echo worker or caller; run the broker before this example."""
import argparse
import asyncio
from swarmwire import PeerEndpoint, RawRPC


async def main(args):
    endpoint = await PeerEndpoint.connect(args.name, unix=args.unix)
    rpc = RawRPC(endpoint)
    try:
        if args.to:
            await endpoint.preconnect(args.to)
            print((await rpc.request(args.to, args.text.encode())).decode())
        else:
            async def handler(data, message):
                return data.upper()
            await rpc.serve(handler, concurrency=4)
    finally:
        await endpoint.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True)
    parser.add_argument('--unix', default='/tmp/swarmwire.sock')
    parser.add_argument('--to')
    parser.add_argument('--text', default='explore faster')
    asyncio.run(main(parser.parse_args()))
