import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess


async def check(backend):
    from .runtime import Swarm
    async def echo(payload, message):
        return payload
    async with Swarm(backend=backend) as swarm:
        await swarm.register('installation-check', echo)
        result = await swarm.request('installation-check', {'ok': True})
        print(json.dumps({'ok': result == {'ok': True}, **swarm.status()}))


async def run(command, backend):
    from .managed import LocalBroker, configured_transport
    if command and command[0] == '--':
        command = command[1:]
    if not command:
        raise ValueError('Provide a command, for example: swarmwire run -- python app.py')
    env = os.environ.copy()
    broker, child = None, None
    try:
        if backend == 'local':
            broker = await LocalBroker().start()
            env.update(SWARMWIRE_BACKEND='external', SWARMWIRE_SOCKET=broker.unix, SWARMWIRE_TOKEN=broker.token)
        else:
            configured_transport(backend)
            env['SWARMWIRE_BACKEND'] = backend
        child = subprocess.Popen(command, env=env)
        return await asyncio.to_thread(child.wait)
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                await asyncio.to_thread(child.wait, timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                await asyncio.to_thread(child.wait)
        if broker is not None:
            await broker.close()


def main():
    parser = argparse.ArgumentParser(description='SwarmWire: local agent communication without a managed cloud service')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('build', help='prepare the native broker with your existing compiler')
    probe = sub.add_parser('check', help='verify installation with one offline request; no API keys')
    probe.add_argument('--backend', choices=('memory', 'local', 'auto', 'external', 'nats'), default='local')
    launch = sub.add_parser('run', help='start a private broker, run your application, then clean up')
    launch.add_argument('--backend', choices=('local', 'memory', 'nats', 'external'), default='local')
    launch.add_argument('application', nargs=argparse.REMAINDER)
    mcp = sub.add_parser('mcp', help='run a stdio MCP adapter connected to an existing broker')
    mcp.add_argument('--agent', required=True)
    mcp.add_argument('--port', type=int, default=8765)
    mcp.add_argument('--unix')
    args = parser.parse_args()
    try:
        if args.command == 'build':
            from ._build import compile_binary, ensure_binary, source_path
            root = Path(__file__).resolve().parent.parent
            print(compile_binary(source_path(), root / 'bin' / 'swarmwired') if (root / 'native').is_dir() else ensure_binary())
        elif args.command == 'check':
            asyncio.run(check(args.backend))
        elif args.command == 'run':
            code = asyncio.run(run(args.application, args.backend))
            raise SystemExit(code if code >= 0 else 128 - code)
        else:
            from .mcp_server import run as run_mcp
            try:
                import uvloop
                uvloop.install()
            except ImportError:
                pass
            asyncio.run(run_mcp(args.agent, args.port, args.unix))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as exc:
        parser.exit(1, 'swarmwire: ' + str(exc) + '\n')


if __name__ == '__main__':
    main()
