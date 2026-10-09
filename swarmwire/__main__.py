import argparse
import asyncio
import os
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="SwarmWire: native agent message transport")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build", help="build the C++ broker with your existing compiler")
    mcp = sub.add_parser("mcp", help="run a stdio MCP adapter connected to a running broker")
    mcp.add_argument("--agent", required=True)
    mcp.add_argument("--port", type=int, default=8765)
    mcp.add_argument("--unix")
    args = parser.parse_args()
    if args.command == "build":
        root = Path(__file__).resolve().parent.parent
        source = root / "native" / "swarmwire.cpp"
        if not source.exists():
            parser.error("build from the source directory; native source is included in the source bundle")
        (root / "bin").mkdir(exist_ok=True)
        subprocess.run([os.getenv("CXX", "c++"), "-O3", "-DNDEBUG", "-std=c++17", "-Wall", "-Wextra", "-Wpedantic", str(source), "-o", str(root / "bin" / "swarmwired")], check=True)
        print(root / "bin" / "swarmwired")
    else:
        from .mcp_server import run
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass
        asyncio.run(run(args.agent, args.port, args.unix))


if __name__ == "__main__":
    main()
