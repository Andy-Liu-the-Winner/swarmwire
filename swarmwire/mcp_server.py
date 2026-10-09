"""MCP stdio facade using the official SDK for protocol negotiation."""
import asyncio
from .client import Agent, Client, encode
from .tools import DEFINITIONS, Tools


async def run(name, port, unix):
    try:
        from mcp.server import Server
        from mcp.server.stdio import stdio_server
        from mcp.types import TextContent, Tool
    except ImportError as exc:
        raise RuntimeError('Install the MCP extra: pip install ".[mcp]"') from exc
    client = await Client.connect(name, port=port, unix=unix)
    tools = Tools(Agent(client))
    server = Server("swarmwire")

    @server.list_tools()
    async def list_tools():
        return [Tool(name=n, description=d, inputSchema=s) for n, d, s in DEFINITIONS]

    @server.call_tool()
    async def call_tool(name, arguments):
        result = await tools.call(name, arguments)
        return [TextContent(type="text", text=encode(result).decode())]

    try:
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())
    finally:
        await client.close()
