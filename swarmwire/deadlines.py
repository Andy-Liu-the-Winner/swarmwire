"""Deadlines without an extra task on modern Python or async-timeout 5."""
import asyncio

try:
    timeout_context = asyncio.timeout
except AttributeError:
    try:
        from async_timeout import timeout as timeout_context
    except ImportError:
        timeout_context = None


async def deadline(awaitable, seconds):
    if seconds is None:
        return await awaitable
    if timeout_context is None:
        return await asyncio.wait_for(awaitable, seconds)
    async with timeout_context(seconds):
        return await awaitable
