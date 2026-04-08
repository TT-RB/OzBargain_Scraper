import asyncio
import os
from aiohttp import web


async def start_health_server(port: int = None):
    port = int(port or os.environ.get("PORT", "8000"))

    async def handle(request):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", handle)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    # keep running until cancelled
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await runner.cleanup()
        raise
