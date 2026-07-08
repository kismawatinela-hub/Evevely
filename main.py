"""
Render Web Service entrypoint.

Render requires a process that listens on $PORT for health checks.
The Telegram bot runs in the main thread; a tiny HTTP server runs in the background.
"""
import asyncio
import os
import threading
import time

from aiohttp import web


def _run_health_server():
    async def _serve():
        app = web.Application()

        async def ok(_request):
            return web.Response(text='OK')

        app.router.add_get('/', ok)
        app.router.add_get('/health', ok)

        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.environ.get('PORT', '10000'))
        await web.TCPSite(runner, '0.0.0.0', port).start()
        print(f'[render] health server listening on 0.0.0.0:{port}')
        await asyncio.Future()

    asyncio.run(_serve())


if __name__ == '__main__':
    threading.Thread(target=_run_health_server, daemon=True).start()
    time.sleep(0.5)
    from bot import run_bot

    run_bot()
