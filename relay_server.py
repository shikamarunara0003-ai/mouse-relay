"""
relay_server.py
----------------
Servidor "puente" (relay) por WebSocket.

- El HOST se conecta y envía su posición de mouse (normalizada 0.0-1.0).
- Los VIEWERS se conectan y reciben esa posición en tiempo real.
- Solo puede haber un host activo a la vez (el último que se registre "gana").

Este script debe correr en algún lugar accesible por internet para que
host y viewers en redes distintas se puedan conectar. Ver README.md
para opciones de despliegue (ngrok, Render, Railway, etc.)
"""

import asyncio
import json
import logging
import os

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("relay")

HOST_CONN = None          # conexión websocket del host activo
VIEWERS: set = set()      # conexiones websocket de los viewers


async def handler(websocket):
    global HOST_CONN
    role = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "register":
                role = data.get("role")
                if role == "host":
                    HOST_CONN = websocket
                    log.info("Host conectado desde %s", websocket.remote_address)
                elif role == "viewer":
                    VIEWERS.add(websocket)
                    log.info("Viewer conectado (%d total)", len(VIEWERS))

            elif msg_type == "pos" and websocket is HOST_CONN:
                # Reenviar la posición a todos los viewers conectados
                if VIEWERS:
                    stale = set()
                    send_tasks = []
                    for viewer in VIEWERS:
                        send_tasks.append(_safe_send(viewer, message, stale))
                    await asyncio.gather(*send_tasks)
                    VIEWERS.difference_update(stale)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        VIEWERS.discard(websocket)
        if websocket is HOST_CONN:
            HOST_CONN = None
            log.info("Host desconectado")
        elif role == "viewer":
            log.info("Viewer desconectado (%d restantes)", len(VIEWERS))


async def _safe_send(viewer, message, stale_set):
    try:
        await viewer.send(message)
    except websockets.exceptions.ConnectionClosed:
        stale_set.add(viewer)


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port, ping_interval=20, ping_timeout=20):
        log.info("Servidor relay escuchando en el puerto %d", port)
        await asyncio.Future()  # correr para siempre


if __name__ == "__main__":
    asyncio.run(main())
