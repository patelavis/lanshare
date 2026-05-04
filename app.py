"""
LANShare — Integrated startup
Wires together: FastAPI app + UDP discovery + background workers
"""

import asyncio
import os
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from server.discovery import start_discovery, stop_discovery
from server.workers import periodic_gc, task_queue
from server.main import TEMP_DIR, app, transfer_sessions


@asynccontextmanager
async def lifespan(app):
    """Startup/shutdown lifecycle."""
    port = int(os.environ.get("PORT", 7070))

    # Start background workers
    await task_queue.start()

    # Start UDP discovery beacon
    asyncio.create_task(start_discovery(http_port=port))

    # Start periodic garbage collection (every 5 min)
    asyncio.create_task(periodic_gc(TEMP_DIR, transfer_sessions, interval=300))

    print(f"\n{'='*55}")
    print(f"  🚀  LANShare  —  LAN File Transfer System")
    print(f"{'='*55}")
    print(f"  HTTP  →  http://0.0.0.0:{port}")
    print(f"  LAN   →  http://{get_local_ip()}:{port}")
    print(f"  UDP   →  discovery on port 7071")
    print(f"  Open the LAN address on any device on your Wi-Fi")
    print(f"{'='*55}\n")

    yield

    # Shutdown
    await task_queue.stop()
    await stop_discovery()


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# Attach lifespan
app.router.lifespan_context = lifespan

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 7070))
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
        ws_ping_interval=10,
        ws_ping_timeout=20,
        access_log=True,
    )
