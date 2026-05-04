import socket
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Set
from ..config.config import CHUNK_SIZE, connections, devices, sync_rooms, log


# ─── Helpers ─────────────────────────────────────────────────────────────────
def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_device_name() -> str:
    return socket.gethostname()


def file_hash(path: Path, algo="sha256") -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


async def broadcast(message: dict, exclude: Optional[str] = None):
    """Broadcast a message to all connected devices."""
    dead = []
    for dev_id, ws in connections.items():
        if dev_id == exclude:
            continue
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(dev_id)
    for dev_id in dead:
        _remove_device(dev_id)


async def send_to(device_id: str, message: dict) -> bool:
    ws = connections.get(device_id)
    if not ws:
        return False
    try:
        await ws.send_json(message)
        return True
    except Exception:
        _remove_device(device_id)
        return False


def _remove_device(device_id: str):
    devices.pop(device_id, None)
    connections.pop(device_id, None)
    for members in sync_rooms.values():
        members.discard(device_id)
    log.info(f"Device removed: {device_id}")
