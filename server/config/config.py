from pathlib import Path
from typing import Dict, List, Optional, Set
from fastapi import WebSocket
from collections import defaultdict
from pydantic import BaseModel
import logging

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("lanshare")

# ─── Config ─────────────────────────────────────────────────────────────────
CHUNK_SIZE = 512 * 1024  # 512 KB chunks
UPLOAD_DIR = Path("./uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR = Path("./uploads/.tmp")
TEMP_DIR.mkdir(exist_ok=True)


# ─── In-memory state ─────────────────────────────────────────────────────────
devices: Dict[str, dict] = {}  # device_id → device info
connections: Dict[str, WebSocket] = {}  # device_id → ws
transfer_sessions: Dict[str, dict] = {}  # session_id → transfer state
chunk_buffers: Dict[str, dict] = {}  # session_id → chunk accumulator
sync_rooms: Dict[str, Set[str]] = defaultdict(set)  # room → device_ids


# ─── Models ──────────────────────────────────────────────────────────────────
class DeviceInfo(BaseModel):
    device_id: str
    name: str
    platform: str
    ip: str
    port: int
    capabilities: List[str] = ["send", "receive", "sync"]


class TransferRequest(BaseModel):
    session_id: str
    sender_id: str
    receiver_ids: List[str]
    files: List[dict]  # [{name, size, mime, path?}]


class SyncRequest(BaseModel):
    room: str
    device_id: str
    folder_path: str
    file_tree: List[dict]
