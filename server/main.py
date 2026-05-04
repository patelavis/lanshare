"""
LANShare — Production-grade LAN file transfer server
FastAPI + WebSocket + AsyncIO + chunked streaming
"""

import asyncio
import hashlib
import json
import logging
import os
import platform
import shutil
import socket
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import aiofiles
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from .config.config import *
from .utils.helper import (
    get_local_ip,
    get_device_name,
    file_hash,
    broadcast,
    send_to,
    _remove_device,
)


app = FastAPI(title="LANShare", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── WebSocket hub ────────────────────────────────────────────────────────────
@app.websocket("/ws/{device_id}")
async def websocket_endpoint(ws: WebSocket, device_id: str):
    await ws.accept()
    connections[device_id] = ws
    log.info(f"WS connected: {device_id}")

    # Send current device list to newcomer
    await ws.send_json(
        {
            "type": "device_list",
            "devices": list(devices.values()),
        }
    )

    try:
        while True:
            data = await ws.receive_json()
            msg_type = data.get("type")

            # ── Register / heartbeat ──────────────────────────────────────
            if msg_type == "register":
                info = data["device"]
                devices[device_id] = {**info, "last_seen": time.time(), "online": True}
                await broadcast(
                    {"type": "device_joined", "device": devices[device_id]},
                    exclude=device_id,
                )
                log.info(f"Registered: {info.get('name')} ({device_id})")

            elif msg_type == "heartbeat":
                if device_id in devices:
                    devices[device_id]["last_seen"] = time.time()
                await ws.send_json({"type": "heartbeat_ack", "ts": time.time()})

            # ── Transfer signalling ───────────────────────────────────────
            elif msg_type == "transfer_request":
                # Forward invitation to each receiver
                session_id = data["session_id"]
                transfer_sessions[session_id] = {
                    "sender_id": device_id,
                    "receiver_ids": data["receiver_ids"],
                    "files": data["files"],
                    "status": "pending",
                    "accepted": set(),
                    "rejected": set(),
                    "created": time.time(),
                }
                for rid in data["receiver_ids"]:
                    await send_to(
                        rid,
                        {
                            "type": "transfer_invitation",
                            "session_id": session_id,
                            "sender": devices.get(device_id, {}),
                            "files": data["files"],
                        },
                    )

            elif msg_type == "transfer_accept":
                sid = data["session_id"]
                if sid in transfer_sessions:
                    transfer_sessions[sid]["accepted"].add(device_id)
                    sender_id = transfer_sessions[sid]["sender_id"]
                    await send_to(
                        sender_id,
                        {
                            "type": "transfer_accepted",
                            "session_id": sid,
                            "receiver_id": device_id,
                        },
                    )

            elif msg_type == "transfer_reject":
                sid = data["session_id"]
                if sid in transfer_sessions:
                    transfer_sessions[sid]["rejected"].add(device_id)
                    sender_id = transfer_sessions[sid]["sender_id"]
                    await send_to(
                        sender_id,
                        {
                            "type": "transfer_rejected",
                            "session_id": sid,
                            "receiver_id": device_id,
                        },
                    )

            elif msg_type == "transfer_progress":
                # Relay progress to sender
                sid = data["session_id"]
                if sid in transfer_sessions:
                    sender_id = transfer_sessions[sid]["sender_id"]
                    await send_to(sender_id, {**data, "receiver_id": device_id})

            elif msg_type == "transfer_complete":
                sid = data["session_id"]
                await broadcast(
                    {
                        "type": "transfer_complete",
                        "session_id": sid,
                        "receiver_id": device_id,
                    }
                )

            # ── Sync room ─────────────────────────────────────────────────
            elif msg_type == "sync_join":
                room = data["room"]
                sync_rooms[room].add(device_id)
                # Notify others in room
                for member in sync_rooms[room]:
                    if member != device_id:
                        await send_to(
                            member,
                            {
                                "type": "sync_peer_joined",
                                "room": room,
                                "device": devices.get(device_id, {}),
                            },
                        )

            elif msg_type == "sync_broadcast":
                room = data.get("room")
                if room and room in sync_rooms:
                    for member in sync_rooms[room]:
                        if member != device_id:
                            await send_to(member, {**data, "from_device": device_id})

            # ── Chat / clipboard share ────────────────────────────────────
            elif msg_type == "clipboard_share":
                target_ids = data.get("targets", list(connections.keys()))
                for tid in target_ids:
                    if tid != device_id:
                        await send_to(
                            tid,
                            {
                                "type": "clipboard_received",
                                "content": data["content"],
                                "from": devices.get(device_id, {}).get(
                                    "name", device_id
                                ),
                            },
                        )

    except WebSocketDisconnect:
        log.info(f"WS disconnected: {device_id}")
    except Exception as e:
        log.error(f"WS error for {device_id}: {e}")
    finally:
        _remove_device(device_id)
        await broadcast({"type": "device_left", "device_id": device_id})


# ─── Device discovery REST ────────────────────────────────────────────────────
@app.get("/api/devices")
async def list_devices():
    # Prune stale (>15s no heartbeat)
    now = time.time()
    alive = {k: v for k, v in devices.items() if now - v.get("last_seen", 0) < 15}
    return {"devices": list(alive.values()), "count": len(alive)}


@app.get("/api/info")
async def server_info():
    return {
        "server_name": get_device_name(),
        "ip": get_local_ip(),
        "platform": platform.system(),
        "version": "1.0.0",
        "upload_dir": str(UPLOAD_DIR.absolute()),
    }


# ─── Chunked upload ───────────────────────────────────────────────────────────
@app.post("/api/upload/init")
async def upload_init(
    session_id: str = Form(...),
    filename: str = Form(...),
    total_size: int = Form(...),
    total_chunks: int = Form(...),
    file_hash: Optional[str] = Form(None),
    sender_id: str = Form(...),
    receiver_id: str = Form(...),
):
    upload_id = f"{session_id}_{uuid.uuid4().hex[:8]}"
    chunk_buffers[upload_id] = {
        "session_id": session_id,
        "filename": filename,
        "total_size": total_size,
        "total_chunks": total_chunks,
        "received_chunks": set(),
        "expected_hash": file_hash,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "created": time.time(),
        "tmp_path": str(TEMP_DIR / upload_id),
    }
    Path(chunk_buffers[upload_id]["tmp_path"]).mkdir(exist_ok=True)
    log.info(
        f"Upload init: {filename} ({total_size} bytes, {total_chunks} chunks) [{upload_id}]"
    )
    return {"upload_id": upload_id, "status": "ready"}


@app.post("/api/upload/chunk")
async def upload_chunk(
    upload_id: str = Form(...),
    chunk_index: int = Form(...),
    chunk_hash: Optional[str] = Form(None),
    chunk_data: UploadFile = File(...),
):
    if upload_id not in chunk_buffers:
        raise HTTPException(404, "Upload session not found")

    state = chunk_buffers[upload_id]
    chunk_path = Path(state["tmp_path"]) / f"chunk_{chunk_index:06d}"

    data = await chunk_data.read()

    # Optional chunk integrity check
    if chunk_hash:
        actual = hashlib.sha256(data).hexdigest()
        if actual != chunk_hash:
            raise HTTPException(400, f"Chunk {chunk_index} hash mismatch")

    async with aiofiles.open(chunk_path, "wb") as f:
        await f.write(data)

    state["received_chunks"].add(chunk_index)
    progress = len(state["received_chunks"]) / state["total_chunks"]

    # Notify receiver via WS
    receiver_id = state.get("receiver_id")
    if receiver_id in connections:
        await send_to(
            receiver_id,
            {
                "type": "transfer_progress",
                "session_id": state["session_id"],
                "upload_id": upload_id,
                "filename": state["filename"],
                "progress": round(progress, 4),
                "received_chunks": len(state["received_chunks"]),
                "total_chunks": state["total_chunks"],
            },
        )

    return {
        "chunk_index": chunk_index,
        "received": len(state["received_chunks"]),
        "total": state["total_chunks"],
        "progress": progress,
    }


@app.post("/api/upload/finalize")
async def upload_finalize(upload_id: str = Form(...)):
    if upload_id not in chunk_buffers:
        raise HTTPException(404, "Upload session not found")

    state = chunk_buffers[upload_id]
    expected = set(range(state["total_chunks"]))
    missing = expected - state["received_chunks"]

    if missing:
        raise HTTPException(400, f"Missing chunks: {sorted(missing)[:10]}")

    # Reassemble
    safe_name = Path(state["filename"]).name
    out_path = UPLOAD_DIR / safe_name
    # Handle duplicates
    counter = 1
    stem = Path(safe_name).stem
    suffix = Path(safe_name).suffix
    while out_path.exists():
        out_path = UPLOAD_DIR / f"{stem}_{counter}{suffix}"
        counter += 1

    async with aiofiles.open(out_path, "wb") as outf:
        for i in range(state["total_chunks"]):
            chunk_path = Path(state["tmp_path"]) / f"chunk_{i:06d}"
            async with aiofiles.open(chunk_path, "rb") as cf:
                await outf.write(await cf.read())

    # Cleanup temp
    shutil.rmtree(state["tmp_path"], ignore_errors=True)

    # Verify hash
    actual_hash = None
    if state.get("expected_hash"):
        actual_hash = file_hash(out_path)
        if actual_hash != state["expected_hash"]:
            out_path.unlink(missing_ok=True)
            del chunk_buffers[upload_id]
            raise HTTPException(400, "File hash mismatch after reassembly")

    file_size = out_path.stat().st_size
    del chunk_buffers[upload_id]

    log.info(f"Upload complete: {out_path.name} ({file_size} bytes)")

    # Notify receiver
    receiver_id = state.get("receiver_id")
    if receiver_id in connections:
        await send_to(
            receiver_id,
            {
                "type": "file_received",
                "session_id": state["session_id"],
                "filename": out_path.name,
                "size": file_size,
                "hash": actual_hash,
                "download_url": f"/api/download/{out_path.name}",
            },
        )

    return {
        "status": "complete",
        "filename": out_path.name,
        "size": file_size,
        "download_url": f"/api/download/{out_path.name}",
    }


# ─── Download (streaming) ──────────────────────────────────────────────────────
@app.get("/api/download/{filename}")
async def download_file(filename: str, request: Request):
    path = UPLOAD_DIR / Path(filename).name  # prevent path traversal
    if not path.exists() or not path.is_file():
        raise HTTPException(404, "File not found")

    file_size = path.stat().st_size
    range_header = request.headers.get("range")

    async def iter_file(start=0, end=None):
        async with aiofiles.open(path, "rb") as f:
            await f.seek(start)
            remaining = (end - start + 1) if end else file_size - start
            while remaining > 0:
                read_size = min(CHUNK_SIZE, remaining)
                chunk = await f.read(read_size)
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    if range_header:
        # Partial content support
        range_val = range_header.replace("bytes=", "")
        start_str, end_str = range_val.split("-")
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Disposition": f'attachment; filename="{path.name}"',
        }
        return StreamingResponse(
            iter_file(start, end),
            status_code=206,
            headers=headers,
            media_type="application/octet-stream",
        )

    return StreamingResponse(
        iter_file(),
        headers={
            "Content-Length": str(file_size),
            "Content-Disposition": f'attachment; filename="{path.name}"',
            "Accept-Ranges": "bytes",
        },
        media_type="application/octet-stream",
    )


# ─── File listing ─────────────────────────────────────────────────────────────
@app.get("/api/files")
async def list_files():
    files = []
    for p in sorted(UPLOAD_DIR.iterdir()):
        if p.is_file() and not p.name.startswith("."):
            stat = p.stat()
            files.append(
                {
                    "name": p.name,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "download_url": f"/api/download/{p.name}",
                }
            )
    return {"files": files, "count": len(files)}


@app.delete("/api/files/{filename}")
async def delete_file(filename: str):
    path = UPLOAD_DIR / Path(filename).name
    if not path.exists():
        raise HTTPException(404, "File not found")
    path.unlink()
    return {"status": "deleted", "filename": filename}


# ─── Folder sync diff ─────────────────────────────────────────────────────────
@app.post("/api/sync/diff")
async def sync_diff(req: SyncRequest):
    """
    Client sends its file tree; server returns which files are new/missing/changed.
    """
    client_files = {f["name"]: f for f in req.file_tree}
    server_files = {}
    for p in UPLOAD_DIR.iterdir():
        if p.is_file() and not p.name.startswith("."):
            stat = p.stat()
            server_files[p.name] = {
                "name": p.name,
                "size": stat.st_size,
                "modified": stat.st_mtime,
            }

    need_upload = [f for f in client_files if f not in server_files]
    need_download = [f for f in server_files if f not in client_files]
    conflicts = []
    for name in client_files:
        if name in server_files:
            cf = client_files[name]
            sf = server_files[name]
            if cf.get("size") != sf["size"]:
                conflicts.append(name)

    return {
        "need_upload": need_upload,
        "need_download": [
            {"name": n, **server_files[n], "download_url": f"/api/download/{n}"}
            for n in need_download
        ],
        "conflicts": conflicts,
    }


# ─── Transfer sessions ────────────────────────────────────────────────────────
@app.get("/api/transfers/{session_id}")
async def get_transfer(session_id: str):
    s = transfer_sessions.get(session_id)
    if not s:
        raise HTTPException(404, "Session not found")
    return {**s, "accepted": list(s["accepted"]), "rejected": list(s["rejected"])}


# ─── Serve UI ─────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    ui_path = Path(__file__).parent.parent / "static" / "index.html"
    if ui_path.exists():
        return HTMLResponse(ui_path.read_text(encoding="utf-8"))
    return HTMLResponse(
        "<h1>LANShare server running. Place index.html in /static/</h1>"
    )


# Mount static assets if folder exists
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    local_ip = get_local_ip()
    port = int(os.environ.get("PORT", 7070))
    print(f"\n{'='*55}")
    print(f"  🚀 LANShare Server")
    print(f"  Local:   http://localhost:{port}")
    print(f"  Network: http://{local_ip}:{port}")
    print(f"  Share this address with devices on your Wi-Fi!")
    print(f"{'='*55}\n")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info",
        ws_ping_interval=10,
        ws_ping_timeout=20,
    )
