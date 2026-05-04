"""
LANShare Background Workers
Handles: folder indexing, chunk GC, stale session cleanup, file hashing queue
Uses asyncio task queue (no broker needed for LAN use — drop-in Celery replacement)
"""

import asyncio
import hashlib
import logging
import shutil
import time
from pathlib import Path
from typing import Callable, Dict, Optional

log = logging.getLogger("lanshare.workers")


class TaskQueue:
    """Simple asyncio-based task queue — Celery-style without the broker."""

    def __init__(self, workers: int = 4):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._workers = workers
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self.stats: Dict[str, int] = {"completed": 0, "failed": 0, "queued": 0}

    async def start(self):
        self._running = True
        self._tasks = [
            asyncio.create_task(self._worker(i)) for i in range(self._workers)
        ]
        log.info(f"TaskQueue started with {self._workers} workers")

    async def stop(self):
        self._running = False
        for _ in self._tasks:
            await self._queue.put(None)  # Poison pills
        await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("TaskQueue stopped")

    async def submit(self, coro_fn: Callable, *args, **kwargs):
        """Enqueue a coroutine function."""
        await self._queue.put((coro_fn, args, kwargs))
        self.stats["queued"] += 1

    async def _worker(self, worker_id: int):
        while self._running:
            item = await self._queue.get()
            if item is None:
                break
            coro_fn, args, kwargs = item
            try:
                await coro_fn(*args, **kwargs)
                self.stats["completed"] += 1
            except Exception as e:
                log.error(f"Worker {worker_id} error: {e}")
                self.stats["failed"] += 1
            finally:
                self._queue.task_done()


# ─── Global task queue ────────────────────────────────────────────────────────
task_queue = TaskQueue(workers=4)


# ─── Worker tasks ─────────────────────────────────────────────────────────────

async def index_folder(folder_path: Path, result_store: dict):
    """
    Walk a folder and build a file index with hashes.
    Result is stored in result_store[str(folder_path)].
    """
    log.info(f"[index_folder] Indexing: {folder_path}")
    index = []
    if not folder_path.exists():
        result_store[str(folder_path)] = {"error": "not found"}
        return

    for p in sorted(folder_path.rglob("*")):
        if p.is_file() and not p.name.startswith("."):
            try:
                stat = p.stat()
                rel = str(p.relative_to(folder_path))
                entry = {
                    "path": rel,
                    "name": p.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "hash": None,  # lazy — computed on demand
                }
                index.append(entry)
            except PermissionError:
                continue

    result_store[str(folder_path)] = {
        "files": index,
        "count": len(index),
        "total_size": sum(f["size"] for f in index),
        "indexed_at": time.time(),
    }
    log.info(f"[index_folder] Done: {len(index)} files")


async def compute_file_hash(file_path: Path, result_store: dict, chunk_size: int = 512 * 1024):
    """Compute SHA-256 of a file without blocking the event loop."""
    log.info(f"[hash] Computing hash: {file_path.name}")
    h = hashlib.sha256()
    loop = asyncio.get_event_loop()

    def _hash_sync():
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()

    digest = await loop.run_in_executor(None, _hash_sync)
    result_store[str(file_path)] = digest
    log.info(f"[hash] {file_path.name} → {digest[:16]}…")


async def cleanup_temp_chunks(temp_dir: Path, max_age_seconds: int = 3600):
    """Remove temp chunk directories older than max_age_seconds."""
    log.info(f"[gc] Cleaning temp chunks in {temp_dir}")
    if not temp_dir.exists():
        return
    now = time.time()
    removed = 0
    for child in temp_dir.iterdir():
        if child.is_dir():
            age = now - child.stat().st_mtime
            if age > max_age_seconds:
                shutil.rmtree(child, ignore_errors=True)
                removed += 1
    log.info(f"[gc] Removed {removed} stale temp dirs")


async def cleanup_stale_sessions(sessions: dict, max_age_seconds: int = 7200):
    """Remove transfer sessions older than max_age_seconds."""
    now = time.time()
    stale = [sid for sid, s in sessions.items() if now - s.get("created", 0) > max_age_seconds]
    for sid in stale:
        sessions.pop(sid, None)
    if stale:
        log.info(f"[gc] Cleaned {len(stale)} stale transfer sessions")


async def periodic_gc(temp_dir: Path, sessions: dict, interval: int = 300):
    """Run GC every `interval` seconds."""
    while True:
        await asyncio.sleep(interval)
        await cleanup_temp_chunks(temp_dir)
        await cleanup_stale_sessions(sessions)


async def watch_folder(
    folder_path: Path,
    on_change: Callable,
    poll_interval: float = 2.0,
):
    """
    Poll a folder for changes and call on_change(added, removed, modified).
    Lightweight alternative to watchdog for cross-platform LAN sync.
    """
    log.info(f"[watch] Watching: {folder_path}")
    snapshot: Dict[str, float] = {}

    def _scan() -> Dict[str, float]:
        result = {}
        if not folder_path.exists():
            return result
        for p in folder_path.rglob("*"):
            if p.is_file() and not p.name.startswith("."):
                try:
                    result[str(p)] = p.stat().st_mtime
                except OSError:
                    pass
        return result

    loop = asyncio.get_event_loop()
    snapshot = await loop.run_in_executor(None, _scan)

    while True:
        await asyncio.sleep(poll_interval)
        try:
            current = await loop.run_in_executor(None, _scan)
            added = [p for p in current if p not in snapshot]
            removed = [p for p in snapshot if p not in current]
            modified = [p for p in current if p in snapshot and current[p] != snapshot[p]]
            if added or removed or modified:
                await on_change(added=added, removed=removed, modified=modified)
            snapshot = current
        except Exception as e:
            log.error(f"[watch] Error: {e}")
