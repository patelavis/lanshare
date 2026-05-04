#!/usr/bin/env python3
"""
LANShare CLI — discover servers & send files from terminal
Usage:
  python cli.py discover              # find servers on LAN
  python cli.py send <file> <server>  # send file to server
  python cli.py ls <server>           # list files on server
  python cli.py get <server> <file>   # download file
"""

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("pip install aiohttp")
    sys.exit(1)

CHUNK_SIZE = 512 * 1024
DISCOVERY_PORT = 7071
MAGIC = "LANSHARE_DISCOVER_V1"


# ── Discovery ──────────────────────────────────────────────────
async def discover(timeout: float = 5.0):
    import socket, time
    servers = []
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(0.5)
    sock.bind(("", 0))

    # Get broadcast address
    local_ip = get_local_ip()
    parts = local_ip.split(".")
    parts[-1] = "255"
    broadcast = ".".join(parts)

    query = json.dumps({"magic": MAGIC, "type": "query"}).encode()
    sock.sendto(query, (broadcast, DISCOVERY_PORT))

    print(f"🔍 Scanning LAN ({broadcast})…")
    deadline = time.time() + timeout
    seen = set()

    while time.time() < deadline:
        try:
            data, addr = sock.recvfrom(1024)
            msg = json.loads(data.decode())
            if msg.get("magic") == MAGIC and msg.get("type") == "beacon":
                key = f"{msg['ip']}:{msg['port']}"
                if key not in seen:
                    seen.add(key)
                    servers.append(msg)
                    print(f"  ✓ {msg['name']} @ http://{msg['ip']}:{msg['port']}")
        except (socket.timeout, json.JSONDecodeError):
            pass

    sock.close()
    if not servers:
        print("  No servers found. Make sure LANShare server is running on the network.")
    return servers


def get_local_ip() -> str:
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ── Send ───────────────────────────────────────────────────────
async def send_file(server_url: str, filepath: str):
    path = Path(filepath)
    if not path.exists():
        print(f"Error: {filepath} not found")
        return

    total_size = path.stat().st_size
    total_chunks = max(1, -(-total_size // CHUNK_SIZE))  # ceiling div
    session_id = "cli_" + os.urandom(4).hex()
    sender_id = "cli_" + os.urandom(4).hex()
    receiver_id = "server"

    base = server_url.rstrip("/")
    print(f"📤 Sending: {path.name} ({fmt_size(total_size)}) → {base}")

    async with aiohttp.ClientSession() as session:
        # Init
        data = aiohttp.FormData()
        data.add_field("session_id", session_id)
        data.add_field("filename", path.name)
        data.add_field("total_size", str(total_size))
        data.add_field("total_chunks", str(total_chunks))
        data.add_field("sender_id", sender_id)
        data.add_field("receiver_id", receiver_id)

        async with session.post(f"{base}/api/upload/init", data=data) as r:
            resp = await r.json()
            upload_id = resp["upload_id"]

        # Chunks
        with open(path, "rb") as f:
            for i in range(total_chunks):
                chunk = f.read(CHUNK_SIZE)
                fd = aiohttp.FormData()
                fd.add_field("upload_id", upload_id)
                fd.add_field("chunk_index", str(i))
                fd.add_field("chunk_data", chunk, filename="chunk", content_type="application/octet-stream")
                async with session.post(f"{base}/api/upload/chunk", data=fd) as r:
                    await r.json()
                pct = int((i + 1) / total_chunks * 100)
                print(f"  [{pct:3d}%] chunk {i+1}/{total_chunks}", end="\r")

        print()

        # Finalize
        fd = aiohttp.FormData()
        fd.add_field("upload_id", upload_id)
        async with session.post(f"{base}/api/upload/finalize", data=fd) as r:
            result = await r.json()
            print(f"  ✓ Saved as: {result.get('filename')} ({fmt_size(result.get('size', 0))})")


# ── List ───────────────────────────────────────────────────────
async def list_files(server_url: str):
    base = server_url.rstrip("/")
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{base}/api/files") as r:
            d = await r.json()
    print(f"\n📂 Files on {base} ({d['count']}):\n")
    for f in d["files"]:
        print(f"  {f['name']:<40} {fmt_size(f['size']):>10}  {f['modified'][:19]}")
    print()


# ── Download ───────────────────────────────────────────────────
async def download_file(server_url: str, filename: str):
    base = server_url.rstrip("/")
    url = f"{base}/api/download/{filename}"
    out = Path(filename)
    print(f"📥 Downloading {filename} from {base}…")
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            total = int(r.headers.get("Content-Length", 0))
            done = 0
            with open(out, "wb") as f:
                async for chunk in r.content.iter_chunked(CHUNK_SIZE):
                    f.write(chunk)
                    done += len(chunk)
                    if total:
                        pct = int(done / total * 100)
                        print(f"  [{pct:3d}%] {fmt_size(done)}/{fmt_size(total)}", end="\r")
    print(f"\n  ✓ Saved: {out.absolute()}")


def fmt_size(b):
    for u in ["B", "KB", "MB", "GB"]:
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"


# ── Main ───────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="LANShare CLI")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("discover")

    s = sub.add_parser("send")
    s.add_argument("file")
    s.add_argument("server", help="http://IP:PORT")

    l = sub.add_parser("ls")
    l.add_argument("server")

    g = sub.add_parser("get")
    g.add_argument("server")
    g.add_argument("file")

    args = p.parse_args()

    if args.cmd == "discover":
        asyncio.run(discover())
    elif args.cmd == "send":
        asyncio.run(send_file(args.server, args.file))
    elif args.cmd == "ls":
        asyncio.run(list_files(args.server))
    elif args.cmd == "get":
        asyncio.run(download_file(args.server, args.file))
    else:
        p.print_help()


if __name__ == "__main__":
    main()
