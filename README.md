# LANShare — LAN File Transfer System

A production-grade, zero-internet local network file transfer system. Share files between Windows, macOS, Linux, Android, and iOS over Wi-Fi with no cloud, no accounts, no configuration.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    LANShare Server (Python)                  │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐ │
│  │  FastAPI     │  │  WebSocket   │  │  UDP Discovery    │ │
│  │  REST API    │  │  Hub (WS)    │  │  Beacon :7071     │ │
│  │  :7070/api   │  │  /ws/{id}    │  │  (auto-find)      │ │
│  └──────────────┘  └──────────────┘  └───────────────────┘ │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │             Async Task Queue (4 workers)              │   │
│  │  • Folder indexing   • Chunk GC   • File hashing     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │ HTTP/WS            │ HTTP/WS            │ HTTP/WS
    ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
    │ Windows │          │ macOS   │          │ Android │
    │ Browser │          │ Browser │          │ Browser │
    └─────────┘          └─────────┘          └─────────┘
```

### Component Breakdown

| Component | Technology | Role |
|-----------|-----------|------|
| HTTP API | FastAPI + AsyncIO | Upload init, chunk upload, download, file listing |
| Real-time hub | FastAPI WebSocket | Device presence, transfer signalling, progress relay |
| Background workers | AsyncIO TaskQueue | File indexing, chunk GC, hash computation, folder watch |
| Discovery | UDP broadcast | Zero-config device discovery on LAN (no manual IP) |
| UI | Vanilla HTML/JS | Cross-platform browser client (no install needed) |
| CLI | Python + aiohttp | Terminal-based send/receive for headless devices |

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the server

```bash
python app.py
```

Or with a custom port:
```bash
PORT=8080 python app.py
```

Output:
```
═══════════════════════════════════════════════════════
  🚀  LANShare  —  LAN File Transfer System
═══════════════════════════════════════════════════════
  HTTP  →  http://0.0.0.0:7070
  LAN   →  http://192.168.1.42:7070      ← share this
  UDP   →  discovery on port 7071
═══════════════════════════════════════════════════════
```

### 3. Connect devices

Open `http://<server-ip>:7070` in any browser on the same Wi-Fi network.

- **No app install** — works in Chrome, Safari, Firefox, Edge
- **Mobile** — open the URL on Android/iOS browser
- **Auto-discovery** — devices running the Python server appear instantly

---

## File Transfer Flow

```
Sender                Server              Receiver
  │                     │                     │
  ├──[WS] transfer_request──────────────────►│
  │                     │◄──[WS] transfer_accept
  │◄──[WS] transfer_accepted                 │
  │                     │                     │
  ├──[HTTP POST] /api/upload/init────────────►│
  │◄──────────────────{upload_id}             │
  │                     │                     │
  ├──[HTTP POST x N] /upload/chunk (parallel)─►
  │                     │──[WS] progress────►│
  │                     │                     │
  ├──[HTTP POST] /upload/finalize────────────►│
  │                     │──[WS] file_received►│
  │◄──[WS] transfer_complete                  │
```

### Key Properties

- **Chunked**: 512 KB chunks, resumable, parallel upload (4 concurrent)
- **Streaming download**: Range-request support, no memory buffering
- **Integrity**: Optional SHA-256 per-chunk and whole-file verification
- **Many-to-many**: One sender → multiple receivers simultaneously
- **No size limit**: Handles files of any size

---

## API Reference

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/devices` | List connected devices |
| GET | `/api/info` | Server info (name, IP, platform) |
| GET | `/api/files` | List received files |
| DELETE | `/api/files/{name}` | Delete a file |
| GET | `/api/download/{name}` | Download file (streaming, range support) |
| POST | `/api/upload/init` | Initialize chunked upload session |
| POST | `/api/upload/chunk` | Upload a chunk |
| POST | `/api/upload/finalize` | Reassemble and verify |
| POST | `/api/sync/diff` | Compute folder sync diff |
| GET | `/api/transfers/{id}` | Get transfer session status |

### WebSocket Events

**Client → Server:**
| Event | Description |
|-------|-------------|
| `register` | Announce device info |
| `heartbeat` | Keep-alive (every 5s) |
| `transfer_request` | Request to send files to receiver(s) |
| `transfer_accept` | Accept an incoming transfer |
| `transfer_reject` | Reject an incoming transfer |
| `transfer_progress` | Report upload progress |
| `transfer_complete` | Signal transfer done |
| `sync_join` | Join a sync room |
| `sync_broadcast` | Broadcast sync event to room peers |
| `clipboard_share` | Share clipboard text to peers |

**Server → Client:**
| Event | Description |
|-------|-------------|
| `device_list` | Full list of connected devices |
| `device_joined` | New device appeared |
| `device_left` | Device disconnected |
| `heartbeat_ack` | Heartbeat response |
| `transfer_invitation` | Incoming transfer request |
| `transfer_accepted` | Receiver accepted |
| `transfer_rejected` | Receiver rejected |
| `transfer_progress` | Upload progress update |
| `file_received` | File fully received and saved |
| `transfer_complete` | Transfer session complete |
| `sync_peer_joined` | Peer joined sync room |
| `clipboard_received` | Incoming clipboard |

---

## CLI Usage

```bash
# Discover servers on LAN
python cli.py discover

# Send a file
python cli.py send ./video.mp4 http://192.168.1.42:7070

# List files on server
python cli.py ls http://192.168.1.42:7070

# Download a file
python cli.py get http://192.168.1.42:7070 video.mp4
```

---

## Folder Sync

The sync system uses a **diff-based approach**:

1. Client joins a named sync room via WebSocket
2. Client sends its local file tree to `/api/sync/diff`
3. Server responds with: files to upload, files to download, conflicts
4. Client acts on the diff (download new, upload missing)
5. Folder changes are broadcast to all room peers via WebSocket

---

## Background Workers

The `TaskQueue` is a drop-in asyncio alternative to Celery for LAN use (no Redis/RabbitMQ broker needed):

```python
from workers import task_queue, index_folder, compute_file_hash

# Index a folder in background
await task_queue.submit(index_folder, Path("/home/user/docs"), result_store)

# Hash a file in background (non-blocking)
await task_queue.submit(compute_file_hash, Path("/uploads/big.zip"), hash_store)
```

To use real Celery for multi-machine deployments, replace `task_queue.submit()` calls with `.delay()`:
```bash
pip install celery redis
celery -A workers worker --loglevel=info
```

---

## Production Deployment

### Run as systemd service (Linux)

```ini
# /etc/systemd/system/lanshare.service
[Unit]
Description=LANShare File Transfer Server
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/lanshare
ExecStart=/usr/bin/python3 app.py
Restart=always
Environment=PORT=7070

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable lanshare
sudo systemctl start lanshare
```

### Nginx reverse proxy (optional HTTPS)

```nginx
server {
    listen 443 ssl;
    server_name lanshare.local;

    location / {
        proxy_pass http://127.0.0.1:7070;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 3600;
        client_max_body_size 0;  # no upload size limit
    }
}
```

---

## Security Notes

- **LAN only** — server binds to `0.0.0.0` but has no authentication by default
- For sensitive environments, add API key middleware to `main.py`
- Files are stored in `./uploads/` — set appropriate OS permissions
- Path traversal is prevented: filenames are sanitized with `Path(name).name`

---

## Project Structure

```
lanshare/
├── app.py              # Integrated startup (FastAPI + lifespan)
├── cli.py              # Command-line client
├── requirements.txt
├── server/
│   ├── main.py         # FastAPI app, all REST + WS endpoints
│   ├── workers.py      # Async task queue, file indexing, GC, folder watch
│   └── discovery.py    # UDP broadcast discovery service
├── static/
│   └── index.html      # Full web UI (HTML/CSS/JS, zero dependencies)
└── uploads/            # Created automatically on first run
    └── .tmp/           # Chunk staging area (auto-cleaned)
```
