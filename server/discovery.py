"""
LANShare UDP Discovery Service
Broadcasts device presence over LAN so peers can find the server without manual IP entry.
"""

import asyncio
import json
import logging
import socket
import time
from typing import Callable, Dict, List, Optional

log = logging.getLogger("lanshare.discovery")

DISCOVERY_PORT = 7071
BROADCAST_INTERVAL = 3  # seconds
MAGIC = "LANSHARE_DISCOVER_V1"


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_broadcast_ip() -> str:
    """Compute broadcast address for the local subnet."""
    ip = get_local_ip()
    parts = ip.split(".")
    parts[-1] = "255"
    return ".".join(parts)


class DiscoveryServer:
    """
    UDP server that:
    1. Broadcasts server presence periodically
    2. Responds to discovery queries from clients
    """

    def __init__(self, http_port: int = 7070, device_name: str = "LANShare"):
        self.http_port = http_port
        self.device_name = device_name
        self.local_ip = get_local_ip()
        self._running = False
        self._transport: Optional[asyncio.BaseTransport] = None

    def _make_beacon(self) -> bytes:
        payload = json.dumps({
            "magic": MAGIC,
            "type": "beacon",
            "name": self.device_name,
            "ip": self.local_ip,
            "port": self.http_port,
            "ts": time.time(),
        }).encode()
        return payload

    async def start(self):
        self._running = True
        loop = asyncio.get_event_loop()

        # Create UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)
        try:
            sock.bind(("", DISCOVERY_PORT))
        except OSError as e:
            log.warning(f"Could not bind discovery port {DISCOVERY_PORT}: {e}")
            return

        class UDPProtocol(asyncio.DatagramProtocol):
            def __init__(self, server: "DiscoveryServer"):
                self.server = server

            def datagram_received(self, data: bytes, addr):
                try:
                    msg = json.loads(data.decode())
                    if msg.get("magic") == MAGIC and msg.get("type") == "query":
                        # Respond directly to querying client
                        response = self.server._make_beacon()
                        if self.transport:
                            self.transport.sendto(response, addr)
                except Exception:
                    pass

            def connection_made(self, transport):
                self.transport = transport

            def error_received(self, exc):
                log.debug(f"UDP error: {exc}")

        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPProtocol(self),
            sock=sock,
        )
        self._transport = transport

        log.info(f"Discovery server listening on UDP:{DISCOVERY_PORT}")

        # Broadcast loop
        broadcast_addr = get_broadcast_ip()
        while self._running:
            try:
                beacon = self._make_beacon()
                transport.sendto(beacon, (broadcast_addr, DISCOVERY_PORT))
                log.debug(f"Beacon → {broadcast_addr}:{DISCOVERY_PORT}")
            except Exception as e:
                log.debug(f"Broadcast error: {e}")
            await asyncio.sleep(BROADCAST_INTERVAL)

    def stop(self):
        self._running = False
        if self._transport:
            self._transport.close()


class DiscoveryClient:
    """
    UDP client that listens for beacons and actively queries for servers.
    Used by CLI tools / tests — the web UI uses HTTP polling instead.
    """

    def __init__(self, on_discovered: Callable[[dict], None]):
        self.on_discovered = on_discovered
        self._servers: Dict[str, dict] = {}
        self._running = False

    async def scan(self, timeout: float = 5.0) -> List[dict]:
        """One-shot scan: send query, collect responses for `timeout` seconds."""
        discovered = []
        loop = asyncio.get_event_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(0.5)
        sock.bind(("", 0))  # random port for client

        query = json.dumps({"magic": MAGIC, "type": "query"}).encode()
        broadcast_addr = get_broadcast_ip()

        try:
            sock.sendto(query, (broadcast_addr, DISCOVERY_PORT))
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
                            discovered.append(msg)
                            self.on_discovered(msg)
                except socket.timeout:
                    pass
        finally:
            sock.close()

        return discovered


# ─── Startup hook for FastAPI ─────────────────────────────────────────────────
_discovery_server: Optional[DiscoveryServer] = None


async def start_discovery(http_port: int = 7070):
    global _discovery_server
    import socket as _socket
    name = _socket.gethostname()
    _discovery_server = DiscoveryServer(http_port=http_port, device_name=name)
    asyncio.create_task(_discovery_server.start())
    log.info("Discovery service started")


async def stop_discovery():
    if _discovery_server:
        _discovery_server.stop()
