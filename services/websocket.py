from typing import List, Dict, Any
from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self._connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self._connections:
            self._connections.remove(ws)

    async def broadcast(self, message: Dict[str, Any]):
        for conn in list(self._connections):
            try:
                await conn.send_json(message)
            except Exception:
                self._connections.remove(conn)


manager = ConnectionManager()

latest_cache: Dict[str, Any] = {
    "top_picks": [],
    "radar": [],
    "macro": {"marquee": [], "sidebar": []},
    "news_feed": [],
    "updated_at": None,
}
