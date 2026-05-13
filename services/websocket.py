import logging
from typing import Any, Dict, List

from fastapi import WebSocket

from config import WS_MAX_CONNECTIONS

logger = logging.getLogger(__name__)


class ConnectionManager:
    """WebSocket 연결 관리자.

    - 연결 상한(WS_MAX_CONNECTIONS) 초과 시 새 연결 거절.
    - broadcast 실패한 연결은 자동 제거.
    """

    def __init__(self):
        self._connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket) -> bool:
        """연결 수락. 상한 도달 시 1013(Try Again Later) 으로 거절하고 False 반환."""
        if len(self._connections) >= WS_MAX_CONNECTIONS:
            logger.warning(
                "WebSocket 연결 거절 — 상한 도달 (%d/%d)",
                len(self._connections), WS_MAX_CONNECTIONS,
            )
            await ws.close(code=1013, reason="server busy")
            return False
        await ws.accept()
        self._connections.append(ws)
        logger.info(
            "WebSocket 연결 수락 — 현재 %d/%d",
            len(self._connections), WS_MAX_CONNECTIONS,
        )
        return True

    def disconnect(self, ws: WebSocket):
        if ws in self._connections:
            self._connections.remove(ws)
            logger.info(
                "WebSocket 연결 종료 — 현재 %d/%d",
                len(self._connections), WS_MAX_CONNECTIONS,
            )

    async def broadcast(self, message: Dict[str, Any]):
        # broadcast 중 실패한 소켓은 한 번에 제거 (iteration 중 변경 회피)
        dead: list[WebSocket] = []
        for conn in list(self._connections):
            try:
                await conn.send_json(message)
            except Exception as e:
                logger.debug("WebSocket broadcast 실패 — 연결 제거: %s", e)
                dead.append(conn)
        for conn in dead:
            self.disconnect(conn)

    @property
    def active_count(self) -> int:
        return len(self._connections)


manager = ConnectionManager()

latest_cache: Dict[str, Any] = {
    "top_picks": [],
    "radar": [],
    "macro": {"marquee": [], "sidebar": []},
    "news_feed": [],
    "market_gauge": None,
    "vix": None,
    "updated_at": None,
    "quote_tick_at": None,  # yfinance 분봉 시세 마지막 반영 시각
}
