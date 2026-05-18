import asyncio
import json
import logging
from typing import Any, Dict, List

from fastapi import WebSocket

from config import WS_BROADCAST_SEND_TIMEOUT_SEC, WS_MAX_CONNECTIONS

logger = logging.getLogger(__name__)

# orjson 미설치 환경 폴백 — 모든 broadcast 페이로드를 stdlib json 으로 직렬화.
# 브라우저 클라이언트가 JSON.parse(event.data) 하도록 text frame 으로 송신해야 하므로
# bytes → str 변환을 거친다. orjson 은 utf-8 bytes 를 반환하므로 decode 비용은 미미.
try:
    import orjson  # type: ignore

    def _dumps_str(msg: Any) -> str:
        return orjson.dumps(msg).decode("utf-8")
except ImportError:  # pragma: no cover
    def _dumps_str(msg: Any) -> str:
        return json.dumps(msg, ensure_ascii=False)


class ConnectionManager:
    """WebSocket 연결 관리자.

    - 연결 상한(WS_MAX_CONNECTIONS) 초과 시 새 연결 거절.
    - broadcast 실패한 연결은 자동 제거.
    - broadcast 가속:
        · 메시지를 1회만 직렬화하고 모든 클라이언트에 동일 bytes 를 전송.
          기존 send_json 은 클라이언트마다 dict → bytes 직렬화를 반복해
          200 연결 × 3ms = 600ms 가 누적됐다.
        · 직렬 for-loop → asyncio.gather 로 병렬 전송. 한 클라이언트의
          느린 네트워크가 다른 클라이언트의 전송을 막지 않게 한다.
        · 각 send 에 WS_BROADCAST_SEND_TIMEOUT_SEC 타임아웃을 둬 죽은
          연결이 broadcast 사이클 전체를 지연시키지 않게 한다.
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
        """모든 연결에 동일 메시지를 병렬 전송한다.

        성능 핫 패스 — 메시지를 1회만 직렬화한 뒤 send_text 로 모든 클라이언트에
        병렬 발사한다. 200 연결 기준 직렬 send_json 대비 수백 배 빠르다.
        브라우저 호환을 위해 text frame 으로 송신(JSON.parse 그대로 사용 가능).
        """
        if not self._connections:
            return

        try:
            payload = _dumps_str(message)
        except (TypeError, ValueError) as e:
            logger.warning("broadcast 직렬화 실패 — 메시지 폐기: %s", e)
            return

        connections = list(self._connections)
        results = await asyncio.gather(
            *(self._safe_send(conn, payload) for conn in connections),
            return_exceptions=False,
        )
        # 송신 실패한 연결만 일괄 제거 — iteration 중 변경 회피
        for conn, ok in zip(connections, results):
            if not ok:
                self.disconnect(conn)

    async def _safe_send(self, conn: WebSocket, payload: str) -> bool:
        """단일 연결에 text 전송 — 예외/타임아웃은 False 반환."""
        try:
            await asyncio.wait_for(
                conn.send_text(payload), timeout=WS_BROADCAST_SEND_TIMEOUT_SEC,
            )
            return True
        except asyncio.TimeoutError:
            logger.debug("WebSocket broadcast 타임아웃 — 연결 제거")
            return False
        except Exception as e:
            logger.debug("WebSocket broadcast 실패 — 연결 제거: %s", e)
            return False

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
