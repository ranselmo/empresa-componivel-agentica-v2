"""F5.3 — Predictive scaling using EMA + slope forecast."""
import os
import httpx
from collections import deque

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
EMA_ALPHA = 0.3
HISTORY_SIZE = 30


class EMAPPredictor:
    """Exponential Moving Average predictor with slope-based forecast."""

    def __init__(self, cell: str, alpha: float = EMA_ALPHA):
        self._cell = cell
        self._alpha = alpha
        self._ema: float | None = None
        self._history: deque[float] = deque(maxlen=HISTORY_SIZE)

    async def fetch_rps(self) -> float:
        query = f'rate(shard_router_requests_total{{pbc="{self._cell}"}}[1m])'
        async with httpx.AsyncClient(timeout=5.0) as http:
            try:
                r = await http.get(
                    f"{PROMETHEUS_URL}/api/v1/query",
                    params={"query": query},
                )
                results = r.json().get("data", {}).get("result", [])
                if results:
                    return float(results[0]["value"][1])
            except Exception:
                pass
        return 0.0

    def update(self, rps: float) -> None:
        self._ema = rps if self._ema is None else self._alpha * rps + (1 - self._alpha) * self._ema
        self._history.append(self._ema)

    def forecast(self, steps_ahead: int = 5) -> float:
        if len(self._history) < 2:
            return self._ema or 0.0
        hist = list(self._history)
        slope = (hist[-1] - hist[0]) / len(hist)
        return max(0.0, self._ema + slope * steps_ahead)

    def recommended_replicas(self, rps_per_replica: float = 50.0) -> int:
        predicted = self.forecast()
        return max(1, int(predicted / rps_per_replica) + 1)

    async def predict(self) -> dict:
        rps = await self.fetch_rps()
        self.update(rps)
        predicted = self.forecast()
        return {
            "cell": self._cell,
            "current_rps": rps,
            "ema": self._ema,
            "predicted_rps_5m": predicted,
            "recommended_replicas": self.recommended_replicas(),
        }


predictors = {
    cell: EMAPPredictor(cell)
    for cell in ["pedidos", "estoque", "notificacoes"]
}
