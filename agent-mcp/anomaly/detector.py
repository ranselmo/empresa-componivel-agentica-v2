"""F5.2 — Anomaly detection using IsolationForest."""
import os
import httpx
import numpy as np
from sklearn.ensemble import IsolationForest

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")

METRICS = [
    "shard_router_requests_total",
    "data_sync_lag_seconds",
    "saga_duration_seconds",
    "circuit_breaker_state",
]


async def _query(metric: str) -> float:
    async with httpx.AsyncClient(timeout=5.0) as http:
        try:
            r = await http.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": metric},
            )
            results = r.json().get("data", {}).get("result", [])
            if results:
                return float(results[0]["value"][1])
        except Exception:
            pass
    return 0.0


class AnomalyDetector:
    def __init__(self, contamination: float = 0.1):
        self._model = IsolationForest(contamination=contamination, random_state=42)
        self._history: list[list[float]] = []
        self._trained = False

    async def collect(self) -> list[float]:
        values = []
        for m in METRICS:
            values.append(await _query(m))
        return values

    def fit(self, sample: list[float]) -> None:
        self._history.append(sample)
        if len(self._history) >= 10:
            X = np.array(self._history[-100:])
            self._model.fit(X)
            self._trained = True

    def predict(self, sample: list[float]) -> dict:
        if not self._trained:
            return {"anomaly": False, "score": None, "reason": "insufficient data"}
        X = np.array([sample])
        score = self._model.score_samples(X)[0]
        is_anomaly = self._model.predict(X)[0] == -1
        return {
            "anomaly": bool(is_anomaly),
            "score": float(score),
            "metrics": dict(zip(METRICS, sample)),
        }

    async def run_once(self) -> dict:
        sample = await self.collect()
        self.fit(sample)
        return self.predict(sample)


detector = AnomalyDetector()
