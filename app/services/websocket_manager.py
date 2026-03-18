from __future__ import annotations

from collections import defaultdict
from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: dict[str, list[WebSocket]] = defaultdict(list)

    async def connect(self, job_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections[job_id].append(websocket)

    def disconnect(self, job_id: str, websocket: WebSocket) -> None:
        if job_id in self.connections and websocket in self.connections[job_id]:
            self.connections[job_id].remove(websocket)
        if job_id in self.connections and not self.connections[job_id]:
            del self.connections[job_id]

    async def broadcast(self, job_id: str, message: dict) -> None:
        for websocket in list(self.connections.get(job_id, [])):
            await websocket.send_json(message)


manager = ConnectionManager()
