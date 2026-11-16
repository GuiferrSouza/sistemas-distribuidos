import logging
import queue
import threading
import time

import zmq

from app.core import ServerState, envelope, pack_message, receive
from app.storage import Storage

LOG = logging.getLogger("network")

# region PUB/COMMUNICATION

class Publisher:
    def __init__(self, state: ServerState, context: zmq.Context) -> None:
        self.state = state
        self.context = context
        self.pending: queue.Queue[tuple[str, dict]] = queue.Queue()
        self.thread = threading.Thread(target=self._run, name="publisher", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def publish(self, topic: str, data: dict) -> None:
        self.pending.put((topic, data))

    def _run(self) -> None:
        socket = self.context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self.state.config.publish_endpoint)
        try:
            while not self.state.stop.is_set():
                try:
                    topic, data = self.pending.get(timeout=0.5)
                except queue.Empty:
                    continue
                outgoing = envelope(self.state.clock, action="PUBLICATION", **data)
                socket.send_multipart([topic.encode("utf-8"), pack_message(outgoing)])
                LOG.info("Published [%s] %s", topic, outgoing)
        finally:
            socket.close(linger=0)

def call_peer(state: ServerState, context: zmq.Context, endpoint: str,
              action: str, timeout_ms: int = 1500, **fields) -> dict:
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.connect(endpoint)
    try:
        outgoing = envelope(state.clock, action=action, **fields)
        LOG.debug("Sent to %s: %s", endpoint, outgoing)
        socket.send(pack_message(outgoing))
        incoming = receive(state.clock, socket.recv())
        LOG.debug("Received from %s: %s", endpoint, incoming)
        return incoming
    except (zmq.Again, zmq.ZMQError, ValueError) as exc:
        raise ConnectionError(f"Peer unavailable: {endpoint}") from exc
    finally:
        socket.close(linger=0)

class PeerServer:
    def __init__(self, state: ServerState, storage: Storage, context: zmq.Context) -> None:
        self.state = state
        self.storage = storage
        self.context = context
        self.thread = threading.Thread(target=self._run, name="peer-listener", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def handle(self, request: dict) -> dict:
        action = str(request.get("action", "")).upper()
        if action == "ELECTION":
            other_rank = int(request.get("rank", 0))
            rank, _, _ = self.state.directory()
            if rank is not None and rank > other_rank:
                self.state.election_requested.set()
                return {"status": "ok", "answer": "OK", "rank": rank}
            return {"status": "ok", "answer": "LOWER_RANK", "rank": rank}
        if action in ("TIME_QUERY", "TIME_SYNC"):
            if action == "TIME_SYNC" and not self.state.is_coordinator():
                return {"status": "error", "error": "not_coordinator"}
            return {"status": "ok", "time_ms": self.state.clock.now_ms()}
        if action == "TIME_SET":
            if request.get("coordinator") != self.state.directory()[2]:
                return {"status": "error", "error": "unknown_coordinator"}
            offset = self.state.clock.adjust_to(int(request["time_ms"]))
            return {"status": "ok", "offset_ms": offset}
        if action == "SYNC_PULL":
            since = max(0, int(request.get("since", 0)))
            events = self.storage.pull_events(since, int(request.get("limit", 100)))
            return {"status": "ok", "events": events}
        return {"status": "error", "error": "unknown_peer_action"}

    def _run(self) -> None:
        socket = self.context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(self.state.config.peer_bind_endpoint)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        LOG.info("Listening for peers on %s", self.state.config.peer_bind_endpoint)
        try:
            while not self.state.stop.is_set():
                if socket not in dict(poller.poll(400)):
                    continue
                try:
                    request = receive(self.state.clock, socket.recv())
                    LOG.debug("Received peer: %s", request)
                    response = self.handle(request)
                    action = request.get("action", "UNKNOWN")
                except (ValueError, TypeError, KeyError) as exc:
                    response = {"status": "error", "error": str(exc)}
                    action = "ERROR"
                outgoing = envelope(self.state.clock, action=action, **response)
                socket.send(pack_message(outgoing))
                LOG.debug("Sent peer: %s", outgoing)
        finally:
            socket.close(linger=0)

# endregion PUB/COMMUNICATION

#------------------------------------------------------------

# region REG/HEARTBEAT

class ReferenceClient:
    def __init__(self, state: ServerState, context: zmq.Context) -> None:
        self.state = state
        self.context = context
        self.thread = threading.Thread(target=self._run, name="reference-client", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _request(self, action: str, **data) -> dict:
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, 1500)
        socket.setsockopt(zmq.RCVTIMEO, 1500)
        socket.connect(self.state.config.reference_endpoint)
        try:
            outgoing = envelope(self.state.clock, action=action, **data)
            LOG.debug("Sent: %s", outgoing)
            socket.send(pack_message(outgoing))
            incoming = receive(self.state.clock, socket.recv())
            LOG.debug("Received: %s", incoming)
            return incoming
        except zmq.Again as exc:
            raise ConnectionError("Reference service unavailable") from exc
        finally:
            socket.close(linger=0)

    def _register(self) -> bool:
        response = self._request(
            "REGISTER", name=self.state.config.name, endpoint=self.state.config.peer_endpoint,
        )
        if response.get("status") != "ok":
            return False
        self.state.set_directory(response["rank"], response["servers"])
        LOG.info("Registered: rank=%s", response["rank"])
        self.state.election_requested.set()
        return True

    def _run(self) -> None:
        registered = False
        last_heartbeat = 0.0
        last_heartbeat_messages = 0
        last_list = 0.0
        while not self.state.stop.is_set():
            try:
                if not registered:
                    registered = self._register()
                    if not registered:
                        self.state.stop.wait(2)
                        continue
                    last_heartbeat = time.monotonic()
                    last_heartbeat_messages = self.state.requests_processed()
                    last_list = 0.0

                now = time.monotonic()
                count = self.state.requests_processed()
                if (count - last_heartbeat_messages >= self.state.config.heartbeat_every_messages
                        or now - last_heartbeat >= min(20, self.state.config.heartbeat_timeout_seconds / 3)):
                    answer = self._request("HEARTBEAT", name=self.state.config.name)
                    if answer.get("status") != "ok":
                        registered = False
                        continue
                    last_heartbeat = now
                    last_heartbeat_messages = count
                    LOG.info("Heartbeat sent (messages=%s)", count)

                if now - last_list >= 3.0:
                    answer = self._request("LIST")
                    if answer.get("status") == "ok":
                        self.state.set_directory(None, answer["servers"])
                    last_list = now
            except (ConnectionError, zmq.ZMQError, KeyError, ValueError, TypeError) as exc:
                LOG.warning("Reference unavailable: %s", exc)
                registered = False
            self.state.stop.wait(1.0)

# endregion REG/HEARTBEAT