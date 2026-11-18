import logging
import queue
import threading
import time

import zmq

from app.core import ServerState, envelope, pack_message, receive
from app.storage import Storage

LOG = logging.getLogger("network")

#------------------------------------------------------------

class Publisher:
    def __init__(self, state: ServerState, context: zmq.Context) -> None:
        self.state = state
        self.context = context
        self.pending: queue.Queue[tuple[str, dict, int]] = queue.Queue()
        self.thread = threading.Thread(target=self._run, name="publisher", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def publish(self, topic: str, data: dict, level: int = logging.INFO) -> None:
        self.pending.put((topic, data, level))

    def _run(self) -> None:
        socket = self.context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self.state.config.publish_endpoint)
        try:
            while not self.state.stop.is_set():
                try:
                    topic, data, level = self.pending.get(timeout=0.5)
                except queue.Empty:
                    continue
                outgoing = envelope(self.state.clock, action="PUBLICATION", **data)
                socket.send_multipart([topic.encode("utf-8"), pack_message(outgoing)])
                LOG.log(level, "Published [%s] %s", topic, outgoing)
        finally:
            socket.close(linger=0)

class PeerClient:
    """Request/reply calls to the other servers.

    libzmq resolves host names inside the I/O thread of the context that owns the socket.
    When a peer container is stopped its name stops resolving and the lookup can block that
    thread for seconds; every other socket sharing the context (broker worker, registry
    client, publisher...) would freeze with it. Therefore each peer gets its own
    zmq.Context (its own I/O thread), and no other component shares it.

    A peer that fails is also skipped for a short, growing delay so a dead server is not
    dialed continuously by the election, Berkeley and replication threads.
    """

    BACKOFF_BASE_SECONDS = 0.5
    BACKOFF_MAX_SECONDS = 2.0

    def __init__(self, state: ServerState) -> None:
        self.state = state
        self.lock = threading.Lock()
        self.contexts: dict[str, zmq.Context] = {}
        self.health: dict[str, dict] = {}  # peer name -> {"failures": int, "retry_at": float}

    def _context_for(self, name: str) -> zmq.Context:
        with self.lock:
            context = self.contexts.get(name)
            if context is None:
                context = self.contexts[name] = zmq.Context()
            return context

    def _mark_failure(self, name: str, exc: Exception) -> None:
        with self.lock:
            health = self.health.setdefault(name, {"failures": 0, "retry_at": 0.0})
            health["failures"] += 1
            delay = min(self.BACKOFF_MAX_SECONDS,
                        self.BACKOFF_BASE_SECONDS * 2 ** (health["failures"] - 1))
            health["retry_at"] = time.monotonic() + delay
            first_failure = health["failures"] == 1
        if first_failure:  # log the transition once instead of every retry
            LOG.warning("Peer %s unreachable (%s)", name, type(exc).__name__)

    def _mark_success(self, name: str) -> None:
        with self.lock:
            recovered = self.health.pop(name, None) is not None
        if recovered:
            LOG.info("Peer %s reachable again", name)

    def call(self, peer: dict, action: str, timeout_ms: int = 1500, **fields) -> dict:
        """Send one request to `peer` ({"name", "endpoint", ...}); raise ConnectionError on failure."""
        name, endpoint = peer["name"], peer["endpoint"]
        with self.lock:
            health = self.health.get(name)
            if health and time.monotonic() < health["retry_at"]:
                raise ConnectionError(f"Peer {name} is backing off")
        # A new REQ socket per call: a timed-out REQ socket cannot be reused safely.
        socket = self._context_for(name).socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        try:
            socket.connect(endpoint)
            outgoing = envelope(self.state.clock, action=action, **fields)
            LOG.debug("Sent to %s: %s", endpoint, outgoing)
            socket.send(pack_message(outgoing))
            incoming = receive(self.state.clock, socket.recv())
            LOG.debug("Received from %s: %s", endpoint, incoming)
        except Exception as exc:  # noqa: BLE001 - zmq, msgpack and envelope errors all mean "peer unusable"
            self._mark_failure(name, exc)
            raise ConnectionError(f"Peer unavailable: {endpoint}") from exc
        finally:
            socket.close(linger=0)
        self._mark_success(name)
        return incoming

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
        if action == "PING":
            return {"status": "ok", "name": self.state.config.name}
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
                # After recv() a reply is mandatory, and an unexpected error (for example a
                # locked SQLite database) must not kill this thread: peers would never get
                # another answer to ELECTION, PING or SYNC_PULL.
                action = "ERROR"
                try:
                    request = receive(self.state.clock, socket.recv())
                    LOG.debug("Received peer: %s", request)
                    action = request.get("action", "UNKNOWN")
                    response = self.handle(request)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Peer request failed: %s", exc)
                    response = {"status": "error", "error": str(exc)}
                    action = "ERROR"
                outgoing = envelope(self.state.clock, action=action, **response)
                socket.send(pack_message(outgoing))
                LOG.debug("Sent peer: %s", outgoing)
        finally:
            socket.close(linger=0)

#------------------------------------------------------------

class ReferenceClient:
    def __init__(self, state: ServerState, context: zmq.Context) -> None:
        self.state = state
        self.context = context
        self.thread = threading.Thread(target=self._run, name="reference-client", daemon=True)
        self.socket = None
        self.registered = False
        self.reachable: bool | None = None  # None until the first exchange

    def start(self) -> None:
        self.thread.start()

    def _reset_socket(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
        self.socket = None

    def _request(self, action: str, timeout_ms: int = 2500, **data) -> dict:
        # This socket belongs exclusively to the reference-client thread. After any error it is
        # discarded: a REQ socket that timed out is stuck waiting for a reply and cannot send again.
        if self.socket is None:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            self.socket.connect(self.state.config.reference_endpoint)
        self.socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
        self.socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        try:
            outgoing = envelope(self.state.clock, action=action, **data)
            LOG.debug("Sent: %s", outgoing)
            self.socket.send(pack_message(outgoing))
            incoming = receive(self.state.clock, self.socket.recv())
            LOG.debug("Received: %s", incoming)
        except (zmq.ZMQError, ValueError, TypeError) as exc:
            self._reset_socket()
            if self.reachable is not False:  # log only the transition, not every retry
                LOG.warning("Reference service unavailable (%s)", type(exc).__name__)
            self.reachable = False
            raise ConnectionError("Reference service unavailable") from exc
        if self.reachable is False:
            LOG.info("Reference service reachable again")
        self.reachable = True
        return incoming

    def _register(self) -> bool:
        response = self._request(
            "REGISTER", name=self.state.config.name, endpoint=self.state.config.peer_endpoint,
        )
        if response.get("status") != "ok":
            return False
        self.state.set_directory(response["rank"], response["servers"])
        LOG.info("Registered: rank=%s", response["rank"])
        return True

    def _unregister(self) -> None:
        """Tell the registry we are leaving so peers stop counting on us without waiting for a timeout."""
        if not self.registered:
            return
        try:
            self._request("UNREGISTER", timeout_ms=1000, name=self.state.config.name)
            LOG.info("Unregistered from reference service")
        except ConnectionError:
            pass  # the heartbeat timeout will remove us

    def _run(self) -> None:
        last_heartbeat = 0.0
        last_heartbeat_messages = 0
        last_list = 0.0
        interval = min(20, self.state.config.heartbeat_timeout_seconds / 3)
        try:
            while not self.state.stop.is_set():
                try:
                    if not self.registered:
                        self.registered = self._register()
                        if not self.registered:
                            self.state.stop.wait(2)
                            continue
                        last_heartbeat = time.monotonic()
                        last_heartbeat_messages = self.state.requests_processed()
                        last_list = 0.0

                    now = time.monotonic()
                    count = self.state.requests_processed()
                    if (count - last_heartbeat_messages >= self.state.config.heartbeat_every_messages
                            or now - last_heartbeat >= interval):
                        answer = self._request("HEARTBEAT", name=self.state.config.name)
                        if answer.get("status") != "ok":
                            self.registered = False  # expired at the registry: register again
                            continue
                        last_heartbeat = now
                        last_heartbeat_messages = count
                        LOG.info("Heartbeat sent (messages=%s)", count)

                    if now - last_list >= 2.0:
                        answer = self._request("LIST")
                        if answer.get("status") == "ok":
                            if not any(s["name"] == self.state.config.name for s in answer["servers"]):
                                self.registered = False  # the registry forgot us
                                continue
                            self.state.set_directory(None, answer["servers"])
                        last_list = now
                except ConnectionError as exc:
                    LOG.debug("Reference unavailable: %s", exc)
                    self.registered = False
                except Exception as exc:  # noqa: BLE001 - this thread must never die
                    LOG.warning("Reference client error: %s", exc)
                    self._reset_socket()
                    self.registered = False
                self.state.stop.wait(1.0)
        finally:
            self._unregister()
            self._reset_socket()
