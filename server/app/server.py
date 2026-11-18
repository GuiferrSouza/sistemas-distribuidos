import logging
import os
import signal
import threading
import time
import uuid

import zmq

from app.cluster import BerkeleySync, ElectionManager, Replicator
from app.core import ServerConfig, ServerState, envelope, pack_message, receive
from app.network import PeerClient, PeerServer, Publisher, ReferenceClient
from app.storage import Storage

LOG = logging.getLogger("server")

#------------------------------------------------------------

class Handlers:
    def __init__(self, state: ServerState, storage: Storage, publisher: Publisher) -> None:
        self.state = state
        self.storage = storage
        self.publisher = publisher

    @staticmethod
    def required(request: dict, name: str, max_length: int = 256) -> str:
        value = request.get(name)
        if not isinstance(value, str) or not value.strip() or len(value) > max_length:
            raise ValueError(f"{name} required (1 to {max_length} characters)")
        return value.strip()

    def handle(self, request: dict) -> dict:
        action = str(request.get("action", "")).upper()
        request_id = str(request.get("request_id") or uuid.uuid4())
        timestamp = int(request["timestamp"])
        username = str(request.get("username", ""))
        response: dict
        try:
            if action == "LOGIN":
                self.required(request, "username")
                self.storage.create_event(
                    "login",
                    {
                        "username": username,
                        "login_at": timestamp,
                        "lamport": int(request["lamport"])
                    },
                    timestamp,
                    event_id=f"login:{request_id}",
                )
                response = {"status": "ok", "username": username}

            elif action == "CREATE_CHANNEL":
                channel = self.required(request, "channel", 80)
                self.required(request, "username")
                if self.storage.channel_exists(channel):
                    response = {"status": "exists", "channel": channel}
                else:
                    self.storage.create_event(
                        "channel",
                        {
                            "name": channel,
                            "created_by": username,
                            "created_at": timestamp
                        },
                        timestamp,
                        event_id=f"channel:{request_id}",
                    )
                    response = {"status": "ok", "channel": channel}

            elif action == "LIST_CHANNELS":
                response = {"status": "ok", "channels": self.storage.get_channels()}

            elif action == "PUBLISH":
                self.required(request, "username")
                channel = self.required(request, "channel", 80)
                content = self.required(request, "content", 4096)
                if not self.storage.channel_exists(channel):
                    response = {
                        "status": "error",
                        "error": "channel_not_found",
                        "channel": channel
                    }
                else:
                    message_id = f"message:{request_id}"
                    created = self.storage.create_event(
                        "message",
                        {
                            "channel": channel,
                            "sender": username,
                            "content": content,
                            "sent_at": timestamp,
                            "lamport": int(request["lamport"])
                        },
                        timestamp,
                        event_id=message_id,
                    )
                    if created:
                        self.publisher.publish(channel, {
                            "kind": "CHANNEL_MESSAGE",
                            "message_id": message_id,
                            "channel": channel,
                            "sender": username,
                            "content": content,
                            "sent_at": timestamp,
                        })
                    response = {
                        "status": "ok",
                        "message_id": message_id,
                        "channel": channel
                    }

            elif action == "GET_HISTORY":
                channel = request.get("channel")
                if channel is not None:
                    channel = self.required(request, "channel", 80)
                response = {
                    "status": "ok",
                    "messages": self.storage.get_history(channel, request.get("limit", 50))
                }

            else:
                response = {
                    "status": "error",
                    "error": "unknown_action",
                    "requested_action": action
                }
        except (TypeError, ValueError, KeyError) as exc:
            response = {"status": "error", "error": str(exc)}

        self.storage.create_event("interaction", {
            "request_id": request_id,
            "username": username,
            "action": action,
            "request": request,
            "response_status": response["status"],
        }, timestamp, event_id=f"interaction:{request_id}")
        self.state.record_client_request()
        LOG.info("Action %s | result %s | bot %s", action, response["status"], username)
        return {"request_id": request_id, **response}

#------------------------------------------------------------

WORKER_HEARTBEAT_SECONDS = 1.0

def _process(state: ServerState, handlers: Handlers, body: bytes) -> bytes:
    """Decode one client request, run it and return the packed reply. Never raises."""
    request_id = None
    try:
        request = receive(state.clock, body)
        request_id = request.get("request_id")
        LOG.info("REQ received: %s", request)
        response = handlers.handle(request)
        action = str(request.get("action", "UNKNOWN"))
    except Exception as exc:  # noqa: BLE001 - a bad request or a storage error must not kill the worker
        LOG.warning("Request failed: %s", exc)
        response = {"status": "error", "error": str(exc)}
        if request_id is not None:
            response["request_id"] = request_id
        action = "ERROR"
    outgoing = envelope(state.clock, action=action, **response)
    LOG.info("REP sent: %s", outgoing)
    return pack_message(outgoing)

def _worker(state: ServerState, context: zmq.Context, handlers: Handlers) -> None:
    """Worker side of the broker protocol (DEALER <-> ROUTER).

    READY / HEARTBEAT tell the broker this server is alive, so it only routes requests to
    servers that are up; REPLY answers a REQUEST; DISCONNECT is sent on a clean shutdown.
    """
    worker = context.socket(zmq.DEALER)
    worker.setsockopt(zmq.LINGER, 0)
    worker.setsockopt(zmq.IDENTITY, state.config.name.encode("utf-8"))
    worker.setsockopt(zmq.IMMEDIATE, 1)  # never queue messages for a broker we are not connected to
    worker.connect(state.config.broker_endpoint)
    poller = zmq.Poller()
    poller.register(worker, zmq.POLLIN)
    LOG.info("Connected to broker: %s", state.config.broker_endpoint)

    def send(frames: list[bytes]) -> bool:
        try:
            worker.send_multipart(frames, flags=zmq.NOBLOCK)
            return True
        except zmq.Again:
            return False

    announced = False
    last_sent = 0.0
    try:
        while not state.stop.is_set():
            if worker in dict(poller.poll(200)):
                frames = worker.recv_multipart()
                if len(frames) == 5 and frames[0] == b"REQUEST" and frames[3] == b"":
                    _, job_id, client_id, _, body = frames
                    reply = _process(state, handlers, body)
                    if send([b"REPLY", job_id, client_id, b"", reply]):
                        last_sent = time.monotonic()
                    else:
                        LOG.warning("Broker unavailable; reply dropped")
                else:
                    LOG.warning("Unexpected frames from broker: %s", len(frames))
            now = time.monotonic()
            if now - last_sent >= WORKER_HEARTBEAT_SECONDS:
                # READY announces a new worker; after that, HEARTBEAT. A broker that restarted
                # registers unknown workers from their next HEARTBEAT.
                if send([b"HEARTBEAT" if announced else b"READY"]):
                    announced = True
                    last_sent = now
    finally:
        if announced:
            send([b"DISCONNECT"])
        worker.close(linger=500)  # give DISCONNECT a moment to leave

def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    )
    config = ServerConfig.from_env()
    state = ServerState(config)
    storage = Storage(config.database_path, config.name)
    # One zmq.Context (= one I/O thread) per component. libzmq resolves host names in the
    # I/O thread, so an unresolvable host (a stopped peer container) must never be able to
    # freeze the sockets of an unrelated component such as the broker worker.
    contexts: list[zmq.Context] = []

    def new_context() -> zmq.Context:
        contexts.append(zmq.Context())
        return contexts[-1]

    peer_client = PeerClient(state)
    publisher = Publisher(state, new_context())
    handlers = Handlers(state, storage, publisher)
    components = [
        publisher,
        PeerServer(state, storage, new_context()),
        ReferenceClient(state, new_context()),
        ElectionManager(state, new_context(), publisher, peer_client),
        BerkeleySync(state, peer_client),
        Replicator(state, storage, peer_client),
    ]

    def stop(*_args) -> None:
        LOG.info("Stopping %s", config.name)
        state.stop.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    for component in components:
        component.start()
    worker = threading.Thread(target=_worker, args=(state, new_context(), handlers), name="client-worker", daemon=True)
    worker.start()
    LOG.info("Server %s ready; database=%s", config.name, config.database_path)
    try:
        while not state.stop.wait(1.0):
            pass
    finally:
        state.stop.set()
        threads = [worker]
        for component in components:
            for attr in ("thread", "subscriber_thread"):
                thread = getattr(component, attr, None)
                if thread is not None:
                    threads.append(thread)
        deadline = time.monotonic() + 4.0  # docker stop waits 10 s before SIGKILL
        for thread in threads:
            thread.join(timeout=max(0.1, deadline - time.monotonic()))
        if not any(thread.is_alive() for thread in threads):
            for context in contexts:
                context.term()

#------------------------------------------------------------

if __name__ == "__main__":
    main()