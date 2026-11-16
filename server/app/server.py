import logging
import os
import signal
import threading
import uuid

import zmq

from app.cluster import BerkeleySync, ElectionManager, Replicator
from app.core import ServerConfig, ServerState, envelope, pack_message, receive
from app.network import PeerServer, Publisher, ReferenceClient
from app.storage import Storage

LOG = logging.getLogger("server")

# region CLI REQ

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
                    "action": action
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

# endregion CLI REQ

#------------------------------------------------------------

# region STARTUP

def _worker(state: ServerState, context: zmq.Context, handlers: Handlers) -> None:
    worker = context.socket(zmq.REP)
    worker.setsockopt(zmq.LINGER, 0)
    worker.connect(state.config.broker_endpoint)
    poller = zmq.Poller()
    poller.register(worker, zmq.POLLIN)
    LOG.info("Connected to broker: %s", state.config.broker_endpoint)
    try:
        while not state.stop.is_set():
            if worker not in dict(poller.poll(400)):
                continue
            try:
                request = receive(state.clock, worker.recv())
                LOG.info("REQ received: %s", request)
                response = handlers.handle(request)
                action = str(request.get("action", "UNKNOWN"))
            except (ValueError, KeyError, TypeError) as exc:
                response = {"status": "error", "error": str(exc)}
                action = "ERROR"
            outgoing = envelope(state.clock, action=action, **response)
            worker.send(pack_message(outgoing))
            LOG.info("REP sent: %s", outgoing)
    finally:
        worker.close(linger=0)

def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    )
    config = ServerConfig.from_env()
    state = ServerState(config)
    storage = Storage(config.database_path, config.name)
    context = zmq.Context()
    publisher = Publisher(state, context)
    handlers = Handlers(state, storage, publisher)
    components = [
        publisher,
        PeerServer(state, storage, context),
        ReferenceClient(state, context),
        ElectionManager(state, context, publisher),
        BerkeleySync(state, context),
        Replicator(state, storage, context),
    ]

    def stop(*_args) -> None:
        LOG.info("Stopping %s", config.name)
        state.stop.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    for component in components:
        component.start()
    worker = threading.Thread(target=_worker, args=(state, context, handlers), name="client-worker", daemon=True)
    worker.start()
    LOG.info("Server %s ready; database=%s", config.name, config.database_path)
    try:
        while not state.stop.wait(1.0):
            pass
    finally:
        state.stop.set()
        worker.join(timeout=2)
        for component in components:
            for attr in ("thread", "subscriber_thread"):
                thread = getattr(component, attr, None)
                if thread is not None:
                    thread.join(timeout=2)
        context.term()

# endregion STARTUP

#------------------------------------------------------------

if __name__ == "__main__":
    main()