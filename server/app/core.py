import os
import threading
import time
from dataclasses import dataclass
from typing import Any

import msgpack

# region CLOCK/PROTOCOL

class Clock:
    def __init__(self):
        self.lock = threading.RLock()
        self.lamport = 0
        self.offset_ms = 0.0

    def tick(self):
        with self.lock:
            self.lamport += 1
            return self.lamport

    def observe(self, received):
        with self.lock:
            self.lamport = max(self.lamport, received)
            return self.lamport

    def now_ms(self):
        with self.lock:
            return int(time.time() * 1000 + self.offset_ms)

    def adjust_to(self, target_ms):
        # Adjust a virtual clock instead of changing the container system clock.
        with self.lock:
            self.offset_ms = float(target_ms) - time.time() * 1000
            return round(self.offset_ms)

def pack_message(message: dict) -> bytes:
    return msgpack.packb(message, use_bin_type=True)

def unpack_message(payload: bytes) -> dict:
    message = msgpack.unpackb(payload, raw=False)
    if not isinstance(message, dict) or not {"timestamp", "lamport"} <= message.keys():
        raise ValueError("Invalid message envelope")
    return message

def envelope(clock: Clock, action: str, **fields: Any) -> dict:
    return {"action": action, **fields, "timestamp": clock.now_ms(), "lamport": clock.tick()}


def receive(clock: Clock, payload: bytes) -> dict:
    message = unpack_message(payload)
    clock.observe(int(message["lamport"]))
    return message

# endregion CLOCK/PROTOCOL

#------------------------------------------------------------

# region CONFIG

@dataclass(frozen=True)
class ServerConfig:
    name: str
    broker_endpoint: str
    publish_endpoint: str
    subscribe_endpoint: str
    reference_endpoint: str
    peer_bind_endpoint: str
    peer_endpoint: str
    database_path: str
    heartbeat_every_messages: int
    heartbeat_timeout_seconds: int
    clock_sync_every_messages: int
    replication_retry_seconds: int

    @classmethod
    def from_env(cls):
        name = os.getenv("SERVER_NAME", "server-1")
        return cls(
            name=name,
            broker_endpoint=os.getenv("BROKER_ENDPOINT", "tcp://broker:5556"),
            publish_endpoint=os.getenv("PUBSUB_PUBLISH_ENDPOINT", "tcp://pubsub-proxy:5557"),
            subscribe_endpoint=os.getenv("PUBSUB_SUBSCRIBE_ENDPOINT", "tcp://pubsub-proxy:5558"),
            reference_endpoint=os.getenv("REFERENCE_ENDPOINT", "tcp://reference:5559"),
            peer_bind_endpoint=os.getenv("PEER_BIND_ENDPOINT", "tcp://*:5560"),
            peer_endpoint=os.getenv("PEER_ENDPOINT", f"tcp://{name}:5560"),
            database_path=os.getenv("DATABASE_PATH", "/data/messages.db"),
            heartbeat_every_messages=int(os.getenv("HEARTBEAT_EVERY_MESSAGES", "10")),
            heartbeat_timeout_seconds=int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "60")),
            clock_sync_every_messages=int(os.getenv("CLOCK_SYNC_EVERY_MESSAGES", "15")),
            replication_retry_seconds=int(os.getenv("REPLICATION_RETRY_SECONDS", "3")),
        )

class ServerState:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.clock = Clock()
        self.lock = threading.RLock()
        self.rank = None
        self.peers = {}
        self.coordinator = None
        self.client_messages = 0
        self.election_requested = threading.Event()
        self.stop = threading.Event()

    def set_directory(self, rank, servers):
        with self.lock:
            if rank is not None:
                self.rank = int(rank)
            self.peers = {server["name"]: dict(server) for server in servers}
            if self.coordinator not in self.peers:
                self.coordinator = None

    def directory(self):
        with self.lock:
            return self.rank, dict(self.peers), self.coordinator

    def set_coordinator(self, name, rank):
        with self.lock:
            peer = self.peers.get(name)
            if not peer or peer["rank"] != rank:
                return False
            if any(item["rank"] > rank for item in self.peers.values()):
                return False
            self.coordinator = name
            return True

    def is_coordinator(self):
        with self.lock:
            return self.coordinator == self.config.name

    def record_client_request(self):
        with self.lock:
            self.client_messages += 1

    def requests_processed(self):
        with self.lock:
            return self.client_messages
        
# endregion CONFIG