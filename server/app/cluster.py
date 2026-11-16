import logging
import threading
import time

import zmq

from app.core import ServerState, receive
from app.network import Publisher, call_peer
from app.storage import Storage

LOG = logging.getLogger("cluster")

#------------------------------------------------------------

class ElectionManager:
    def __init__(self, state: ServerState, context: zmq.Context, publisher: Publisher) -> None:
        self.state = state
        self.context = context
        self.publisher = publisher
        self.thread = threading.Thread(target=self._run, name="election", daemon=True)
        self.subscriber_thread = threading.Thread(target=self._listen, name="coordinator-sub", daemon=True)
        self.last_announcement = 0.0
        self.last_election = 0.0

    def start(self) -> None:
        self.subscriber_thread.start()
        self.thread.start()

    def _listen(self) -> None:
        socket = self.context.socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt_string(zmq.SUBSCRIBE, "servers")
        socket.connect(self.state.config.subscribe_endpoint)
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            while not self.state.stop.is_set():
                if socket not in dict(poller.poll(500)):
                    continue
                try:
                    topic, body = socket.recv_multipart()
                    incoming = receive(self.state.clock, body)
                    if (topic == b"servers" and incoming.get("kind") == "COORDINATOR"
                            and self.state.set_coordinator(str(incoming["leader"]), int(incoming["rank"]))):
                        LOG.info("Coordinator announced: %s (rank %s)", incoming["leader"], incoming["rank"])
                except (ValueError, KeyError, ValueError, TypeError) as exc:
                    LOG.warning("Invalid announcement: %s", exc)
        finally:
            socket.close(linger=0)

    def _announce(self, rank: int) -> None:
        if self.state.set_coordinator(self.state.config.name, rank):
            self.publisher.publish("servers", {
                "kind": "COORDINATOR", "leader": self.state.config.name, "rank": rank,
            })
            self.last_announcement = time.monotonic()
            LOG.info("Elected coordinator: %s (rank %s)", self.state.config.name, rank)

    def _elect(self, rank: int, peers: dict[str, dict]) -> None:
        higher = sorted(
            (p for p in peers.values() if int(p["rank"]) > rank),
            key=lambda p: p["rank"], reverse=True,
        )
        any_higher_replied = False
        for peer in higher:
            try:
                answer = call_peer(self.state, self.context, peer["endpoint"],
                                   "ELECTION", rank=rank, name=self.state.config.name)
                if answer.get("answer") == "OK":
                    any_higher_replied = True
            except ConnectionError:
                LOG.warning("Higher-ranked server did not respond: %s", peer["name"])
        if not any_higher_replied:
            self._announce(rank)
        else:
            LOG.info("Election requested from higher-ranked server")

    def _run(self) -> None:
        while not self.state.stop.is_set():
            rank, peers, leader = self.state.directory()
            now = time.monotonic()
            if rank is not None and peers:
                highest = max(peers.values(), key=lambda p: p["rank"])
                election_needed = (
                    leader is None or leader != highest["name"] or
                    self.state.election_requested.is_set()
                )
                if election_needed and now - self.last_election >= 3.0:
                    self.state.election_requested.clear()
                    self.last_election = now
                    self._elect(rank, peers)
                if self.state.is_coordinator() and now - self.last_announcement >= 10:
                    self._announce(rank)
            self.state.stop.wait(1.0)

#------------------------------------------------------------

class BerkeleySync:
    def __init__(self, state: ServerState, context: zmq.Context) -> None:
        self.state = state
        self.context = context
        self.thread = threading.Thread(target=self._run, name="berkeley", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _coordinator_sync(self, peers: dict[str, dict]) -> None:
        observations = [self.state.clock.now_ms()]
        for peer in peers.values():
            if peer["name"] == self.state.config.name:
                continue
            try:
                start = time.monotonic()
                answer = call_peer(self.state, self.context, peer["endpoint"], "TIME_QUERY")
                rtt = time.monotonic() - start
                if answer.get("status") == "ok":
                    observations.append(int(answer["time_ms"]) + int(rtt * 500))
            except (ConnectionError, ValueError):
                LOG.warning("No clock response from %s", peer["name"])
        average = round(sum(observations) / len(observations))
        offset = self.state.clock.adjust_to(average)
        for peer in peers.values():
            if peer["name"] == self.state.config.name:
                continue
            try:
                call_peer(self.state, self.context, peer["endpoint"], "TIME_SET",
                          coordinator=self.state.config.name,
                          time_ms=self.state.clock.now_ms())
            except ConnectionError:
                LOG.warning("Could not adjust %s", peer["name"])
        LOG.info("Berkeley: average=%s, local offset=%sms, participants=%s",
                 average, offset, len(observations))

    def _follower_sync(self, leader: str, peers: dict[str, dict]) -> None:
        info = peers.get(leader)
        if not info:
            return
        try:
            start = time.monotonic()
            answer = call_peer(self.state, self.context, info["endpoint"], "TIME_SYNC")
            rtt = time.monotonic() - start
            if answer.get("status") == "ok":
                offset = self.state.clock.adjust_to(int(answer["time_ms"]) + int(rtt * 500))
                LOG.info("Clock synchronized with %s; offset=%sms", leader, offset)
        except (ConnectionError, ValueError):
            LOG.warning("Coordinator did not respond to synchronization: %s", leader)
            self.state.election_requested.set()

    def _run(self) -> None:
        last_count = 0
        last_run = 0.0
        while not self.state.stop.is_set():
            now = time.monotonic()
            count = self.state.requests_processed()
            if (count - last_count >= self.state.config.clock_sync_every_messages
                    or now - last_run >= 30):
                rank, peers, leader = self.state.directory()
                if rank is not None and leader:
                    if self.state.is_coordinator():
                        self._coordinator_sync(peers)
                    else:
                        self._follower_sync(leader, peers)
                    last_count, last_run = count, now
            self.state.stop.wait(2.0)

#------------------------------------------------------------

class Replicator:
    def __init__(self, state: ServerState, storage: Storage, context: zmq.Context) -> None:
        self.state = state
        self.storage = storage
        self.context = context
        self.thread = threading.Thread(target=self._run, name="replication", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _pull_from(self, peer: dict) -> None:
        cursor = self.storage.get_cursor(peer["name"])
        fetched = 0
        for _ in range(10):  # up to 1,000 events per peer per pass
            answer = call_peer(self.state, self.context, peer["endpoint"],
                               "SYNC_PULL", since=cursor, limit=100, timeout_ms=5000)
            if answer.get("status") != "ok":
                break
            events = answer.get("events", [])
            for event in events:
                if self.storage.apply_event(event):
                    fetched += 1
                cursor = max(cursor, int(event["sequence"]))
            if events:
                self.storage.set_cursor(peer["name"], cursor)
            if len(events) < 100:
                break
        if fetched:
            LOG.info("Replicated %s events from %s (cursor=%s)",
                     fetched, peer["name"], cursor)

    def _run(self) -> None:
        while not self.state.stop.is_set():
            _, peers, _ = self.state.directory()
            for peer in peers.values():
                if peer["name"] == self.state.config.name:
                    continue
                try:
                    self._pull_from(peer)
                except (ConnectionError, zmq.ZMQError, ValueError, TypeError, KeyError) as exc:
                    LOG.warning("Replication with %s: %s", peer["name"], exc)
            self.state.stop.wait(self.state.config.replication_retry_seconds)