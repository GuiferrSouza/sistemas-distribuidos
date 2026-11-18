import logging
import threading
import time

import zmq

from app.core import ServerState, receive
from app.network import PeerClient, Publisher
from app.storage import Storage

LOG = logging.getLogger("cluster")

# region COORDINATOR

ELECTION_COOLDOWN_SECONDS = 3.0 # minimum time between two elections started by this server
ANNOUNCE_REFRESH_SECONDS = 10.0 # the coordinator repeats its announcement (PUB/SUB is lossy)
PING_INTERVAL_SECONDS = 2.0 # followers probe the coordinator
PING_FAILURES_BEFORE_ELECTION = 2 # consecutive failed probes before the coordinator is declared dead

class ElectionManager:
    def __init__(self, state: ServerState, context: zmq.Context, publisher: Publisher, peer_client: PeerClient) -> None:
        self.state = state
        self.context = context
        self.publisher = publisher
        self.peer_client = peer_client
        self.thread = threading.Thread(target=self._run, name="election", daemon=True)
        self.subscriber_thread = threading.Thread(target=self._listen, name="coordinator-sub", daemon=True)
        self.last_announcement = 0.0
        self.last_election = 0.0
        self.last_ping = 0.0
        self.ping_failures = 0

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
                    if topic == b"servers" and incoming.get("kind") == "COORDINATOR":
                        self._on_announcement(str(incoming["leader"]), int(incoming["rank"]))
                except (ValueError, KeyError, TypeError) as exc:
                    LOG.warning("Invalid announcement: %s", exc)
        finally:
            socket.close(linger=0)

    def _on_announcement(self, leader: str, rank: int) -> None:
        if leader == self.state.config.name:
            return  # our own announcement coming back through the proxy
        own_rank, _, _ = self.state.directory()
        if own_rank is not None and rank < own_rank:
            # Bully: a live server with a higher rank must not follow a lower-ranked one.
            LOG.info("Ignoring coordinator %s (rank %s): local rank %s is higher", leader, rank, own_rank)
            self.state.election_requested.set()
            return
        previous = self.state.directory()[2]
        if self.state.set_coordinator(leader, rank):
            self.ping_failures = 0
            # The coordinator repeats its announcement periodically: only a change is worth an INFO line.
            level = logging.DEBUG if previous == leader else logging.INFO
            LOG.log(level, "Coordinator announced: %s (rank %s)", leader, rank)
        else:
            LOG.info("Announcement from %s ignored: not in the server directory yet", leader)

    def _announce(self, rank: int, force: bool = False) -> None:
        name = self.state.config.name
        already_leader = self.state.is_coordinator()
        if not self.state.set_coordinator(name, rank):
            return
        now = time.monotonic()
        if already_leader and not force and now - self.last_announcement < ANNOUNCE_REFRESH_SECONDS:
            return  # nothing new to say
        # A periodic refresh is not an election: keep it out of the INFO log.
        level = logging.DEBUG if already_leader and not force else logging.INFO
        self.publisher.publish("servers", {"kind": "COORDINATOR", "leader": name, "rank": rank}, level=level)
        self.last_announcement = now
        if not already_leader:
            LOG.info("Elected coordinator: %s (rank %s)", name, rank)
        elif force:
            LOG.info("Coordinator announcement repeated on request: %s (rank %s)", name, rank)
        else:
            LOG.debug("Coordinator announcement refreshed: %s", name)

    def _elect(self, rank: int, peers: dict[str, dict], force_announce: bool) -> None:
        higher = sorted(
            (p for p in peers.values() if int(p["rank"]) > rank),
            key=lambda p: p["rank"], reverse=True,
        )
        any_higher_replied = False
        for peer in higher:
            try:
                answer = self.peer_client.call(peer, "ELECTION", rank=rank, name=self.state.config.name)
                if answer.get("answer") == "OK":
                    any_higher_replied = True
            except ConnectionError:
                LOG.debug("Higher-ranked server did not respond: %s", peer["name"])
        if not any_higher_replied:
            self._announce(rank, force=force_announce)
            return
        # A live server with a higher rank takes over: step down and wait for its announcement.
        if self.state.clear_coordinator(self.state.config.name):
            LOG.info("Stepping down: higher-ranked server is alive")
        LOG.info("Election requested from higher-ranked server")

    def _check_coordinator(self, leader: str, peers: dict[str, dict], now: float) -> None:
        """Followers probe the coordinator; two failed probes in a row start a new election."""
        if now - self.last_ping < PING_INTERVAL_SECONDS:
            return
        self.last_ping = now
        peer = peers.get(leader)
        if peer is None:
            return
        try:
            self.peer_client.call(peer, "PING", timeout_ms=1000)
            self.ping_failures = 0
        except ConnectionError:
            self.ping_failures += 1
            if self.ping_failures >= PING_FAILURES_BEFORE_ELECTION:
                LOG.warning("Coordinator %s stopped answering; starting an election", leader)
                self.ping_failures = 0
                self.state.clear_coordinator(leader)
                self.last_election = 0.0

    def _tick(self) -> None:
        rank, peers, leader = self.state.directory()
        if rank is None or not peers:
            return
        name = self.state.config.name
        now = time.monotonic()
        requested = self.state.election_requested.is_set()
        # An incumbent coordinator that sees a higher-ranked server in the directory (for example one
        # that just came back) must give the Bully algorithm a chance to hand over the role.
        outranked = leader == name and any(int(p["rank"]) > rank for p in peers.values())
        if (leader is None or requested or outranked) and now - self.last_election >= ELECTION_COOLDOWN_SECONDS:
            self.state.election_requested.clear()
            self.last_election = now
            self._elect(rank, peers, force_announce=requested)
        elif leader == name:
            if now - self.last_announcement >= ANNOUNCE_REFRESH_SECONDS:
                self._announce(rank)
        elif leader is not None:
            self._check_coordinator(leader, peers, now)

    def _run(self) -> None:
        while not self.state.stop.is_set():
            try:
                self._tick()
            except Exception:  # eleection loop must never die
                LOG.exception("Election loop error")
            self.state.stop.wait(1.0)

# endregion COORDINATOR

#------------------------------------------------------------

class BerkeleySync:
    def __init__(self, state: ServerState, peer_client: PeerClient) -> None:
        self.state = state
        self.peer_client = peer_client
        self.thread = threading.Thread(target=self._run, name="berkeley", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _coordinator_sync(self, peers: dict[str, dict]) -> None:
        observations = [self.state.clock.now_ms()]
        participants = []
        for peer in peers.values():
            if peer["name"] == self.state.config.name:
                continue
            try:
                start = time.monotonic()
                answer = self.peer_client.call(peer, "TIME_QUERY")
                rtt = time.monotonic() - start
                if answer.get("status") == "ok":
                    observations.append(int(answer["time_ms"]) + int(rtt * 500))
                    participants.append(peer)
            except (ConnectionError, ValueError, KeyError):
                LOG.debug("No clock response from %s; skipping it in this round", peer["name"])
        average = round(sum(observations) / len(observations))
        offset = self.state.clock.adjust_to(average)
        for peer in participants:  # only servers that took part in the round are adjusted
            try:
                self.peer_client.call(peer, "TIME_SET", coordinator=self.state.config.name,
                                      time_ms=self.state.clock.now_ms())
            except ConnectionError:
                LOG.debug("Could not adjust %s", peer["name"])
        LOG.info("Berkeley: average=%s, local offset=%sms, participants=%s",
                 average, offset, len(observations))

    def _follower_sync(self, leader: str, peers: dict[str, dict]) -> None:
        info = peers.get(leader)
        if not info:
            return
        try:
            start = time.monotonic()
            answer = self.peer_client.call(peer=info, action="TIME_SYNC")
            rtt = time.monotonic() - start
            if answer.get("status") == "ok":
                offset = self.state.clock.adjust_to(int(answer["time_ms"]) + int(rtt * 500))
                LOG.info("Clock synchronized with %s; offset=%sms", leader, offset)
            else:
                LOG.info("%s refused clock synchronization (%s); requesting an election",
                         leader, answer.get("error"))
                self.state.election_requested.set()
        except (ConnectionError, ValueError, KeyError):
            # Detecting a dead coordinator is the ElectionManager's job (PING); starting an election
            # here as well only produced redundant elections.
            LOG.debug("Coordinator did not respond to synchronization: %s", leader)

    def _run(self) -> None:
        last_count = 0
        last_run = 0.0
        while not self.state.stop.is_set():
            try:
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
            except Exception:  # noqa: BLE001 - the sync loop must never die
                LOG.exception("Berkeley loop error")
            self.state.stop.wait(2.0)

#------------------------------------------------------------

class Replicator:
    def __init__(self, state: ServerState, storage: Storage, peer_client: PeerClient) -> None:
        self.state = state
        self.storage = storage
        self.peer_client = peer_client
        self.thread = threading.Thread(target=self._run, name="replication", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def _pull_from(self, peer: dict) -> None:
        cursor = self.storage.get_cursor(peer["name"])
        fetched = 0
        for _ in range(10):  # up to 1,000 events per peer per pass
            answer = self.peer_client.call(peer, "SYNC_PULL", timeout_ms=5000, since=cursor, limit=100)
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
                except ConnectionError as exc:
                    # An unavailable peer is expected during a failure; PeerClient logs the transition.
                    LOG.debug("Replication with %s postponed: %s", peer["name"], exc)
                except Exception as exc:  # noqa: BLE001 - keep replicating with the other peers
                    LOG.warning("Replication with %s failed: %s", peer["name"], exc)
            self.state.stop.wait(self.state.config.replication_retry_seconds)