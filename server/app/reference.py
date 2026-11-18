import logging
import os
import time

import zmq

from app.core import Clock, envelope, pack_message, receive

log = logging.getLogger("reference")

class ReferenceService:
    def __init__(self):
        self.clock = Clock()
        self.servers = {}
        self.ranks = {}
        self.next_rank = 1
        self.timeout = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))

    def available(self):
        return sorted(
            ({"name": name, "rank": data["rank"], "endpoint": data["endpoint"]}
             for name, data in self.servers.items()),
            key=lambda server: server["rank"],
        )

    def expire(self):
        now = time.monotonic()
        for name, data in list(self.servers.items()):
            if now - data["last_seen"] > self.timeout:
                del self.servers[name]
                log.info("Server timed out: %s", name)

    def handle(self, request):
        action = request.get("action")
        name = request.get("name")
        if action == "REGISTER":
            if not name or not request.get("endpoint"):
                return {"status": "error", "error": "Missing server name or endpoint"}
            known = name in self.servers
            if name not in self.ranks:
                self.ranks[name] = self.next_rank
                self.next_rank += 1
            self.servers[name] = {
                "rank": self.ranks[name],
                "endpoint": request["endpoint"],
                "last_seen": time.monotonic(),
            }
            log.info("%s %s (rank %s)", "Refreshed" if known else "Registered", name, self.ranks[name])
            return {"status": "ok", "rank": self.ranks[name], "servers": self.available()}
        if action == "UNREGISTER":
            # Graceful shutdown: peers stop considering this server alive immediately.
            if self.servers.pop(name, None) is not None:
                log.info("Server left: %s", name)
            return {"status": "ok"}
        if action == "LIST":
            return {"status": "ok", "servers": self.available()}
        if action == "HEARTBEAT":
            if name not in self.servers:
                return {"status": "error", "error": "not_registered"}
            self.servers[name]["last_seen"] = time.monotonic()
            return {"status": "ok"}
        return {"status": "error", "error": "unknown_action"}

def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [reference] %(message)s")
    service = ReferenceService()
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(os.getenv("REFERENCE_BIND", "tcp://*:5559"))
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    try:
        while True:
            service.expire()
            if socket not in dict(poller.poll(500)):
                continue
            # Once recv() succeeds a reply MUST be sent, whatever goes wrong in between;
            # otherwise the REP socket would stay stuck in its "must send" state.
            action = "ERROR"
            try:
                request = receive(service.clock, socket.recv())
                action = request.get("action", "UNKNOWN")
                level = logging.DEBUG if action == "LIST" else logging.INFO
                log.log(level, "Received %s", request)
                answer = service.handle(request)
            except Exception as error:  # noqa: BLE001 - the service must keep answering
                log.warning("Request failed: %s", error)
                answer = {"status": "error", "error": str(error)}
                action = "ERROR"
            response = envelope(service.clock, action, **answer)
            socket.send(pack_message(response))
            log.log(logging.DEBUG if action == "LIST" else logging.INFO, "Sent %s", response)
    finally:
        socket.close(0)
        context.term()

if __name__ == "__main__":
    main()
