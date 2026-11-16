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
        self.next_rank = 1
        self.timeout = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "60"))

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
            if name not in self.servers:
                self.servers[name] = {"rank": self.next_rank}
                self.next_rank += 1
            data = self.servers[name]
            data.update(endpoint=request["endpoint"], last_seen=time.monotonic())
            log.info("Registered %s (rank %s)", name, data["rank"])
            return {"status": "ok", "rank": data["rank"], "servers": self.available()}
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
            try:
                request = receive(service.clock, socket.recv())
                log.info("Received %s", request)
                answer = service.handle(request)
                action = request.get("action", "UNKNOWN")
            except (ValueError, TypeError, KeyError) as error:
                answer = {"status": "error", "error": str(error)}
                action = "ERROR"
            response = envelope(service.clock, action, **answer)
            socket.send(pack_message(response))
            log.info("Sent %s", response)
    finally:
        socket.close(0)
        context.term()

if __name__ == "__main__":
    main()