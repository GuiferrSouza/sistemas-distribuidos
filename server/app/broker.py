import logging
import os
import time
import uuid
from collections import deque

import msgpack
import zmq

from app.core import Clock, envelope, pack_message

LOG = logging.getLogger("broker")
# A worker that sends nothing for WORKER_TIMEOUT seconds is considered dead. Workers send a
# heartbeat every second, so this tolerates two lost heartbeats.
WORKER_TIMEOUT = float(os.getenv("WORKER_TIMEOUT_SECONDS", "3.0"))
# Maximum time a request may wait for a worker. Must stay below the client receive timeout (5 s).
QUEUE_TIMEOUT = float(os.getenv("QUEUE_TIMEOUT_SECONDS", "4.0"))
MAX_WAITING = 200


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s [broker] %(levelname)s %(message)s")
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)   # clients (REQ)
    backend = context.socket(zmq.ROUTER)    # servers (DEALER workers)
    frontend.setsockopt(zmq.LINGER, 0)
    backend.setsockopt(zmq.LINGER, 0)
    backend.setsockopt(zmq.ROUTER_HANDOVER, 1)  # a restarted worker may reuse its identity
    frontend.bind(os.getenv("FRONTEND_BIND", "tcp://*:5555"))
    backend.bind(os.getenv("BACKEND_BIND", "tcp://*:5556"))
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)
    clock = Clock()

    workers = {}         # identity -> {"seen": last heartbeat, "busy": job id or None}
    idle = deque()       # round-robin of ready workers
    waiting = deque()    # (client identity, body, queued time)
    inflight = {}        # job id -> (worker identity, client identity, body, queued time)

    def name_of(worker_id):
        return worker_id.decode(errors="replace")

    def make_idle(worker_id):
        worker = workers.get(worker_id)
        if worker and worker["busy"] is None and worker_id not in idle:
            idle.append(worker_id)

    def reject(client_id, body, reason):
        try:
            request = msgpack.unpackb(body, raw=False)
            action = request.get("action", "ERROR") if isinstance(request, dict) else "ERROR"
            request_id = request.get("request_id") if isinstance(request, dict) else None
        except Exception:  # noqa: BLE001 - any undecodable body is answered generically
            action, request_id = "ERROR", None
        answer = envelope(clock, action, status="error", error=reason, request_id=request_id)
        frontend.send_multipart([client_id, b"", pack_message(answer)])

    def requeue(job_id, reason):
        """Put a request that was handed to a vanished worker back at the head of the queue."""
        job = inflight.pop(job_id, None)
        if job:
            waiting.appendleft(job[1:])
            LOG.warning("Re-queued request from %s: %s", name_of(job[0]), reason)

    def drop_worker(worker_id, reason):
        worker = workers.pop(worker_id, None)
        if worker is None:
            return
        try:
            idle.remove(worker_id)
        except ValueError:
            pass
        if worker["busy"] is not None:
            requeue(worker["busy"], reason)
        LOG.warning("Worker offline: %s (%s)", name_of(worker_id), reason)

    LOG.info("Broker ready (health-aware round-robin)")
    try:
        while True:
            readable = dict(poller.poll(100))
            now = time.monotonic()

            if backend in readable:
                frames = backend.recv_multipart()
                if len(frames) >= 2:
                    worker_id, command = frames[:2]
                    if command in (b"READY", b"HEARTBEAT"):
                        worker = workers.get(worker_id)
                        if worker is None or command == b"READY":
                            if worker and worker["busy"] is not None:
                                requeue(worker["busy"], "worker restarted")
                            workers[worker_id] = {"seen": now, "busy": None}
                            LOG.info("Worker online: %s", name_of(worker_id))
                        else:
                            worker["seen"] = now
                        make_idle(worker_id)
                    elif command == b"DISCONNECT":
                        drop_worker(worker_id, "left gracefully")
                    elif command == b"REPLY" and len(frames) == 6:
                        _, _, job_id, client_id, separator, body = frames
                        job = inflight.get(job_id)
                        worker = workers.get(worker_id)
                        if job and job[0] == worker_id and job[1] == client_id and separator == b"":
                            inflight.pop(job_id)
                            frontend.send_multipart([client_id, b"", body])
                            if worker:
                                worker["seen"] = now
                                worker["busy"] = None
                                make_idle(worker_id)

            if frontend in readable:
                frames = frontend.recv_multipart()
                if len(frames) == 3 and frames[1] == b"":
                    client_id, _, body = frames
                    if len(waiting) >= MAX_WAITING:
                        reject(client_id, body, "broker_overloaded")
                    else:
                        waiting.append((client_id, body, now))

            for worker_id, worker in list(workers.items()):
                if now - worker["seen"] > WORKER_TIMEOUT:
                    drop_worker(worker_id, "heartbeat timeout")

            # Expire queued requests rather than answering them after the client gave up.
            for _ in range(len(waiting)):
                client_id, body, queued_at = waiting.popleft()
                if now - queued_at > QUEUE_TIMEOUT:
                    reject(client_id, body, "no_server_available")
                else:
                    waiting.append((client_id, body, queued_at))

            while waiting and idle:
                worker_id = idle.popleft()
                worker = workers.get(worker_id)
                if not worker or worker["busy"] is not None:
                    continue
                client_id, body, queued_at = waiting.popleft()
                job_id = uuid.uuid4().bytes
                worker["busy"] = job_id
                inflight[job_id] = (worker_id, client_id, body, queued_at)
                backend.send_multipart([worker_id, b"REQUEST", job_id, client_id, b"", body])
    finally:
        frontend.close(0)
        backend.close(0)
        context.term()


if __name__ == "__main__":
    main()
