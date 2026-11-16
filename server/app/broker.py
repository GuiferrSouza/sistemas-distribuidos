import logging
import os

import zmq

def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [broker] %(message)s")
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.DEALER)
    frontend.bind(os.getenv("FRONTEND_BIND", "tcp://*:5555"))
    backend.bind(os.getenv("BACKEND_BIND", "tcp://*:5556"))
    logging.info("Broker ready")
    try:
        zmq.proxy(frontend, backend)
    finally:
        frontend.close(0)
        backend.close(0)
        context.term()

if __name__ == "__main__":
    main()