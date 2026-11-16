import logging
import os

import zmq

def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s [pubsub] %(message)s")
    context = zmq.Context()
    xsub = context.socket(zmq.XSUB)
    xpub = context.socket(zmq.XPUB)
    xsub.bind(os.getenv("XSUB_BIND", "tcp://*:5557"))
    xpub.bind(os.getenv("XPUB_BIND", "tcp://*:5558"))
    logging.info("Pub/Sub proxy ready")
    try:
        zmq.proxy(xsub, xpub)
    finally:
        xsub.close(0)
        xpub.close(0)
        context.term()

if __name__ == "__main__":
    main()