"""
Minimal ZMQ bus for ZmqGui — only the pieces the dashboard uses.

Ported from ModularTradeApp/console/core/messaging.py. Stripped down
to the SUB-side utilities we actually need (the dashboard never
publishes). The message envelope convention is preserved byte-for-byte
so any publisher written against `Bus.pub_send` will work with us
without changes.

Multipart framing:
    frame 0  topic bytes (for SUB filtering)
    frame 1  UTF-8 JSON body

The body is also expected to carry its topic under the "topic" key,
because upstream code (see `_process_message` in app.py) routes by
that field — downstream arb_scanner etc. already inject it before
publishing, so this is a convention, not an extra cost.
"""

import json
import logging

import zmq

logger = logging.getLogger(__name__)


class Bus:
    """Thin wrapper over a zmq.Context — the dashboard only subscribes."""

    def __init__(self, ctx: zmq.Context = None):
        self.ctx = ctx or zmq.Context()

    def subscriber(self, endpoint: str, topics: list[str]) -> zmq.Socket:
        """
        Create a SUB socket connected to one PUB endpoint, filtered by topics.

        `endpoint` is a full ZMQ URL (e.g. "tcp://127.0.0.1:5552").
        Pass [""] to subscribe to every topic. Returns the socket so the
        caller can register it with a zmq.Poller and call additional
        `connect()` on it to merge multiple PUB streams into one recv loop.
        """
        s = self.ctx.socket(zmq.SUB)
        s.connect(endpoint)
        for t in topics:
            s.setsockopt_string(zmq.SUBSCRIBE, t)
        return s

    def close(self):
        self.ctx.term()

    @staticmethod
    def sub_recv(socket: zmq.Socket, flags: int = 0) -> dict:
        """
        Receive one multipart frame and return the decoded JSON body.

        The topic in frame 0 is discarded — the body is expected to
        carry its topic under msg["topic"] (that's the producer-side
        contract, injected by publishers like the orchestrator and the
        arb_scanner before send_multipart).
        """
        frames = socket.recv_multipart(flags=flags)
        return json.loads(frames[1])
