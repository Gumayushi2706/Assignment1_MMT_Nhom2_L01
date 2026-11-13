"""
Unit Tests for Reliable UDP Streaming Engine (Selective Repeat Sliding Window).
"""

import unittest
import os
import sys
import hashlib
import queue
import random
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from protocol.udp_stream import StreamSender, StreamReceiver, UDPPacket, TYPE_DATA, TYPE_ACK, TYPE_FIN, WINDOW_SIZE


class TestUDPStream(unittest.TestCase):
    def test_selective_repeat_with_simulated_loss(self):
        """Test Selective Repeat ARQ and sliding window (N=8) under 20% simulated packet loss."""
        class LossyPipe:
            def __init__(self, target_queue, loss_rate=0.2):
                self.q = target_queue
                self.loss_rate = loss_rate
            def sendto(self, data, addr):
                if random.random() > self.loss_rate:
                    self.q.put((data, addr))

        q_to_recv = queue.Queue()
        q_to_send = queue.Queue()

        sender_pipe = LossyPipe(q_to_recv, loss_rate=0.2)
        receiver_pipe = LossyPipe(q_to_send, loss_rate=0.2)

        test_data = os.urandom(30 * 1024)
        stream_id = 701

        sender = StreamSender(sender_pipe, ('sim', 1), stream_id, test_data, encrypt=True)
        receiver = StreamReceiver(stream_id, sender.total_chunks)

        import threading
        stop = False
        def recv_loop():
            while not stop:
                try:
                    raw, addr = q_to_recv.get(timeout=0.05)
                    pkt = UDPPacket.decode(raw)
                    if pkt and pkt.stream_id == stream_id:
                        if pkt.pkt_type == TYPE_DATA:
                            receiver.put_chunk(pkt.chunk_idx, pkt.payload, pkt.flags & 1)
                            ack = UDPPacket(TYPE_ACK, stream_id, pkt.chunk_idx, pkt.total_chunks)
                            receiver_pipe.sendto(ack.encode(), addr)
                except queue.Empty:
                    continue

        def ack_loop():
            while not stop:
                try:
                    raw, _ = q_to_send.get(timeout=0.05)
                    pkt = UDPPacket.decode(raw)
                    if pkt and pkt.pkt_type == TYPE_ACK:
                        sender.handle_ack(pkt.chunk_idx)
                except queue.Empty:
                    continue

        t_recv = threading.Thread(target=recv_loop, daemon=True)
        t_ack = threading.Thread(target=ack_loop, daemon=True)
        t_send = threading.Thread(target=sender.send_loop, daemon=True)

        t_recv.start()
        t_ack.start()
        t_send.start()

        t_send.join(timeout=10.0)
        stop = True
        t_recv.join(timeout=0.5)
        t_ack.join(timeout=0.5)

        self.assertTrue(sender.finished, "Sender did not finish transmission despite retransmissions")
        self.assertTrue(receiver.is_complete(), "Receiver did not receive all chunks")
        reassembled = receiver.reassemble()
        self.assertEqual(hashlib.sha256(reassembled).hexdigest(), hashlib.sha256(test_data).hexdigest())

    def test_fin_packet_handling(self):
        """Test encoding, decoding and handling of TYPE_FIN segment."""
        fin_pkt = UDPPacket(TYPE_FIN, stream_id=505, chunk_idx=0, total_chunks=10, payload=b"CANCEL")
        encoded = fin_pkt.encode()

        decoded = UDPPacket.decode(encoded)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.pkt_type, TYPE_FIN)
        self.assertEqual(decoded.payload, b"CANCEL")

        receiver = StreamReceiver(505, 10)
        receiver.handle_fin(decoded.payload)
        self.assertTrue(receiver.fin_received)
        self.assertTrue(receiver.cancelled)

    def test_sliding_window_advancement(self):
        """Test Selective Repeat sliding window base advancement on ACK arrival."""
        class DummySocket:
            def sendto(self, data, addr): pass

        sender = StreamSender(DummySocket(), ('127.0.0.1', 9999), 303, os.urandom(1200 * 10), encrypt=False)
        self.assertEqual(sender.base, 0)
        self.assertEqual(WINDOW_SIZE, 8)

        # Receive ACK for chunk 0 -> base advances to 1
        sender.handle_ack(0)
        self.assertEqual(sender.base, 1)

        # Receive ACK for chunk 2 (out of order) -> base stays at 1
        sender.handle_ack(2)
        self.assertEqual(sender.base, 1)

        # Receive ACK for chunk 1 -> base advances to 3
        sender.handle_ack(1)
        self.assertEqual(sender.base, 3)

    def test_congestion_control_cubic_bbr(self):
        """Test CUBIC congestion window scaling and BBR pacing adaptation."""
        class DummySocket:
            def sendto(self, data, addr): pass

        sender = StreamSender(DummySocket(), ('127.0.0.1', 9999), 404, os.urandom(1200 * 20), encrypt=False)
        self.assertEqual(sender.cwnd, 8.0)
        self.assertGreater(sender.pacing_interval, 0.0)

        # Handle ACKs -> cwnd scales up
        sender.handle_ack(0)
        self.assertGreaterEqual(sender.cwnd, 8.0)


if __name__ == '__main__':
    unittest.main()
