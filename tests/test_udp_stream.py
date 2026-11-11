"""
Unit Tests for Reliable UDP Streaming Engine (Selective Repeat Sliding Window).
"""

import unittest
import os
import sys
import socket
import threading
import hashlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from protocol.udp_stream import StreamSender, StreamReceiver, UDPPacket, TYPE_DATA, TYPE_ACK


class TestUDPStream(unittest.TestCase):
    def test_reliable_stream_transfer_no_loss(self):
        """Test sending 50KB binary data over UDP with Selective Repeat and AES-GCM."""
        sender_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender_sock.bind(("127.0.0.1", 20002))
        receiver_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver_sock.bind(("127.0.0.1", 20001))

        test_data = os.urandom(50 * 1024)
        stream_id = 101

        sender = StreamSender(sender_sock, ("127.0.0.1", 20001), stream_id, test_data, encrypt=True)
        receiver = StreamReceiver(stream_id, sender.total_chunks)

        stop_recv = False
        def recv_loop():
            while not stop_recv:
                try:
                    receiver_sock.settimeout(0.1)
                    raw, addr = receiver_sock.recvfrom(65535)
                    pkt = UDPPacket.decode(raw)
                    if pkt and pkt.stream_id == stream_id:
                        if pkt.pkt_type == TYPE_DATA:
                            receiver.put_chunk(pkt.chunk_idx, pkt.payload, pkt.flags & 1)
                            ack = UDPPacket(TYPE_ACK, stream_id, pkt.chunk_idx, pkt.total_chunks)
                            receiver_sock.sendto(ack.encode(), addr)
                except socket.timeout:
                    continue
                except Exception:
                    break

        recv_thread = threading.Thread(target=recv_loop, daemon=True)
        recv_thread.start()

        def ack_loop():
            while not stop_recv:
                try:
                    sender_sock.settimeout(0.1)
                    raw, _ = sender_sock.recvfrom(65535)
                    pkt = UDPPacket.decode(raw)
                    if pkt and pkt.pkt_type == TYPE_ACK:
                        sender.handle_ack(pkt.chunk_idx)
                except socket.timeout:
                    continue
                except Exception:
                    break

        ack_thread = threading.Thread(target=ack_loop, daemon=True)
        ack_thread.start()

        send_thread = threading.Thread(target=sender.send_loop, daemon=True)
        send_thread.start()

        try:
            send_thread.join(timeout=10.0)
            self.assertTrue(sender.finished, "Sender did not finish transmission in time")
            self.assertTrue(receiver.is_complete(), "Receiver did not receive all chunks")

            reassembled = receiver.reassemble()
            self.assertEqual(hashlib.sha256(reassembled).hexdigest(), hashlib.sha256(test_data).hexdigest())
        finally:
            stop_recv = True
            recv_thread.join(timeout=0.3)
            ack_thread.join(timeout=0.3)
            try: sender_sock.close()
            except Exception: pass
            try: receiver_sock.close()
            except Exception: pass


if __name__ == '__main__':
    unittest.main()
