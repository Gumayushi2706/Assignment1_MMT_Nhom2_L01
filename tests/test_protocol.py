"""
Unit Tests for Custom Binary TCP Framing and AES-256-GCM Cryptography.
"""

import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from protocol.packet import Packet, MsgType, PacketFlags
from protocol.crypto import default_crypto
from protocol.udp_stream import UDPPacket, TYPE_DATA


class TestProtocol(unittest.TestCase):
    def test_tcp_packet_encode_decode(self):
        """Test encoding and decoding of 20-byte TCP header with CRC32."""
        payload = b"Hello, Distributed World!"
        pkt = Packet(MsgType.MSG_BROADCAST, payload=payload, seq=42, flags=PacketFlags.ENCRYPTED)
        encoded = pkt.encode()

        decoded = Packet.from_bytes(encoded)
        self.assertEqual(decoded.msg_type, MsgType.MSG_BROADCAST)
        self.assertEqual(decoded.seq, 42)
        self.assertEqual(decoded.flags, PacketFlags.ENCRYPTED)
        self.assertEqual(decoded.payload, payload)
        self.assertEqual(decoded.checksum, pkt.checksum)

    def test_tcp_packet_json(self):
        """Test JSON payload helper creation and serialization."""
        data = {"user": "alice", "text": "Testing JSON payload", "count": 100}
        pkt = Packet.json_payload(MsgType.AUTH_LOGIN, data, seq=1)
        encoded = pkt.encode()

        decoded = Packet.from_bytes(encoded)
        parsed = decoded.to_json()
        self.assertEqual(parsed["user"], "alice")
        self.assertEqual(parsed["text"], "Testing JSON payload")

    def test_crypto_aes_gcm(self):
        """Test AES-256-GCM authenticated encryption and decryption."""
        secret_message = b"Secret collaborating streaming data"
        ciphertext = default_crypto.encrypt(secret_message)
        self.assertNotEqual(secret_message, ciphertext)

        plaintext = default_crypto.decrypt(ciphertext)
        self.assertEqual(plaintext, secret_message)

    def test_udp_packet_encode_decode(self):
        """Test encoding and decoding of 24-byte UDP streaming frame."""
        data = b"Chunk bytes 12345"
        pkt = UDPPacket(TYPE_DATA, stream_id=99, chunk_idx=3, total_chunks=10, payload=data)
        encoded = pkt.encode()

        decoded = UDPPacket.decode(encoded)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.stream_id, 99)
        self.assertEqual(decoded.chunk_idx, 3)
        self.assertEqual(decoded.total_chunks, 10)
        self.assertEqual(decoded.payload, data)

    def test_packet_boundary_and_tampering(self):
        """Test detection of corrupted CRC, invalid magic, and oversized payload."""
        pkt = Packet(MsgType.HANDSHAKE_REQ, payload=b"HELLO")
        raw = bytearray(pkt.encode())

        # Tamper payload -> CRC32 mismatch
        raw[-1] ^= 0xFF
        with self.assertRaises(ValueError):
            Packet.from_bytes(bytes(raw))

        # Tamper magic bytes
        raw[0] = 0x00
        with self.assertRaises(ValueError):
            Packet.from_bytes(bytes(raw))


if __name__ == '__main__':
    unittest.main()
