"""
Unit Tests for Custom Binary TCP Framing and AES-256-GCM Cryptography.
"""

import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from protocol.packet import Packet, MsgType, PacketFlags
from protocol.crypto import default_crypto, hash_password, verify_password
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

    def test_salted_password_hashing_scrypt(self):
        """Test salted scrypt password derivation (Step 3 / RFC 7914)."""
        password = "SuperSecretPassword2026!"
        salt, pwd_hash = hash_password(password)

        self.assertGreater(len(salt), 0)
        self.assertEqual(len(pwd_hash), 64)  # 32 bytes * 2 = 64 hex chars

        self.assertTrue(verify_password(password, salt, pwd_hash))
        self.assertFalse(verify_password("WrongPassword", salt, pwd_hash))

    def test_udp_heartbeat_packet_and_token(self):
        """Test UDP Heartbeat/Discovery packet encoding, decoding, and token verification (Step 6/7)."""
        import json
        from protocol.udp_stream import TYPE_HEARTBEAT
        from server.user_manager import UserManager

        hb_payload = json.dumps({"username": "alice", "token": "a1b2c3d4e5f6", "action": "discovery"}).encode('utf-8')
        pkt = UDPPacket(TYPE_HEARTBEAT, stream_id=0, chunk_idx=0, total_chunks=0, payload=hb_payload)
        encoded = pkt.encode()

        decoded = UDPPacket.decode(encoded)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.pkt_type, TYPE_HEARTBEAT)
        data = json.loads(decoded.payload.decode('utf-8'))
        self.assertEqual(data["username"], "alice")
        self.assertEqual(data["token"], "a1b2c3d4e5f6")

        mgr = UserManager()
        # Test registration
        ok, msg = mgr.register_user("alice", "mypassword123")
        self.assertTrue(ok)
        ok_dup, _ = mgr.register_user("alice", "mypassword123")
        self.assertFalse(ok_dup)

        # Test session and token verification
        session = mgr.add_user("alice", None, ("127.0.0.1", 12345), udp_port=54321, token="a1b2c3d4e5f6")
        self.assertIsNotNone(session)
        self.assertTrue(mgr.verify_token("alice", "a1b2c3d4e5f6"))
        self.assertFalse(mgr.verify_token("alice", "invalid_token"))


if __name__ == '__main__':
    unittest.main()
