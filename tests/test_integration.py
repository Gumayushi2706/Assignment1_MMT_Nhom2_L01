"""
Comprehensive Integration & Scalability Tests for CO3093 Collaboration System.
Covers Handshake, Concurrency (15 clients), Unicast Messaging, UDP Streaming, and Resilience on Abrupt Disconnect.
"""

import unittest
import socket
import threading
import time
import os
import sys
import hashlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server.server import CollaborationServer
from client.client import CollaborationClient


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_tcp_port = 18888
        cls.server_udp_port = 18889
        cls.server = CollaborationServer(host="127.0.0.1", tcp_port=cls.server_tcp_port, udp_port=cls.server_udp_port, verbose=False)
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        time.sleep(0.2)

    def test_01_handshake_and_login(self):
        """Test basic TCP handshake and client authentication."""
        client = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)
        ok, msg = client.connect_and_login("TestUser1")
        self.assertTrue(ok, f"Login failed: {msg}")
        self.assertTrue(client.is_connected)
        time.sleep(0.2)
        client.disconnect()

    def test_02_concurrency_15_clients(self):
        """Test server concurrency with 15 simultaneous client connections."""
        clients = []
        errors = []

        def worker(idx):
            c = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)
            ok, msg = c.connect_and_login(f"Concurrent_{idx}")
            if not ok:
                errors.append(f"Client {idx} failed: {msg}")
            else:
                clients.append(c)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(15)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Errors during concurrency connect: {errors}")
        self.assertEqual(len(clients), 15)

        time.sleep(0.5)
        all_users = self.server.user_manager.get_all_users()
        self.assertGreaterEqual(len(all_users), 15)

        clients[0].send_broadcast_message("Hello concurrency test from client 0!")
        time.sleep(0.5)

        for c in clients:
            c.disconnect()
        time.sleep(0.5)

    def test_03_unicast_messaging_and_encryption(self):
        """Test end-to-end encrypted direct messaging between two peers."""
        alice = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)
        bob = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)

        alice.connect_and_login("Alice")
        bob.connect_and_login("Bob")
        time.sleep(0.3)

        secret_text = "Super Confidential Message 12345"
        alice.send_unicast_message("Bob", secret_text)
        time.sleep(0.5)

        received_texts = [m["text"] for m in bob.messages if m["sender"] == "Alice"]
        self.assertIn(secret_text, received_texts)

        alice.disconnect()
        bob.disconnect()

    def test_04_reliable_udp_file_transfer(self):
        """Test streaming an 80KB binary file over UDP with SHA-256 integrity check."""
        charlie = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)
        david = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)

        charlie.connect_and_login("Charlie")
        david.connect_and_login("David")
        time.sleep(0.3)

        test_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "test_sample.dat"))
        sample_data = os.urandom(80 * 1024)
        with open(test_file_path, "wb") as f:
            f.write(sample_data)
        expected_hash = hashlib.sha256(sample_data).hexdigest()

        ok, msg = charlie.send_file("David", test_file_path)
        self.assertTrue(ok, f"Failed to send file: {msg}")

        start_time = time.time()
        transfer_done = False
        saved_file_path = None
        while time.time() - start_time < 5.0:
            time.sleep(0.1)
            for tid, t in david.transfers.items():
                if t.get("filename") == "test_sample.dat" and t.get("progress") >= 100.0:
                    transfer_done = True
                    saved_file_path = t.get("saved_path")
                    break
            if transfer_done:
                break

        self.assertTrue(transfer_done, "File transfer over UDP did not complete within timeout")
        self.assertIsNotNone(saved_file_path)
        self.assertTrue(os.path.exists(saved_file_path))
        with open(saved_file_path, "rb") as f:
            rec_data = f.read()
        self.assertEqual(hashlib.sha256(rec_data).hexdigest(), expected_hash)

        if os.path.exists(test_file_path):
            os.remove(test_file_path)
        if saved_file_path and os.path.exists(saved_file_path):
            os.remove(saved_file_path)

        charlie.disconnect()
        david.disconnect()

    def test_05_resilience_on_abrupt_disconnect(self):
        """Test server resilience when a client abruptly resets connection (RST)."""
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            raw_sock.connect(("127.0.0.1", self.server_tcp_port))

            from protocol.packet import Packet, MsgType, send_packet, recv_packet
            send_packet(raw_sock, Packet(MsgType.HANDSHAKE_REQ, payload=b"HELLO"))
            recv_packet(raw_sock)
            send_packet(raw_sock, Packet.json_payload(MsgType.AUTH_LOGIN, {"username": "CrashingUser", "udp_port": 12345}))
            recv_packet(raw_sock)

            self.assertIsNotNone(self.server.user_manager.get_user("CrashingUser"))

            # Force abrupt socket reset
            raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b'\x01\x00\x00\x00\x00\x00\x00\x00')
        finally:
            raw_sock.close()
        time.sleep(0.5)

        test_client = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)
        ok, msg = test_client.connect_and_login("HealthyClient")
        self.assertTrue(ok)
        test_client.disconnect()

    def test_06_unicast_to_offline_user_ack(self):
        """Test server returns USER_NOT_FOUND when unicast target is not online."""
        sender = CollaborationClient(server_host="127.0.0.1", server_tcp_port=self.server_tcp_port)
        ok, msg = sender.connect_and_login("UnicastSender")
        self.assertTrue(ok)
        time.sleep(0.2)

        sender.send_unicast_message("NonExistentUser123", "Hello ghost")
        time.sleep(0.3)

        sys_messages = [m["text"] for m in sender.messages if m["sender"] == "SYSTEM"]
        self.assertTrue(any("offline" in m.lower() for m in sys_messages), f"Expected offline notice in: {sys_messages}")
        sender.disconnect()

    def test_07_web_server_path_traversal_defense(self):
        """Test RawSocketHTTPServer blocks directory traversal attempts in /downloads/."""
        from client.web_server import RawSocketHTTPServer
        import urllib.request
        dummy_client = CollaborationClient()
        web_server = RawSocketHTTPServer(dummy_client, host="127.0.0.1", port=15088)
        web_server.start()
        time.sleep(0.3)

        try:
            req = urllib.request.Request(f"http://127.0.0.1:{web_server.port}/downloads/../../README.md")
            try:
                resp = urllib.request.urlopen(req)
                resp.close()
                self.fail("Path traversal succeeded unexpectedly")
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 404)
                e.close()
        finally:
            web_server.stop()

    def test_08_co3094_rest_apis(self):
        """Test CO3094 RESTful endpoints: /login, /get-list, /broadcast-peer, /logout, and cookies."""
        from client.web_server import RawSocketHTTPServer
        import urllib.request
        import json

        client = CollaborationClient()
        web_server = RawSocketHTTPServer(client, host="127.0.0.1", port=15099)
        web_server.start()
        time.sleep(0.3)

        try:
            # Test /login endpoint with JSON payload and cookie emission
            login_payload = json.dumps({"username": "RestUser", "server_ip": "127.0.0.1", "server_port": self.server_tcp_port}).encode('utf-8')
            req = urllib.request.Request(f"http://127.0.0.1:{web_server.port}/login", data=login_payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                body = json.loads(resp.read().decode('utf-8'))
                self.assertTrue(body.get("success"))
                set_cookie = resp.headers.get("Set-Cookie", "")
                self.assertIn("session_user=RestUser", set_cookie)

            # Test /get-list endpoint
            req = urllib.request.Request(f"http://127.0.0.1:{web_server.port}/get-list")
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                body = json.loads(resp.read().decode('utf-8'))
                self.assertEqual(body.get("username"), "RestUser")
                self.assertTrue(body.get("is_connected"))

            # Test /broadcast-peer endpoint
            msg_payload = json.dumps({"message": "Hello from CO3094 REST"}).encode('utf-8')
            req = urllib.request.Request(f"http://127.0.0.1:{web_server.port}/broadcast-peer", data=msg_payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                body = json.loads(resp.read().decode('utf-8'))
                self.assertTrue(body.get("success"))

            # Test /logout endpoint
            req = urllib.request.Request(f"http://127.0.0.1:{web_server.port}/logout", data=b"{}", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                body = json.loads(resp.read().decode('utf-8'))
                self.assertTrue(body.get("success"))

        finally:
            client.disconnect()
            web_server.stop()


if __name__ == '__main__':
    unittest.main()
