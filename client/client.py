"""
CO3093 - Client Core Networking Engine.
Manages dual-plane connectivity: TCP for signaling/chat and UDP for reliable sliding-window file streaming.
"""

import socket
import threading
import time
import os
import hashlib
import json
import logging

from protocol.packet import Packet, MsgType, PacketFlags, send_packet, recv_packet
from protocol.crypto import default_crypto
from protocol.udp_stream import UDPPacket, StreamSender, StreamReceiver, TYPE_DATA, TYPE_ACK, TYPE_FIN, TYPE_HEARTBEAT

logger = logging.getLogger("ClientEngine")


class CollaborationClient:
    """Handles connection, password authentication, messaging, and UDP file transfers."""
    def __init__(self, server_host="127.0.0.1", server_tcp_port=8888):
        self.server_host = server_host
        self.server_tcp_port = server_tcp_port
        self.server_udp_port = 8889

        self.username = ""
        self.auth_token = ""
        self.is_connected = False
        self.tcp_sock = None
        self.udp_sock = None
        self.udp_port = 0

        self.active_users = []
        self.messages = []
        self.transfers = {}
        self.active_receivers = {}
        self.active_senders = {}
        self.stream_ready_events = {}

        self.running = False
        self.lock = threading.RLock()
        self.seq_counter = 0

        self.download_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "downloads"))
        os.makedirs(self.download_dir, exist_ok=True)

    def _next_seq(self):
        """Generate monotonic sequence numbers for outgoing packets."""
        with self.lock:
            self.seq_counter += 1
            return self.seq_counter

    def connect_and_login(self, username, password="", host=None, port=None, is_register=False):
        """Connect to server, handshake, and authenticate/register with password (Step 2 & 3)."""
        if host: self.server_host = host
        if port: self.server_tcp_port = port

        try:
            # Bind UDP socket for incoming file streams & UDP heartbeats
            self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_sock.bind(("0.0.0.0", 0))
            self.udp_port = self.udp_sock.getsockname()[1]

            # Connect via TCP
            self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_sock.settimeout(5.0)
            self.tcp_sock.connect((self.server_host, self.server_tcp_port))

            # Step 2: Handshake
            send_packet(self.tcp_sock, Packet(MsgType.HANDSHAKE_REQ, payload=b"CLIENT_HELLO"))
            ack = recv_packet(self.tcp_sock)
            if ack.msg_type != MsgType.HANDSHAKE_ACK:
                return False, "Handshake failed"

            try:
                ack_data = ack.to_json()
                self.server_udp_port = int(ack_data.get("udp_port", 8889))
            except Exception:
                pass

            # Step 3: Login or Register with username and password (scrypt + salt)
            msg_code = MsgType.AUTH_REGISTER if is_register else MsgType.AUTH_LOGIN
            auth_pkt = Packet.json_payload(msg_code, {
                "username": username,
                "password": password,
                "udp_port": self.udp_port,
                "is_register": is_register
            })
            send_packet(self.tcp_sock, auth_pkt)

            while True:
                resp = recv_packet(self.tcp_sock)
                if resp.msg_type == MsgType.AUTH_FAILURE:
                    self.tcp_sock.close()
                    self.udp_sock.close()
                    return False, resp.to_json().get("reason", "Login failed")
                elif resp.msg_type == MsgType.AUTH_SUCCESS:
                    self.username = username
                    self.is_connected = True
                    self.running = True
                    resp_data = resp.to_json()
                    self.auth_token = resp_data.get("token", "")
                    self.active_users = resp_data.get("users", [])

                    self.tcp_sock.settimeout(None)
                    threading.Thread(target=self._tcp_listen_loop, name=f"TCP-{username}", daemon=True).start()
                    threading.Thread(target=self._udp_listen_loop, name=f"UDP-{username}", daemon=True).start()
                    threading.Thread(target=self._heartbeat_loop, name=f"Heartbeat-{username}", daemon=True).start()

                    return True, "Login successful"
                elif resp.msg_type == MsgType.USER_JOINED:
                    continue
                else:
                    return False, f"Unexpected response: {resp.msg_type.name}"

        except Exception as e:
            for s in (self.tcp_sock, self.udp_sock):
                if s:
                    try: s.close()
                    except Exception: pass
            return False, str(e)

    def disconnect(self):
        """Send disconnect notification and close sockets."""
        if not self.is_connected:
            return
        self.running = False
        self.is_connected = False
        self.username = ""
        self.auth_token = ""
        with self.lock:
            self.active_users.clear()
            self.messages.clear()
            self.transfers.clear()
            for sender in list(self.active_senders.values()):
                try: sender.cancel()
                except Exception: pass
            self.active_senders.clear()
            self.active_receivers.clear()
            self.stream_ready_events.clear()
        try:
            if self.tcp_sock:
                send_packet(self.tcp_sock, Packet(MsgType.DISCONNECT, payload=b"BYE"))
                self.tcp_sock.close()
            if self.udp_sock:
                self.udp_sock.close()
        except Exception:
            pass

    def _send_chat(self, target, text, is_broadcast):
        """Send encrypted chat message with AES-256-GCM (Step 5 & 8)."""
        if not self.is_connected: return False
        enc = default_crypto.encrypt_string(text)
        mtype = MsgType.MSG_BROADCAST if is_broadcast else MsgType.MSG_UNICAST
        payload = {"text": enc} if is_broadcast else {"target": target, "text": enc}
        pkt = Packet.json_payload(mtype, payload, seq=self._next_seq(), flags=PacketFlags.ENCRYPTED)
        try:
            send_packet(self.tcp_sock, pkt)
            with self.lock:
                self.messages.append({
                    "id": self.seq_counter, "sender": self.username, "target": target,
                    "text": text, "timestamp": time.time(), "is_broadcast": is_broadcast, "is_mine": True
                })
            return True
        except Exception:
            return False

    def send_broadcast_message(self, text):
        """Send encrypted broadcast message to global chat (Step 5)."""
        return self._send_chat("ALL", text, True)

    def send_unicast_message(self, target_user, text):
        """Send encrypted direct message to a specific peer (Step 5)."""
        return self._send_chat(target_user, text, False)

    def send_file(self, target_user, file_path=None, file_bytes=None, filename=None):
        """Stream a file reliably over UDP using Selective Repeat (Step 6)."""
        if not self.is_connected: return False, "Client is not connected"

        if file_bytes is None:
            if not file_path or not os.path.exists(file_path):
                return False, "File does not exist"
            filename = os.path.basename(filename or file_path)
            with open(file_path, "rb") as f:
                file_data = f.read()
        else:
            filename = os.path.basename(filename or "uploaded_file.bin") or "uploaded_file.bin"
            file_data = file_bytes

        file_size = len(file_data)
        file_hash = hashlib.sha256(file_data).hexdigest()
        stream_id = int(time.time() * 1000) % 1000000000

        target_info = next((u for u in self.active_users if u["username"] == target_user), None)
        if not target_info:
            return False, f"Target user '{target_user}' is not online"

        target_ip = target_info.get("ip", "127.0.0.1")
        if target_ip in ("127.0.0.1", "0.0.0.0"):
            target_ip = self.server_host if self.server_host not in ("127.0.0.1", "localhost", "0.0.0.0") else "127.0.0.1"
        target_udp_port = target_info.get("udp_port", 0)
        if not target_udp_port or target_udp_port <= 0:
            return False, f"Target user '{target_user}' has invalid UDP port ({target_udp_port})"

        with self.lock:
            self.transfers[stream_id] = {
                "stream_id": stream_id, "role": "sender", "filename": filename,
                "file_size": file_size, "target": target_user, "progress": 0.0,
                "status": "Starting", "sha256": file_hash
            }

        def on_prog(acked, total, pct):
            with self.lock:
                if stream_id in self.transfers:
                    self.transfers[stream_id]["progress"] = round(pct, 1)
                    self.transfers[stream_id]["status"] = "Completed" if pct >= 100.0 else "Transferring..."

        sender = StreamSender(
            self.udp_sock, (target_ip, target_udp_port), stream_id, file_data,
            encrypt=True, on_progress=on_prog
        )
        ready_event = threading.Event()
        with self.lock:
            self.active_senders[stream_id] = sender
            self.stream_ready_events[stream_id] = ready_event

        # Send signaling announcement over TCP
        announce = Packet.json_payload(
            MsgType.FILE_ANNOUNCE,
            {
                "target": target_user, "filename": filename, "file_size": file_size,
                "sha256": file_hash, "stream_id": stream_id,
                "total_chunks": sender.total_chunks, "sender_udp_port": self.udp_port
            }
        )
        send_packet(self.tcp_sock, announce)

        # Launch UDP sending worker thread upon receiving FILE_ANNOUNCE_ACK
        def _send_worker():
            ready_event.wait(timeout=1.0)
            sender.send_loop()

        threading.Thread(target=_send_worker, name=f"Sender-{stream_id}", daemon=True).start()
        return True, f"Started sending file '{filename}' to {target_user}"

    def _tcp_listen_loop(self):
        """Listen for control packets and incoming messages from server."""
        while self.running:
            try:
                pkt = recv_packet(self.tcp_sock)
            except Exception:
                self.is_connected = False
                self.running = False
                break

            msg_type = pkt.msg_type

            if msg_type == MsgType.USER_JOINED:
                data = pkt.to_json()
                with self.lock:
                    self.active_users = [u for u in self.active_users if u.get("username") != data.get("username")]
                    self.active_users.append(data)

            elif msg_type == MsgType.USER_LEFT:
                uname = pkt.to_json().get("username")
                with self.lock:
                    self.active_users = [u for u in self.active_users if u.get("username") != uname]

            elif msg_type in (MsgType.MSG_BROADCAST, MsgType.MSG_UNICAST):
                data = pkt.to_json()
                text = data.get("text", "")
                if pkt.flags & PacketFlags.ENCRYPTED:
                    try: text = default_crypto.decrypt_string(text)
                    except Exception: text = "[Decryption Error]"

                with self.lock:
                    self.messages.append({
                        "id": pkt.seq,
                        "sender": data.get("sender"),
                        "target": "ALL" if data.get("is_broadcast") else self.username,
                        "text": text,
                        "timestamp": data.get("timestamp", time.time()),
                        "is_broadcast": data.get("is_broadcast", False),
                        "is_mine": False
                    })

            elif msg_type == MsgType.MSG_DELIVERY_ACK:
                data = pkt.to_json()
                if data.get("status") == "USER_NOT_FOUND":
                    tgt = data.get("target", "unknown")
                    with self.lock:
                        self.messages.append({
                            "id": pkt.seq, "sender": "SYSTEM", "target": self.username,
                            "text": f"Cannot deliver message: user '{tgt}' is offline.",
                            "timestamp": time.time(), "is_broadcast": False, "is_mine": False
                        })

            elif msg_type == MsgType.FILE_ANNOUNCE:
                data = pkt.to_json()
                stream_id = data.get("stream_id")
                filename = os.path.basename(data.get("filename", "file.bin")) or "file.bin"
                file_size = data.get("file_size", 0)
                total_chunks = data.get("total_chunks", max(1, (file_size + 1199) // 1200))

                with self.lock:
                    self.transfers[stream_id] = {
                        "stream_id": stream_id, "role": "receiver", "filename": filename,
                        "file_size": file_size, "sender": data.get("sender"), "progress": 0.0,
                        "status": "Receiving", "sha256": data.get("sha256")
                    }

                def on_recv_prog(got, tot, pct):
                    with self.lock:
                        if stream_id in self.transfers:
                            self.transfers[stream_id]["progress"] = min(99.0, round(pct, 1))

                sender_name = data.get("sender")
                receiver = StreamReceiver(stream_id, total_chunks, on_progress=on_recv_prog)
                with self.lock:
                    self.active_receivers[stream_id] = receiver

                # Reply with FILE_ANNOUNCE_ACK so sender begins streaming
                ack_pkt = Packet.json_payload(MsgType.FILE_ANNOUNCE_ACK, {
                    "target": sender_name, "stream_id": stream_id, "status": "READY"
                })
                try:
                    send_packet(self.tcp_sock, ack_pkt)
                except Exception:
                    pass

            elif msg_type == MsgType.FILE_ANNOUNCE_ACK:
                data = pkt.to_json()
                stream_id = data.get("stream_id")
                with self.lock:
                    ready_event = self.stream_ready_events.get(stream_id)
                    if ready_event:
                        ready_event.set()

    def _udp_listen_loop(self):
        """Worker thread processing incoming UDP data chunks and ACKs."""
        while self.running:
            try:
                raw, addr = self.udp_sock.recvfrom(65535)
                pkt = UDPPacket.decode(raw)
                if not pkt: continue

                if pkt.pkt_type == TYPE_DATA:
                    receiver = self.active_receivers.get(pkt.stream_id)
                    if receiver:
                        is_enc = bool(pkt.flags & 1)
                        receiver.put_chunk(pkt.chunk_idx, pkt.payload, is_enc)
                        ack = UDPPacket(TYPE_ACK, pkt.stream_id, pkt.chunk_idx, pkt.total_chunks)
                        self.udp_sock.sendto(ack.encode(), addr)

                        if receiver.is_complete() and not receiver.finalized:
                            receiver.finalized = True
                            self._finalize_received_file(pkt.stream_id, receiver)

                elif pkt.pkt_type == TYPE_ACK:
                    sender = self.active_senders.get(pkt.stream_id)
                    if sender:
                        sender.handle_ack(pkt.chunk_idx)

                elif pkt.pkt_type == TYPE_FIN:
                    receiver = self.active_receivers.get(pkt.stream_id)
                    if receiver:
                        receiver.handle_fin(pkt.payload)

                elif pkt.pkt_type == TYPE_HEARTBEAT:
                    try:
                        resp_data = json.loads(pkt.payload.decode('utf-8'))
                        if resp_data.get("status") == "OK" and "users" in resp_data:
                            with self.lock:
                                self.active_users = resp_data["users"]
                    except Exception:
                        pass

            except Exception:
                if not self.running: break
                continue

    def _finalize_received_file(self, stream_id, receiver):
        """Reassemble received chunks and persist to disk."""
        try:
            file_bytes = receiver.reassemble()
            info = self.transfers.get(stream_id, {})
            filename = os.path.basename(info.get("filename", f"file_{stream_id}.bin")) or f"file_{stream_id}.bin"
            actual_hash = hashlib.sha256(file_bytes).hexdigest()
            save_path = os.path.join(self.download_dir, filename)

            base, ext = os.path.splitext(save_path)
            c = 1
            while os.path.exists(save_path):
                save_path = f"{base}_{c}{ext}"
                c += 1

            with open(save_path, "wb") as f:
                f.write(file_bytes)

            with self.lock:
                self.transfers[stream_id]["saved_filename"] = os.path.basename(save_path)
                self.transfers[stream_id]["saved_path"] = save_path
                self.transfers[stream_id]["progress"] = 100.0
                self.transfers[stream_id]["status"] = "Completed" if actual_hash == info.get("sha256") else "Corrupted"
        except Exception as e:
            logger.error(f"Error finalizing file: {e}")

    def _heartbeat_loop(self):
        """Send periodic UDP Heartbeat with token and peer discovery every 5 seconds (Step 6/7)."""
        while self.running:
            time.sleep(5.0)
            if self.is_connected and self.udp_sock and self.auth_token:
                try:
                    hb_data = json.dumps({
                        "username": self.username,
                        "token": self.auth_token,
                        "action": "discovery"
                    }).encode('utf-8')
                    hb_pkt = UDPPacket(TYPE_HEARTBEAT, 0, 0, 0, payload=hb_data)
                    self.udp_sock.sendto(hb_pkt.encode(), (self.server_host, self.server_udp_port))
                except Exception:
                    pass

            if self.is_connected and self.tcp_sock:
                try:
                    send_packet(self.tcp_sock, Packet(MsgType.HEARTBEAT_PING, payload=b"PING"))
                except Exception:
                    break
