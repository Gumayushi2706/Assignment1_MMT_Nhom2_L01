"""
CO3093 - Central Collaboration Server.
Handles TCP signaling, user authentication with password, message routing, file signaling, and heartbeat tracking.
"""

import socket
import threading
import time
import os
import sys
import secrets
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from protocol.packet import Packet, MsgType, send_packet, recv_packet
from protocol.udp_stream import UDPPacket, TYPE_HEARTBEAT
from server.user_manager import UserManager


class CollaborationServer:
    """Central server managing client connections, authentication, and routing."""
    def __init__(self, host="0.0.0.0", tcp_port=8888, udp_port=8889, verbose=True):
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.verbose = verbose

        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_sock = None

        self.user_manager = UserManager()
        self.running = False

    def _log(self, msg):
        """Print concise event status."""
        if self.verbose:
            print(msg, flush=True)

    def start(self):
        """Bind TCP & UDP sockets, start worker threads, and listen for client connections."""
        self.running = True
        self.tcp_sock.bind((self.host, self.tcp_port))
        self.tcp_sock.listen(50)

        # Bind UDP socket for token-authenticated heartbeats and discovery (Step 6/7)
        try:
            self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_sock.bind((self.host, self.udp_port))
            threading.Thread(target=self._udp_listen_loop, name="UDP-Server-Worker", daemon=True).start()
        except Exception as e:
            self._log(f"[Server Warning] Could not bind UDP port {self.udp_port}: {e}")

        threading.Thread(target=self._heartbeat_watchdog, name="Watchdog-Worker", daemon=True).start()
        self._log(f"[Server] Listening on {self.host} (TCP: {self.tcp_port}, UDP: {self.udp_port})")

        try:
            while self.running:
                try:
                    client_sock, client_addr = self.tcp_sock.accept()
                    threading.Thread(
                        target=self._handle_client,
                        args=(client_sock, client_addr),
                        name=f"Client-{client_addr[0]}:{client_addr[1]}",
                        daemon=True
                    ).start()
                except (socket.timeout, OSError):
                    if not self.running:
                        break
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self):
        """Shut down the server and close open sockets."""
        if not self.running:
            return
        self.running = False
        try:
            self.tcp_sock.close()
        except Exception:
            pass
        if self.udp_sock:
            try:
                self.udp_sock.close()
            except Exception:
                pass
        for session in self.user_manager.get_all_sessions():
            try:
                session.tcp_sock.close()
            except Exception:
                pass
        self._log("[Server] Stopped.")

    def _udp_listen_loop(self):
        """Worker thread processing UDP heartbeats with token and peer discovery requests."""
        while self.running and self.udp_sock:
            try:
                raw, addr = self.udp_sock.recvfrom(65535)
                pkt = UDPPacket.decode(raw)
                data = None
                is_custom_pkt = False
                if pkt and pkt.pkt_type == TYPE_HEARTBEAT:
                    is_custom_pkt = True
                    try:
                        data = json.loads(pkt.payload.decode('utf-8'))
                    except Exception:
                        data = None
                else:
                    try:
                        data = json.loads(raw.decode('utf-8'))
                    except Exception:
                        data = None

                if not data:
                    continue

                uname = data.get("username")
                token = data.get("token")
                session = self.user_manager.get_user(uname)
                if session and session.token and session.token == token:
                    self.user_manager.update_heartbeat(uname)
                    session.ip = addr[0]
                    reply_dict = {"status": "OK", "username": uname, "users": self.user_manager.get_all_users()}
                else:
                    reply_dict = {"status": "REJECTED", "reason": "Invalid token"}

                reply_bytes = json.dumps(reply_dict).encode('utf-8')
                out_data = UDPPacket(TYPE_HEARTBEAT, 0, 0, 0, payload=reply_bytes).encode() if is_custom_pkt else reply_bytes
                self.udp_sock.sendto(out_data, addr)
            except OSError:
                if not self.running:
                    break
            except Exception:
                pass

    def _broadcast_packet(self, packet, exclude_user=None):
        """Send packet to all authenticated online clients."""
        for session in self.user_manager.get_all_sessions():
            if exclude_user and session.username == exclude_user:
                continue
            try:
                with session.lock:
                    send_packet(session.tcp_sock, packet)
            except Exception:
                pass

    def _send_to_user(self, username, packet):
        """Send unicast packet to a specific user."""
        session = self.user_manager.get_user(username)
        if not session:
            return False
        try:
            with session.lock:
                send_packet(session.tcp_sock, packet)
            return True
        except Exception:
            return False

    def _handle_client(self, sock, addr):
        """Worker thread servicing an individual client connection."""
        sock.settimeout(60.0)
        current_user = None

        try:
            while self.running:
                try:
                    packet = recv_packet(sock)
                except (socket.timeout, ConnectionResetError, BrokenPipeError, EOFError, OSError):
                    break
                except ValueError:
                    break

                msg_type = packet.msg_type

                # 1. TCP Handshake (Step 2)
                if msg_type == MsgType.HANDSHAKE_REQ:
                    ack = Packet.json_payload(MsgType.HANDSHAKE_ACK, {
                        "server": "CO3093-Server",
                        "udp_port": self.udp_port
                    })
                    send_packet(sock, ack)

                # 2. Authentication & Login / Registration with Password (Step 3)
                elif msg_type in (MsgType.AUTH_LOGIN, MsgType.AUTH_REGISTER):
                    login_data = packet.to_json()
                    username = login_data.get("username", "").strip()
                    password = login_data.get("password", "")
                    udp_port = login_data.get("udp_port", 0)
                    is_register = (msg_type == MsgType.AUTH_REGISTER) or bool(login_data.get("is_register", False))

                    if not username:
                        send_packet(sock, Packet.json_payload(MsgType.AUTH_FAILURE, {"reason": "Username cannot be empty"}))
                        continue

                    # Verify or register password (scrypt + salt)
                    auth_ok, auth_msg = self.user_manager.authenticate_or_register(username, password, is_register=is_register)
                    if not auth_ok:
                        send_packet(sock, Packet.json_payload(MsgType.AUTH_FAILURE, {"reason": auth_msg}))
                        continue

                    token = secrets.token_hex(16)
                    session = self.user_manager.add_user(username, sock, addr, udp_port=udp_port, token=token)
                    if not session:
                        send_packet(sock, Packet.json_payload(MsgType.AUTH_FAILURE, {"reason": "Username already online"}))
                    else:
                        current_user = username
                        session.authenticated = True
                        session.token = token
                        self._log(f"[+] {username} joined")
                        send_packet(sock, Packet.json_payload(MsgType.AUTH_SUCCESS, {
                            "username": username,
                            "token": token,
                            "udp_port": self.udp_port,
                            "users": self.user_manager.get_all_users()
                        }))
                        self._broadcast_packet(Packet.json_payload(MsgType.USER_JOINED, {
                            "username": username, "ip": addr[0], "udp_port": udp_port
                        }), exclude_user=username)

                # 3. Broadcast Messaging (Step 5)
                elif msg_type == MsgType.MSG_BROADCAST:
                    if not current_user: continue
                    data = packet.to_json()
                    self._log(f"[Chat] {current_user} -> ALL")
                    data["sender"] = current_user
                    data["timestamp"] = time.time()
                    data["is_broadcast"] = True
                    relay_pkt = Packet.json_payload(MsgType.MSG_BROADCAST, data, flags=packet.flags)
                    self._broadcast_packet(relay_pkt, exclude_user=current_user)
                    send_packet(sock, Packet.json_payload(MsgType.MSG_DELIVERY_ACK, {"status": "DELIVERED", "msg_id": packet.seq}))

                # 4. Unicast Messaging (Step 5)
                elif msg_type == MsgType.MSG_UNICAST:
                    if not current_user: continue
                    data = packet.to_json()
                    target_user = data.get("target")
                    self._log(f"[Chat] {current_user} -> {target_user}")
                    data["sender"] = current_user
                    data["timestamp"] = time.time()
                    data["is_broadcast"] = False
                    relay_pkt = Packet.json_payload(MsgType.MSG_UNICAST, data, flags=packet.flags)
                    success = self._send_to_user(target_user, relay_pkt)
                    ack_status = "DELIVERED" if success else "USER_NOT_FOUND"
                    send_packet(sock, Packet.json_payload(MsgType.MSG_DELIVERY_ACK, {"status": ack_status, "target": target_user}))

                # 5. File Streaming Signaling (Step 6)
                elif msg_type == MsgType.FILE_ANNOUNCE:
                    data = packet.to_json()
                    target_user = data.get("target")
                    data["sender"] = current_user
                    self._log(f"[File] {current_user} -> {target_user}: {data.get('filename', '')}")
                    self._send_to_user(target_user, Packet.json_payload(MsgType.FILE_ANNOUNCE, data))

                elif msg_type == MsgType.FILE_ANNOUNCE_ACK:
                    data = packet.to_json()
                    target_user = data.get("target")
                    data["sender"] = current_user
                    self._send_to_user(target_user, Packet.json_payload(MsgType.FILE_ANNOUNCE_ACK, data))

                # 6. Heartbeat (Step 7)
                elif msg_type == MsgType.HEARTBEAT_PING:
                    if current_user:
                        self.user_manager.update_heartbeat(current_user)
                    send_packet(sock, Packet(MsgType.HEARTBEAT_PONG, payload=b"PONG"))

                # 7. Disconnect (Step 7)
                elif msg_type == MsgType.DISCONNECT:
                    break

        except Exception:
            pass
        finally:
            if current_user:
                removed = self.user_manager.remove_user(current_user)
                if removed:
                    self._log(f"[-] {current_user} left")
                    self._broadcast_packet(Packet.json_payload(MsgType.USER_LEFT, {"username": current_user}))
            try:
                sock.close()
            except Exception:
                pass

    def _heartbeat_watchdog(self):
        """Periodically purge inactive users who fail to send heartbeats."""
        while self.running:
            time.sleep(5.0)
            stale_sessions = self.user_manager.cleanup_inactive_users(timeout_sec=15.0)
            for sess in stale_sessions:
                u = sess.username
                self._log(f"[-] {u} timeout")
                try:
                    sess.tcp_sock.close()
                except Exception:
                    pass
                self._broadcast_packet(Packet.json_payload(MsgType.USER_LEFT, {"username": u, "reason": "Timeout"}))
