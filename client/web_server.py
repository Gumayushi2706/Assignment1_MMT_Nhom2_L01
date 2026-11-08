"""
NexusStream - Raw Socket HTTP 1.1 Web UI Server (Step 9).
Built purely using standard low-level TCP sockets (no Flask, Django, Tornado, or WebSockets).
Parses HTTP/1.1 requests, serves static HTML/CSS/JS, and provides REST JSON endpoints.
"""

import socket
import threading
import json
import os
import urllib.parse
import mimetypes
import base64
import logging

logger = logging.getLogger("RawWebUIServer")


class RawSocketHTTPServer:
    """Lightweight HTTP 1.1 server implemented directly over raw TCP sockets."""
    def __init__(self, client, host="127.0.0.1", port=8080):
        self.client = client
        self.host = host
        self.port = port
        self.server_sock = None
        self.running = False
        self.static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "web"))

    def start(self):
        """Bind and listen on a free port, automatically incrementing if occupied."""
        for p in range(self.port, self.port + 50):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                if hasattr(socket, 'SO_REUSEADDR'):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self.host, p))
                self.server_sock = sock
                self.port = p
                break
            except OSError:
                sock.close()

        self.server_sock.listen(20)
        self.running = True
        logger.debug(f"Client Web UI available at: http://{self.host}:{self.port}")
        threading.Thread(target=self._accept_loop, daemon=True, name="RawHTTP-Acceptor").start()

    def stop(self):
        """Stop the HTTP server and close socket."""
        self.running = False
        if self.server_sock:
            try: self.server_sock.close()
            except Exception: pass

    def _accept_loop(self):
        """Accept incoming HTTP client connections."""
        while self.running:
            try:
                conn, _ = self.server_sock.accept()
                threading.Thread(target=self._handle_http_request, args=(conn,), daemon=True).start()
            except OSError:
                break

    def _handle_http_request(self, conn):
        """Parse raw HTTP request and route to appropriate GET/POST handlers."""
        conn.settimeout(5.0)
        try:
            req_data = bytearray()
            while b"\r\n\r\n" not in req_data:
                chunk = conn.recv(2048)
                if not chunk: break
                req_data.extend(chunk)

            if not req_data:
                conn.close()
                return

            header_part, _, body_start = req_data.partition(b"\r\n\r\n")
            lines = header_part.decode('utf-8', errors='ignore').split("\r\n")
            req_line = lines[0].split()
            if len(req_line) < 2:
                conn.close()
                return

            method, path = req_line[0].upper(), urllib.parse.urlparse(req_line[1]).path
            headers = {k.strip().lower(): v.strip() for k, v in [line.split(":", 1) for line in lines[1:] if ":" in line]}
            content_length = int(headers.get("content-length", 0))

            body = bytearray(body_start)
            while len(body) < content_length:
                chunk = conn.recv(min(4096, content_length - len(body)))
                if not chunk: break
                body.extend(chunk)

            if method == "GET":
                self._handle_get(conn, path)
            elif method == "POST":
                self._handle_post(conn, path, bytes(body))
            elif method == "OPTIONS":
                self._send_res(conn, 204, "No Content", b"")
            else:
                self._send_res(conn, 405, "Method Not Allowed", b"Method Not Allowed")
        except Exception as e:
            logger.warning(f"HTTP handler error: {e}")
        finally:
            try: conn.close()
            except Exception: pass

    def _send_res(self, conn, code, msg, body, content_type="text/plain"):
        """Send formatted HTTP response headers and body."""
        headers = [
            f"HTTP/1.1 {code} {msg}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body)}",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Methods: GET, POST, OPTIONS",
            "Access-Control-Allow-Headers: Content-Type",
            "Connection: close"
        ]
        conn.sendall(("\r\n".join(headers) + "\r\n\r\n").encode('utf-8') + body)

    def _send_json(self, conn, data, code=200):
        """Helper to send JSON response."""
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self._send_res(conn, code, "OK", body, content_type="application/json; charset=utf-8")

    def _handle_get(self, conn, path):
        """Handle GET requests: state API, file download, and static assets."""
        # State and peer discovery endpoints (supports /api/state and /get-list)
        if path.rstrip("/") in ("/api/state", "/get-list", "/active-peers"):
            with self.client.lock:
                state = {
                    "username": self.client.username,
                    "is_connected": self.client.is_connected,
                    "server_host": self.client.server_host,
                    "server_port": self.client.server_tcp_port,
                    "udp_port": self.client.udp_port,
                    "active_users": self.client.active_users,
                    "messages": self.client.messages[-100:],
                    "transfers": list(self.client.transfers.values())
                }
            self._send_json(conn, state)
            return

        if path.startswith("/downloads/"):
            fname = urllib.parse.unquote(path[len("/downloads/"):])
            fpath = os.path.abspath(os.path.join(self.client.download_dir, fname))
            if os.path.exists(fpath) and os.path.isfile(fpath):
                ctype, _ = mimetypes.guess_type(fpath)
                with open(fpath, "rb") as f:
                    self._send_res(conn, 200, "OK", f.read(), content_type=ctype or "application/octet-stream")
                return
            self._send_res(conn, 404, "Not Found", b"File not found")
            return

        fname = "index.html" if path in ("/", "") else path.lstrip("/")
        fpath = os.path.abspath(os.path.join(self.static_dir, fname))
        if fpath.startswith(self.static_dir) and os.path.exists(fpath) and os.path.isfile(fpath):
            ctype, _ = mimetypes.guess_type(fpath)
            with open(fpath, "rb") as f:
                self._send_res(conn, 200, "OK", f.read(), content_type=ctype or "text/html; charset=utf-8")
        else:
            self._send_res(conn, 404, "Not Found", b"Not Found")

    def _handle_post(self, conn, path, body):
        """Handle POST requests: connect, disconnect, message send, and file upload."""
        try:
            data = json.loads(body.decode('utf-8')) if body else {}
            ep = path.rstrip("/")

            if ep in ("/api/connect", "/login", "/connect-peer", "/submit-info"):
                uname = (data.get("username") or data.get("user") or "").strip()
                host = data.get("host") or data.get("server_ip") or "127.0.0.1"
                port = int(data.get("port") or data.get("server_port") or 8888)
                ok, msg = self.client.connect_and_login(uname, host, port)
                self._send_json(conn, {"success": ok, "message": msg})

            elif ep in ("/api/disconnect", "/logout"):
                self.client.disconnect()
                self._send_json(conn, {"success": True, "message": "Disconnected"})

            elif ep in ("/api/send_message", "/broadcast-peer", "/send-peer"):
                target = data.get("target") or data.get("receiver") or ("ALL" if "broadcast" in ep else "ALL")
                text = (data.get("text") or data.get("message") or data.get("body") or "").strip()
                if not text:
                    self._send_json(conn, {"success": False, "message": "Empty message"}, 400)
                    return
                ok = self.client.send_broadcast_message(text) if target == "ALL" else self.client.send_unicast_message(target, text)
                self._send_json(conn, {"success": ok})

            elif ep in ("/api/upload_and_send", "/send-file"):
                target = data.get("target") or data.get("receiver")
                filename = data.get("filename", "upload.bin")
                file_bytes = base64.b64decode(data.get("content_b64", ""))
                ok, msg = self.client.send_file(target, file_bytes=file_bytes, filename=filename)
                self._send_json(conn, {"success": ok, "message": msg})

            else:
                self._send_res(conn, 404, "Not Found", b"API endpoint not found")
        except Exception as e:
            self._send_json(conn, {"success": False, "error": str(e)}, 500)
