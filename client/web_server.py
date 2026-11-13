"""
CO3093 - Raw Socket HTTP 1.1 Web UI Server (Step 9).
Built purely using standard low-level TCP sockets (no Flask, Django, or WebSockets).
"""

import socket
import threading
import time
import json
import os
import urllib.parse
import mimetypes
import base64
import sys
import logging

logger = logging.getLogger("RawWebUIServer")


class RawSocketHTTPServer:
    """Lightweight HTTP 1.1 server implemented directly over raw TCP sockets."""
    def __init__(self, client, host="127.0.0.1", port=5001):
        self.client = client
        self.host = host
        self.port = port
        self.server_sock = None
        self.running = False
        self.static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "web"))

    def start(self):
        """Bind and listen on a free port, auto-incrementing if occupied."""
        for p in range(self.port, self.port + 50):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                if sys.platform == "win32" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                elif hasattr(socket, "SO_REUSEADDR"):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self.host, p))
                self.server_sock = sock
                self.port = p
                break
            except OSError:
                sock.close()

        if not self.server_sock:
            raise RuntimeError(f"Could not bind to any port in range {self.port} to {self.port + 50}")

        self.server_sock.listen(20)
        self.running = True
        logger.debug(f"Web UI: http://{self.host}:{self.port}")
        threading.Thread(target=self._accept_loop, daemon=True, name="RawHTTP-Acceptor").start()

    def stop(self):
        """Stop HTTP server and close socket."""
        self.running = False
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass

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
                if not chunk:
                    break
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

            method = req_line[0].upper()
            parsed_url = urllib.parse.urlparse(req_line[1])
            path = parsed_url.path
            headers = {k.strip().lower(): v.strip() for k, v in [line.split(":", 1) for line in lines[1:] if ":" in line]}
            content_length = int(headers.get("content-length", 0))

            body = bytearray(body_start)
            while len(body) < content_length:
                chunk = conn.recv(min(65536, content_length - len(body)))
                if not chunk:
                    break
                body.extend(chunk)

            if method == "GET":
                self._handle_get(conn, path)
            elif method in ("POST", "PUT"):
                self._handle_post(conn, path, bytes(body))
            elif method == "OPTIONS":
                self._send_res(conn, 204, "No Content", b"")
            else:
                self._send_res(conn, 405, "Method Not Allowed", b"Method Not Allowed")
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _send_res(self, conn, code, msg, body, content_type="text/plain", extra_headers=None, **kwargs):
        """Send formatted HTTP response."""
        headers = [
            f"HTTP/1.1 {code} {msg}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body)}",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Methods: GET, POST, OPTIONS",
            "Access-Control-Allow-Headers: Content-Type",
            "Connection: close"
        ]
        if extra_headers:
            headers.extend(extra_headers)
        conn.sendall(("\r\n".join(headers) + "\r\n\r\n").encode('utf-8') + body)

    def _send_json(self, conn, data, code=200, extra_headers=None, **kwargs):
        """Helper to send JSON response."""
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self._send_res(conn, code, "OK", body, content_type="application/json; charset=utf-8", extra_headers=extra_headers, **kwargs)

    def _handle_get(self, conn, path):
        """Handle GET requests for state API, file download, and static assets."""
        normalized_path = path.rstrip("/")

        # State API (polling)
        if normalized_path in ("/api/state", "/state", "/get-list", "/add-list"):
            with self.client.lock:
                is_conn = self.client.is_connected
                state = {
                    "username": self.client.username if is_conn else "",
                    "is_connected": is_conn,
                    "server_host": self.client.server_host,
                    "server_port": self.client.server_tcp_port,
                    "udp_port": self.client.udp_port if is_conn else 0,
                    "active_users": self.client.active_users if is_conn else [],
                    "messages": self.client.messages[-100:] if is_conn else [],
                    "transfers": list(self.client.transfers.values()) if is_conn else []
                }
            self._send_json(conn, state)
            return

        # Download received files (with path traversal guard)
        if path.startswith("/downloads/"):
            fname = urllib.parse.unquote(path[len("/downloads/"):])
            fpath = os.path.abspath(os.path.join(self.client.download_dir, fname))
            if fpath.startswith(self.client.download_dir) and os.path.exists(fpath) and os.path.isfile(fpath):
                ctype, _ = mimetypes.guess_type(fpath)
                with open(fpath, "rb") as f:
                    self._send_res(conn, 200, "OK", f.read(), content_type=ctype or "application/octet-stream")
                return
            self._send_res(conn, 404, "Not Found", b"File not found")
            return

        # Static assets (HTML, CSS, JS, logo)
        fname = "index.html" if path in ("/", "") else path.lstrip("/")
        fpath = os.path.abspath(os.path.join(self.static_dir, fname))
        if fpath.startswith(self.static_dir) and os.path.exists(fpath) and os.path.isfile(fpath):
            ctype, _ = mimetypes.guess_type(fpath)
            with open(fpath, "rb") as f:
                self._send_res(conn, 200, "OK", f.read(), content_type=ctype or "text/html; charset=utf-8")
        else:
            self._send_res(conn, 404, "Not Found", b"Not Found")

    def _handle_post(self, conn, path, body):
        """Handle POST requests for login, disconnect, chat, and file transfer."""
        try:
            data = json.loads(body.decode('utf-8')) if body else {}
            ep = path.rstrip("/")

            # Login or Register with password (scrypt + salt)
            if ep in ("/api/connect", "/login", "/register", "/api/register", "/submit-info", "/connect-peer"):
                uname = (data.get("username") or data.get("user") or "").strip()
                pwd = data.get("password", "")
                host = data.get("host") or data.get("server_ip") or "127.0.0.1"
                port = int(data.get("port") or data.get("server_port") or 8888)
                is_register = bool(data.get("is_register", False)) or ep in ("/register", "/api/register")

                if not uname:
                    self._send_json(conn, {"success": False, "message": "Username cannot be empty"}, 400)
                    return

                if self.client.is_connected:
                    if self.client.username == uname:
                        self._send_json(conn, {"success": True, "message": "Already connected", "username": uname})
                        return
                    self.client.disconnect()
                    time.sleep(0.1)

                ok, msg = self.client.connect_and_login(uname, password=pwd, host=host, port=port, is_register=is_register)
                extra = [f"Set-Cookie: session_user={uname}; Path=/"] if ok else None
                code = 200 if ok else 400
                self._send_json(conn, {"success": ok, "message": msg, "username": uname}, code=code, extra_headers=extra)

            # Disconnect
            elif ep in ("/api/disconnect", "/logout"):
                self.client.disconnect()
                extra = ["Set-Cookie: session_user=; Path=/; Max-Age=0"]
                self._send_json(conn, {"success": True, "message": "Disconnected"}, extra_headers=extra)

            # Send message
            elif ep in ("/api/send_message", "/send", "/broadcast-peer", "/send-peer"):
                is_broadcast = (ep == "/broadcast-peer") or data.get("target") == "ALL"
                target = "ALL" if is_broadcast else data.get("target", "ALL")
                text = (data.get("text") or data.get("message") or "").strip()
                if not text:
                    self._send_json(conn, {"success": False, "message": "Empty message"}, 400)
                    return
                ok = self.client.send_broadcast_message(text) if target == "ALL" else self.client.send_unicast_message(target, text)
                self._send_json(conn, {"success": ok})

            # Upload & send file
            elif ep in ("/api/upload_and_send", "/send-file"):
                target = data.get("target")
                raw_filename = data.get("filename", "upload.bin")
                filename = os.path.basename(raw_filename) or "upload.bin"
                content_b64 = data.get("content_b64", "")
                file_bytes = base64.b64decode(content_b64) if content_b64 else None
                file_path = data.get("file_path")

                ok, msg = self.client.send_file(target, file_path=file_path, file_bytes=file_bytes, filename=filename)
                self._send_json(conn, {"success": ok, "message": msg})

            else:
                self._send_res(conn, 404, "Not Found", b"API endpoint not found")
        except Exception as e:
            self._send_json(conn, {"success": False, "error": str(e)}, 500)
