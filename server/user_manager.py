"""
CO3093 - User State & Session Manager.
Maintains thread-safe user records, salted password authentication, and heartbeat tracking.
Complies with Step 3 (Authentication & State Management).
"""

import time
import threading
from protocol.crypto import hash_password, verify_password


class UserSession:
    """Represents an active client connection."""
    def __init__(self, username, tcp_sock, addr, udp_port=0, token=""):
        self.username = username
        self.tcp_sock = tcp_sock
        self.ip = addr[0]
        self.tcp_port = addr[1]
        self.udp_port = udp_port
        self.last_heartbeat = time.time()
        self.authenticated = False
        self.token = token
        self.lock = threading.RLock()

    def to_dict(self):
        """Serialize user information for peer discovery."""
        return {
            "username": self.username,
            "ip": self.ip,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
            "last_active": self.last_heartbeat
        }


class UserManager:
    """Thread-safe registry of registered accounts and active connected users."""
    def __init__(self):
        self._users = {}     # {username: UserSession}
        self._accounts = {}  # {username: {"salt": salt, "hash": hash}}
        self._lock = threading.RLock()

    def register_user(self, username, password=""):
        """Explicitly register a new user account with scrypt + salt."""
        with self._lock:
            if username in self._accounts:
                return False, "Username already registered"
            salt, pwd_hash = hash_password(password or "")
            self._accounts[username] = {"salt": salt, "hash": pwd_hash}
            return True, "Account registered successfully"

    def authenticate_or_register(self, username, password="", is_register=False):
        """Verify existing user password or auto-register new account (scrypt + salt)."""
        with self._lock:
            if is_register:
                return self.register_user(username, password)
            if username in self._accounts:
                rec = self._accounts[username]
                if not verify_password(password or "", rec["salt"], rec["hash"]):
                    return False, "Incorrect password"
                return True, "Login successful"
            return self.register_user(username, password)

    def verify_token(self, username, token):
        """Verify heartbeat/discovery authentication token."""
        with self._lock:
            sess = self._users.get(username)
            if sess and sess.token and sess.token == token:
                return True
            return False

    def add_user(self, username, tcp_sock, addr, udp_port=0, token=""):
        """Register active user session if not already logged in."""
        with self._lock:
            if username in self._users:
                return None
            session = UserSession(username, tcp_sock, addr, udp_port=udp_port, token=token)
            self._users[username] = session
            return session

    def remove_user(self, username):
        """Remove user upon disconnection."""
        with self._lock:
            return self._users.pop(username, None)

    def get_user(self, username):
        """Look up user session by username."""
        with self._lock:
            return self._users.get(username)

    def get_all_users(self):
        """Return serialized info for all authenticated active users."""
        with self._lock:
            return [s.to_dict() for s in self._users.values() if s.authenticated]

    def get_all_sessions(self):
        """Return all active UserSession objects for broadcast."""
        with self._lock:
            return [s for s in self._users.values() if s.authenticated]

    def update_heartbeat(self, username):
        """Update last active timestamp for user."""
        with self._lock:
            user = self._users.get(username)
            if user:
                user.last_heartbeat = time.time()

    def cleanup_inactive_users(self, timeout_sec=15.0):
        """Remove users who failed to send heartbeat within timeout period."""
        now = time.time()
        with self._lock:
            stale = [name for name, s in self._users.items() if now - s.last_heartbeat > timeout_sec]
            return [self._users.pop(name) for name in stale if name in self._users]
