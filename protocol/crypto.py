"""
CO3093 - Security Layer (AES-256-GCM AEAD & Salted Password Hashing).
Complies with Rubric Step 3 (Password Auth) and Step 8 (Payload Encryption).
"""

import os
import base64
import hashlib
import secrets
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# 32-byte (256-bit) shared symmetric key for payload encryption
SHARED_KEY_32_BYTES = b"CO3093_SECURE_COLLAB_KEY_2026_32"
assert len(SHARED_KEY_32_BYTES) == 32


class CryptoManager:
    """Manages symmetric payload encryption and decryption using AES-256-GCM."""
    def __init__(self, key=None):
        self.key = key if key else SHARED_KEY_32_BYTES
        self.aesgcm = AESGCM(self.key)

    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt bytes using a random 12-byte nonce (AES-GCM AEAD)."""
        nonce = os.urandom(12)
        ciphertext = self.aesgcm.encrypt(nonce, plaintext, None)
        return nonce + ciphertext

    def decrypt(self, encrypted_data: bytes) -> bytes:
        """Decrypt bytes by extracting the 12-byte nonce and ciphertext."""
        if len(encrypted_data) < 28:
            raise ValueError("Encrypted data is too short (< 28 bytes)")
        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        return self.aesgcm.decrypt(nonce, ciphertext, None)

    def encrypt_string(self, text: str) -> str:
        """Encrypt a text string and return as Base64."""
        raw_bytes = text.encode('utf-8')
        enc_bytes = self.encrypt(raw_bytes)
        return base64.b64encode(enc_bytes).decode('ascii')

    def decrypt_string(self, b64_text: str) -> str:
        """Decrypt a Base64 string back to plaintext."""
        enc_bytes = base64.b64decode(b64_text.encode('ascii'))
        dec_bytes = self.decrypt(enc_bytes)
        return dec_bytes.decode('utf-8')


# Global shared crypto instance used across client and server
default_crypto = CryptoManager()


def hash_password(password: str, salt: str = None) -> tuple[str, str]:
    """Salted scrypt password hashing (Step 3 / RFC 7914)."""
    salt_val = salt if salt else secrets.token_hex(16)
    salt_bytes = bytes.fromhex(salt_val)
    # Using standard scrypt parameters: N=16384, r=8, p=1, dklen=32
    hashed = hashlib.scrypt(
        password.encode('utf-8'),
        salt=salt_bytes,
        n=16384,
        r=8,
        p=1,
        maxmem=0,
        dklen=32
    ).hex()
    return salt_val, hashed


def verify_password(password: str, salt: str, expected_hash: str) -> bool:
    """Verify password against stored salt and expected hash using scrypt."""
    _, hashed = hash_password(password, salt=salt)
    return hashed == expected_hash
