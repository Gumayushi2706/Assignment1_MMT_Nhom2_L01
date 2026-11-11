"""
CO3093 - Security Layer (AES-256-GCM AEAD Cryptography).
Implements authenticated payload encryption with 256-bit symmetric keys and 96-bit random nonces.
"""

import os
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# 32-byte (256-bit) shared symmetric key
SHARED_KEY_32_BYTES = b"CO3093_SECURE_COLLAB_KEY_2026_32"
assert len(SHARED_KEY_32_BYTES) == 32


class CryptoManager:
    """Manages symmetric payload encryption and decryption using AES-256-GCM."""
    def __init__(self, key=None):
        self.key = key if key else SHARED_KEY_32_BYTES
        self.aesgcm = AESGCM(self.key)

    def encrypt(self, plaintext):
        """Encrypt bytes using a random 12-byte nonce."""
        nonce = os.urandom(12)
        ciphertext = self.aesgcm.encrypt(nonce, plaintext, None)
        return nonce + ciphertext

    def decrypt(self, encrypted_data):
        """Decrypt bytes by extracting the 12-byte nonce and ciphertext."""
        if len(encrypted_data) < 28:
            raise ValueError("Encrypted data is too short (< 28 bytes)")
        nonce = encrypted_data[:12]
        ciphertext = encrypted_data[12:]
        return self.aesgcm.decrypt(nonce, ciphertext, None)

    def encrypt_string(self, text):
        """Encrypt a text string and return as a Base64-encoded string."""
        raw_bytes = text.encode('utf-8')
        enc_bytes = self.encrypt(raw_bytes)
        return base64.b64encode(enc_bytes).decode('ascii')

    def decrypt_string(self, b64_text):
        """Decrypt a Base64-encoded string back to the original text string."""
        enc_bytes = base64.b64decode(b64_text.encode('ascii'))
        dec_bytes = self.decrypt(enc_bytes)
        return dec_bytes.decode('utf-8')


# Global crypto instance used across client and server
default_crypto = CryptoManager()
