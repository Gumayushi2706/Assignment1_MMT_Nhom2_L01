"""
CO3093 - Reliable UDP Streaming Protocol.
Selective Repeat ARQ with sliding window flow control (N=8) and chunk sizes bounded below MTU (1200 bytes).
Fixed 24-byte Header: Magic (2B) | Type (1B) | Flags (1B) | StreamID (4B) | ChunkIdx (4B) | TotalChunks (4B) | Len (2B) | Reserved (2B) | CRC32 (4B)
"""

import struct
import zlib
import time
import threading
from protocol.crypto import default_crypto

UDP_MAGIC = 0x5546           # 'UF' (UDP Framing)
CHUNK_PAYLOAD_SIZE = 1200   # Bounded below 1500-byte Ethernet MTU to prevent IP fragmentation
WINDOW_SIZE = 8             # Sliding window size N
RETRANSMIT_TIMEOUT = 0.25   # Retransmission timeout (250ms)
MAX_CHUNK_RETRANSMITS = 30  # Max consecutive retransmission attempts before failing

UDP_HEADER_FORMAT = "!HBBIIIH2sI"
UDP_HEADER_SIZE = struct.calcsize(UDP_HEADER_FORMAT)
assert UDP_HEADER_SIZE == 24

TYPE_DATA = 1   # Chunk payload segment
TYPE_ACK = 2    # Selective acknowledgment segment

FLAG_NONE = 0x00
FLAG_ENCRYPTED = 0x01


class UDPPacket:
    """Binary UDP packet with a fixed 24-byte header."""
    def __init__(self, pkt_type, stream_id, chunk_idx, total_chunks, payload=b"", flags=FLAG_NONE):
        self.magic = UDP_MAGIC
        self.pkt_type = pkt_type
        self.flags = flags
        self.stream_id = stream_id
        self.chunk_idx = chunk_idx
        self.total_chunks = total_chunks
        self.payload = payload
        self.data_len = len(payload)
        self.checksum = zlib.crc32(payload) & 0xFFFFFFFF

    def encode(self):
        """Serialize UDP header and payload into bytes."""
        hdr = struct.pack(
            UDP_HEADER_FORMAT, self.magic, self.pkt_type, self.flags,
            self.stream_id, self.chunk_idx, self.total_chunks,
            self.data_len, b"\x00\x00", self.checksum
        )
        return hdr + self.payload

    @classmethod
    def decode(cls, raw):
        """Decode raw bytes into a UDPPacket and verify CRC32."""
        if len(raw) < UDP_HEADER_SIZE:
            return None
        magic, p_type, flags, s_id, c_idx, tot, d_len, _, checksum = struct.unpack(
            UDP_HEADER_FORMAT, raw[:UDP_HEADER_SIZE]
        )
        if magic != UDP_MAGIC:
            return None
        payload = raw[UDP_HEADER_SIZE:UDP_HEADER_SIZE + d_len]
        if len(payload) != d_len or (zlib.crc32(payload) & 0xFFFFFFFF) != checksum:
            return None
        return cls(p_type, s_id, c_idx, tot, payload, flags)


class StreamSender:
    """Sends binary data reliably over UDP using Selective Repeat ARQ."""
    def __init__(self, sock, target_addr, stream_id, raw_data, encrypt=True, on_progress=None):
        self.sock = sock
        self.target_addr = target_addr
        self.stream_id = stream_id
        self.on_progress = on_progress

        # Chunk and optionally encrypt payloads
        self.chunks = []
        for i in range(0, max(1, len(raw_data)), CHUNK_PAYLOAD_SIZE):
            part = raw_data[i:i + CHUNK_PAYLOAD_SIZE]
            if encrypt:
                self.chunks.append((default_crypto.encrypt(part), FLAG_ENCRYPTED))
            else:
                self.chunks.append((part, FLAG_NONE))
        self.total_chunks = len(self.chunks)

        self.base = 0
        self.next_seq = 0
        self.acked = set()
        self.send_times = {}
        self.retransmit_counts = {}
        self.lock = threading.RLock()
        self.running = True
        self.finished = False
        self.failed = False

    def cancel(self):
        """Immediately abort sending loop."""
        with self.lock:
            self.running = False

    def handle_ack(self, ack_idx):
        """Process incoming ACK and advance sliding window base."""
        with self.lock:
            if ack_idx not in self.acked and 0 <= ack_idx < self.total_chunks:
                self.acked.add(ack_idx)
                while self.base in self.acked and self.base < self.total_chunks:
                    self.base += 1
                if self.on_progress:
                    pct = (len(self.acked) / self.total_chunks) * 100.0
                    self.on_progress(len(self.acked), self.total_chunks, pct)
                if len(self.acked) >= self.total_chunks:
                    self.finished = True

    def send_loop(self):
        """Dispatch chunks within window and retransmit timed-out segments."""
        while self.running and not self.finished:
            now = time.time()
            to_send = []

            with self.lock:
                # Dispatch new chunks in sliding window [base, base + WINDOW_SIZE)
                while self.next_seq < min(self.base + WINDOW_SIZE, self.total_chunks):
                    to_send.append(self.next_seq)
                    self.send_times[self.next_seq] = now
                    self.next_seq += 1

                # Retransmit unacknowledged chunks exceeding timeout
                for idx in range(self.base, min(self.next_seq, self.total_chunks)):
                    if idx not in self.acked and (now - self.send_times.get(idx, 0) > RETRANSMIT_TIMEOUT):
                        retries = self.retransmit_counts.get(idx, 0) + 1
                        if retries > MAX_CHUNK_RETRANSMITS:
                            self.failed = True
                            self.running = False
                            break
                        self.retransmit_counts[idx] = retries
                        to_send.append(idx)
                        self.send_times[idx] = now

            for idx in to_send:
                payload, flags = self.chunks[idx]
                pkt = UDPPacket(TYPE_DATA, self.stream_id, idx, self.total_chunks, payload, flags)
                try:
                    self.sock.sendto(pkt.encode(), self.target_addr)
                except Exception:
                    pass

            time.sleep(0.005)


class StreamReceiver:
    """Receives, decrypts, and reassembles out-of-order UDP chunks."""
    def __init__(self, stream_id, total_chunks, on_progress=None):
        self.stream_id = stream_id
        self.total_chunks = total_chunks
        self.received_chunks = {}
        self.on_progress = on_progress
        self.lock = threading.RLock()

    def put_chunk(self, chunk_idx, payload, is_encrypted):
        """Store chunk, decrypt if encrypted, and update progress."""
        with self.lock:
            if chunk_idx in self.received_chunks:
                return False
            data = default_crypto.decrypt(payload) if is_encrypted else payload
            self.received_chunks[chunk_idx] = data

            if self.on_progress:
                pct = (len(self.received_chunks) / self.total_chunks) * 100.0
                self.on_progress(len(self.received_chunks), self.total_chunks, pct)
            return True

    def is_complete(self):
        """Check if all chunks have been received."""
        with self.lock:
            return len(self.received_chunks) >= self.total_chunks

    def reassemble(self):
        """Assemble all chunks into contiguous bytes in sequential order."""
        with self.lock:
            if not self.is_complete():
                raise ValueError("Stream incomplete: missing chunks")
            file_data = bytearray()
            for i in range(self.total_chunks):
                file_data.extend(self.received_chunks[i])
            return bytes(file_data)
