"""Protocol checks using real UDP sockets, runnable on Windows or Linux."""

import binascii
import heapq
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def packet(kind, seq, payload=b""):
    header = struct.pack("!IIII", kind, seq, len(payload), 0)
    checksum = binascii.crc32(header + payload) & 0xFFFFFFFF
    return struct.pack("!IIII", kind, seq, len(payload), checksum) + payload


def unpack(raw):
    kind, seq, length, checksum = struct.unpack("!IIII", raw[:16])
    assert len(raw) == 16 + length
    assert checksum == binascii.crc32(raw[:12] + b"\0" * 4 + raw[16:]) & 0xFFFFFFFF
    return kind, seq, raw[16:]


class TransportTests(unittest.TestCase):
    def endpoint(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        sock.settimeout(2)
        self.addCleanup(sock.close)
        return sock

    def launch(self, folder, role, port, window, stdout=subprocess.PIPE):
        proc = subprocess.Popen(
            [sys.executable, str(ROOT / folder / (role + ".py")),
             "127.0.0.1", str(port), str(window)],
            stdin=subprocess.PIPE, stdout=stdout, stderr=subprocess.PIPE,
        )
        def cleanup():
            if proc.poll() is None:
                proc.kill()
            proc.communicate()
        self.addCleanup(cleanup)
        return proc

    def start_receiver(self, folder, window):
        probe = self.endpoint()
        port = probe.getsockname()[1]
        probe.close()
        proc = self.launch(folder, "receiver", port, window)
        peer = self.endpoint()
        address = ("127.0.0.1", port)
        deadline = time.monotonic() + 5
        peer.settimeout(0.1)
        while time.monotonic() < deadline:
            peer.sendto(packet(0, 0), address)
            try:
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (3, 1, b""))
                peer.settimeout(2)
                return proc, peer, address
            except (socket.timeout, ConnectionResetError):
                if proc.poll() is not None:
                    self.fail(proc.communicate()[1].decode(errors="replace"))
        self.fail("Receiver did not acknowledge START")

    def test_receiver_buffer_checksum_window_and_duplicates(self):
        for folder in ("RTP-base", "RTP-opt"):
            with self.subTest(folder=folder):
                proc, peer, addr = self.start_receiver(folder, 3)
                def send(seq, data, ack):
                    peer.sendto(packet(2, seq, data), addr)
                    self.assertEqual(unpack(peer.recvfrom(2048)[0]), (3, ack, b""))
                # Malformed, corrupt, out-of-window, and new START are ignored.
                bad = bytearray(packet(2, 1, b"bad"))
                bad[-1] ^= 1
                malformed = struct.pack("!IIII", 2, 1, 99, 0) + b"X"
                crc = binascii.crc32(malformed) & 0xFFFFFFFF
                malformed = malformed[:12] + struct.pack("!I", crc) + malformed[16:]
                for raw in (b"short", bytes(bad), malformed,
                            packet(2, 1, b"X" * 1457), packet(9, 1),
                            packet(2, 4, b"outside"), packet(0, 0), packet(1, 4)):
                    peer.sendto(raw, addr)
                    peer.settimeout(0.12)
                    with self.assertRaises(socket.timeout):
                        peer.recvfrom(2048)
                peer.settimeout(2)
                stranger = self.endpoint()
                stranger.sendto(packet(2, 1, b"not the peer"), addr)
                stranger.settimeout(0.12)
                with self.assertRaises(socket.timeout):
                    stranger.recvfrom(2048)
                send(2, b"B", 1 if folder == "RTP-base" else 2)
                send(2, b"B", 1 if folder == "RTP-base" else 2)
                send(1, b"A", 3 if folder == "RTP-base" else 1)
                send(1, b"A", 3 if folder == "RTP-base" else 1)
                send(3, b"\x00\xff\n", 4 if folder == "RTP-base" else 3)
                peer.sendto(packet(1, 4), addr)
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (3, 5, b""))
                out, err = proc.communicate(timeout=3)
                self.assertEqual(proc.returncode, 0, err)
                self.assertEqual(out, b"AB\x00\xff\n")

    def test_sender_window_ack_and_retransmission(self):
        for folder in ("RTP-base", "RTP-opt"):
            with self.subTest(folder=folder):
                peer = self.endpoint()
                proc = self.launch(folder, "sender", peer.getsockname()[1], 3)
                data = bytes(range(256)) * 30
                proc.stdin.write(data)
                proc.stdin.close()
                proc.stdin = None
                raw, addr = peer.recvfrom(2048)
                self.assertEqual(unpack(raw), (0, 0, b""))
                peer.sendto(packet(3, 1), addr)
                initial = [unpack(peer.recvfrom(2048)[0]) for _ in range(3)]
                self.assertEqual([p[1] for p in initial], [1, 2, 3])
                self.assertTrue(all(len(p[2]) <= 1456 for p in initial))
                received = {seq: payload for _, seq, payload in initial}
                # A corrupt future ACK must never advance the sender's window.
                bad_ack = bytearray(packet(3, 4))
                bad_ack[-1] ^= 1
                peer.sendto(bad_ack, addr)
                peer.sendto(packet(3, 999), addr)
                waiting_since = time.monotonic()
                if folder == "RTP-opt":
                    peer.sendto(packet(3, 3), addr)
                    peer.sendto(packet(3, 2), addr)
                    peer.sendto(packet(3, 2), addr)
                else:
                    peer.sendto(packet(3, 1), addr)
                expected = [1, 2, 3] if folder == "RTP-base" else [1]
                resent = [unpack(peer.recvfrom(2048)[0])[1] for _ in expected]
                self.assertEqual(resent, expected)
                self.assertGreaterEqual(time.monotonic() - waiting_since, 0.4)
                peer.sendto(packet(3, 4 if folder == "RTP-base" else 1), addr)
                next_expected = 4
                while True:
                    kind, seq, payload = unpack(peer.recvfrom(2048)[0])
                    if kind == 1:
                        # Drop END ACK: sender must still terminate after 500 ms.
                        end_since = time.monotonic()
                        break
                    self.assertEqual(kind, 2)
                    received[seq] = payload
                    while next_expected in received:
                        next_expected += 1
                    peer.sendto(packet(3, next_expected if folder == "RTP-base" else seq), addr)
                out, err = proc.communicate(timeout=2)
                self.assertEqual(proc.returncode, 0, err)
                self.assertGreaterEqual(time.monotonic() - end_since, 0.4)
                self.assertLess(time.monotonic() - end_since, 1.5)
                self.assertEqual(b"".join(received[k] for k in sorted(received)), data)

    def test_timer_resets_on_progress_not_duplicate_acks(self):
        for folder in ("RTP-base", "RTP-opt"):
            with self.subTest(folder=folder):
                peer = self.endpoint()
                proc = self.launch(folder, "sender", peer.getsockname()[1], 1)
                proc.stdin.write(b"X" * 1456 + b"Y")
                proc.stdin.close()
                proc.stdin = None
                raw, addr = peer.recvfrom(2048)
                self.assertEqual(unpack(raw), (0, 0, b""))
                peer.sendto(packet(3, 1), addr)
                self.assertEqual(unpack(peer.recvfrom(2048)[0])[1], 1)
                time.sleep(0.3)
                advance = 2 if folder == "RTP-base" else 1
                peer.sendto(packet(3, advance), addr)
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (2, 2, b"Y"))
                since = time.monotonic()
                time.sleep(0.3)
                peer.sendto(packet(3, advance), addr)
                # The second DATA must time out around 500 ms after progress,
                # not at the original deadline or 500 ms after the duplicate.
                peer.settimeout(0.35)
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (2, 2, b"Y"))
                self.assertGreaterEqual(time.monotonic() - since, 0.4)
                self.assertLess(time.monotonic() - since, 0.7)
                peer.sendto(packet(3, 3 if folder == "RTP-base" else 2), addr)
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (1, 3, b""))
                peer.sendto(packet(3, 4), addr)
                _, err = proc.communicate(timeout=2)
                self.assertEqual(proc.returncode, 0, err)

    def test_invalid_window_is_rejected(self):
        for folder in ("RTP-base", "RTP-opt"):
            for role in ("sender", "receiver"):
                with self.subTest(folder=folder, role=role):
                    proc = self.launch(folder, role, 40000, 0)
                    _, err = proc.communicate(timeout=5)
                    self.assertEqual(proc.returncode, 2)
                    self.assertIn(b"window_size must be positive", err)

    def test_sender_retries_lost_start(self):
        for folder in ("RTP-base", "RTP-opt"):
            with self.subTest(folder=folder):
                peer = self.endpoint()
                proc = self.launch(folder, "sender", peer.getsockname()[1], 1)
                proc.stdin.close()
                proc.stdin = None
                raw, addr = peer.recvfrom(2048)
                self.assertEqual(unpack(raw), (0, 0, b""))
                since = time.monotonic()
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (0, 0, b""))
                self.assertGreaterEqual(time.monotonic() - since, 0.4)
                peer.sendto(packet(3, 1), addr)
                self.assertEqual(unpack(peer.recvfrom(2048)[0]), (1, 1, b""))
                peer.sendto(packet(3, 2), addr)
                _, err = proc.communicate(timeout=2)
                self.assertEqual(proc.returncode, 0, err)

    def test_end_to_end(self):
        for folder in ("RTP-base", "RTP-opt"):
            for window, data in ((1, b""), (1, b"hello\n"),
                                 (3, b"X" * 1456), (3, b"Y" * 1457),
                                 (3, b"Z" * 2912),
                                 (3, bytes(range(256)) * 40),
                                 (128, (ROOT / "test_scripts/test_message.txt").read_bytes())):
                with self.subTest(folder=folder, window=window, size=len(data)):
                    receiver, peer, addr = self.start_receiver(folder, window)
                    # START already established by the probe; use a fresh receiver
                    # for the actual sender so it owns the complete connection.
                    peer.sendto(packet(1, 1), addr)
                    receiver.communicate(timeout=2)
                    output = tempfile.TemporaryFile()
                    self.addCleanup(output.close)
                    receiver = self.launch(folder, "receiver", addr[1], window, stdout=output)
                    time.sleep(0.3)
                    sender = self.launch(folder, "sender", addr[1], window)
                    _, err = sender.communicate(data, timeout=15)
                    self.assertEqual(sender.returncode, 0, err)
                    _, err = receiver.communicate(timeout=3)
                    self.assertEqual(receiver.returncode, 0, err)
                    output.seek(0)
                    self.assertEqual(output.read(), data)

    def test_end_to_end_with_network_faults(self):
        cases = ((folder, window) for folder in ("RTP-base", "RTP-opt")
                 for window in (1, 3, 128))
        for folder, window in cases:
            with self.subTest(folder=folder, window=window):
                receiver, peer, addr = self.start_receiver(folder, window)
                peer.sendto(packet(1, 1), addr)
                receiver.communicate(timeout=2)
                output = tempfile.TemporaryFile()
                self.addCleanup(output.close)
                receiver = self.launch(folder, "receiver", addr[1], window, stdout=output)
                proxy = self.endpoint()
                stop = threading.Event()
                errors = []
                faulted = set()

                def relay():
                    sender_addr = None
                    pending = []
                    counter = 0
                    dropped_data = dropped_acks = 0
                    proxy.settimeout(0.01)
                    try:
                        while not stop.is_set():
                            now = time.monotonic()
                            while pending and pending[0][0] <= now:
                                _, _, raw, target = heapq.heappop(pending)
                                proxy.sendto(raw, target)
                            try:
                                raw, source = proxy.recvfrom(65535)
                            except (socket.timeout, ConnectionResetError):
                                continue
                            kind, seq, _ = unpack(raw)
                            if source == addr:
                                target = sender_addr
                            else:
                                sender_addr = source
                                target = addr
                            if kind == 2 and seq == 1 and dropped_data < 2:
                                dropped_data += 1
                                faulted.add("drop_data")
                                continue
                            if kind == 2 and seq == 2 and "corrupt" not in faulted:
                                faulted.add("corrupt")
                                raw = raw[:-1] + bytes([raw[-1] ^ 1])
                            if kind == 3 and "drop_data" in faulted and dropped_acks < 2:
                                dropped_acks += 1
                                faulted.add("drop_ack")
                                continue
                            # Different delays reorder both DATA and ACKs.
                            delay = 0.08 if seq % 3 == 0 else 0.01
                            if kind == 3 and "drop_ack" in faulted and "delay_ack" not in faulted:
                                delay = 0.65
                                faulted.add("delay_ack")
                            counter += 1
                            heapq.heappush(pending, (now + delay, counter, raw, target))
                            if kind == 2 or (kind == 3 and "drop_data" in faulted):
                                counter += 1
                                heapq.heappush(pending, (now + delay + 0.02, counter, raw, target))
                    except Exception as exc:
                        errors.append(exc)

                thread = threading.Thread(target=relay, daemon=True)
                thread.start()
                data = bytes(range(256)) * 60
                sender = self.launch(folder, "sender", proxy.getsockname()[1], window)
                try:
                    _, err = sender.communicate(data, timeout=20)
                    self.assertEqual(sender.returncode, 0, err)
                    _, err = receiver.communicate(timeout=3)
                    self.assertEqual(receiver.returncode, 0, err)
                finally:
                    stop.set()
                    thread.join(timeout=2)
                self.assertFalse(errors)
                self.assertEqual(faulted, {"drop_data", "corrupt", "drop_ack", "delay_ack"})
                output.seek(0)
                self.assertEqual(output.read(), data)

    @unittest.expectedFailure
    def test_opt_duplicate_start_ack_known_spec_ambiguity(self):
        """Track the unresolved identical START/DATA-1 ACK wire formats."""
        peer = self.endpoint()
        proc = self.launch("RTP-opt", "sender", peer.getsockname()[1], 1)
        proc.stdin.write(b"lost data")
        proc.stdin.close()
        proc.stdin = None
        raw, addr = peer.recvfrom(2048)
        self.assertEqual(unpack(raw), (0, 0, b""))
        peer.sendto(packet(3, 1), addr)
        self.assertEqual(unpack(peer.recvfrom(2048)[0]), (2, 1, b"lost data"))
        # DATA was dropped. This is a duplicate START ACK, not a DATA ACK,
        # but there is no phase identifier in the specified packet header.
        peer.sendto(packet(3, 1), addr)
        self.assertEqual(unpack(peer.recvfrom(2048)[0]), (2, 1, b"lost data"))


if __name__ == "__main__":
    unittest.main()
