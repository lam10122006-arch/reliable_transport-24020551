import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum


MAX_PAYLOAD = 1472 - 16
TIMEOUT = 0.5


def make_packet(kind, seq_num, data=b""):
    header = PacketHeader(type=kind, seq_num=seq_num, length=len(data))
    header.checksum = compute_checksum(header / data)
    return bytes(header / data)


def receive_ack(sock, destination):
    """Ignore malformed/corrupt ACKs and packets from other endpoints."""
    raw, address = sock.recvfrom(65535)
    if address != destination or len(raw) != 16:
        return None
    header = PacketHeader(raw)
    checksum = compute_checksum(raw[:12] + b"\0" * 4)
    if header.type != 3 or header.length != 0 or header.checksum != checksum:
        return None
    return header.seq_num


def sender(receiver_ip, receiver_port, window_size):
    """Send binary stdin using a sliding window and cumulative ACKs."""
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    data = sys.stdin.buffer.read()
    packets = [make_packet(2, i // MAX_PAYLOAD + 1, data[i:i + MAX_PAYLOAD])
               for i in range(0, len(data), MAX_PAYLOAD)]
    destination = (socket.gethostbyname(receiver_ip), receiver_port)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        start = make_packet(0, 0)
        sock.sendto(start, destination)
        deadline = time.monotonic() + TIMEOUT
        # START itself may be lost; its ACK is assumed reliable by the spec.
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                sock.sendto(start, destination)
                deadline = time.monotonic() + TIMEOUT
                continue
            sock.settimeout(remaining)
            try:
                if receive_ack(sock, destination) == 1:
                    break
            except (socket.timeout, ConnectionResetError):
                continue

        base = next_seq = 1
        deadline = time.monotonic() + TIMEOUT
        while base <= len(packets):
            while next_seq <= len(packets) and next_seq < base + window_size:
                sock.sendto(packets[next_seq - 1], destination)
                next_seq += 1
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # No cumulative progress: resend every outstanding packet.
                for seq in range(base, next_seq):
                    sock.sendto(packets[seq - 1], destination)
                deadline = time.monotonic() + TIMEOUT
                continue
            sock.settimeout(remaining)
            try:
                ack = receive_ack(sock, destination)
            except (socket.timeout, ConnectionResetError):
                continue
            if ack is not None and base < ack <= next_seq:
                base = ack
                deadline = time.monotonic() + TIMEOUT

        end_seq = len(packets) + 1
        sock.sendto(make_packet(1, end_seq), destination)
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            try:
                if receive_ack(sock, destination) == end_seq + 1:
                    break
            except (socket.timeout, ConnectionResetError):
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()
    if args.window_size <= 0:
        parser.error("window_size must be positive")

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
