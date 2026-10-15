import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


def receiver(receiver_ip, receiver_port, window_size):
    """Output ordered bytes while acknowledging each accepted DATA packet."""
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    peer = None
    expected = 1
    buffered = {}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((receiver_ip, receiver_port))

        def acknowledge(seq):
            header = PacketHeader(type=3, seq_num=seq, length=0)
            header.checksum = compute_checksum(header)
            sock.sendto(bytes(header), peer)

        while True:
            raw, address = sock.recvfrom(65535)
            if not 16 <= len(raw) <= 1472:
                continue
            header = PacketHeader(raw[:16])
            if header.length != len(raw) - 16:
                continue
            checksum = compute_checksum(raw[:12] + b"\0" * 4 + raw[16:])
            if header.checksum != checksum:
                continue
            if peer is None:
                if header.type == 0 and header.seq_num == 0 and header.length == 0:
                    peer = address
                    acknowledge(1)
                continue
            if address != peer:
                continue
            if header.type == 2:
                seq = header.seq_num
                if seq < 1 or seq >= expected + window_size:
                    continue
                if seq >= expected:
                    buffered.setdefault(seq, raw[16:])
                # Only contiguous data may reach stdout; duplicates never do.
                while expected in buffered:
                    sys.stdout.buffer.write(buffered.pop(expected))
                    expected += 1
                sys.stdout.buffer.flush()
                acknowledge(seq)
            elif header.type == 1 and header.length == 0 and header.seq_num == expected:
                acknowledge(expected + 1)
                return


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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
