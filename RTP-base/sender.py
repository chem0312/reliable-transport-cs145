
import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16
MAX_PACKET_SIZE = 1472
MAX_PAYLOAD = MAX_PACKET_SIZE - HEADER_SIZE

START = 0
END = 1
DATA = 2
ACK = 3

TIMEOUT = 0.5


def build_packet(packet_type, seq_num, payload=b""):
    header = PacketHeader(
        type=packet_type,
        seq_num=seq_num,
        length=len(payload),
        checksum=0,
    )
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def decode_packet(raw):
    if len(raw) < HEADER_SIZE:
        return None

    header = PacketHeader(raw[:HEADER_SIZE])
    payload = raw[HEADER_SIZE:]

    if header.length != len(payload):
        return None

    received_checksum = header.checksum
    header.checksum = 0

    if compute_checksum(header / payload) != received_checksum:
        return None

    header.checksum = received_checksum
    return header, payload


def sender(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")

    destination = (receiver_ip, receiver_port)
    data = sys.stdin.buffer.read()

    chunks = [
        data[i:i + MAX_PAYLOAD]
        for i in range(0, len(data), MAX_PAYLOAD)
    ]

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT)

    try:
        # 1. Start the connection.
        start_packet = build_packet(START, 0)

        while True:
            sock.sendto(start_packet, destination)

            try:
                raw, _ = sock.recvfrom(2048)
            except socket.timeout:
                continue

            result = decode_packet(raw)
            if result is None:
                continue

            header, _ = result
            if header.type == ACK and header.seq_num == 1:
                break

        # 2. Prepare DATA packets. Their sequence numbers start at 1.
        packets = {
            i + 1: build_packet(DATA, i + 1, chunk)
            for i, chunk in enumerate(chunks)
        }

        base = 1
        next_seq = 1
        last_seq = len(chunks)
        timer_started = time.monotonic()

        # 3. Send DATA using a sliding window.
        while base <= last_seq:
            while (
                next_seq <= last_seq
                and next_seq < base + window_size
            ):
                sock.sendto(packets[next_seq], destination)
                next_seq += 1

            elapsed = time.monotonic() - timer_started

            # If the window has not advanced, retransmit
            # every outstanding packet in the current window.
            if elapsed >= TIMEOUT:
                for seq in range(base, next_seq):
                    sock.sendto(packets[seq], destination)

                timer_started = time.monotonic()
                continue

            sock.settimeout(TIMEOUT - elapsed)

            try:
                raw, _ = sock.recvfrom(2048)
            except socket.timeout:
                for seq in range(base, next_seq):
                    sock.sendto(packets[seq], destination)

                timer_started = time.monotonic()
                continue

            result = decode_packet(raw)
            if result is None:
                continue

            header, _ = result

            if header.type != ACK:
                continue

            ack_num = header.seq_num

            # A cumulative ACK acknowledges all DATA packets
            # with sequence numbers smaller than ack_num.
            if base < ack_num <= next_seq:
                base = ack_num
                timer_started = time.monotonic()

        # 4. End the connection after all DATA is acknowledged.
        end_seq = last_seq + 1
        end_packet = build_packet(END, end_seq)

        sock.sendto(end_packet, destination)
        deadline = time.monotonic() + TIMEOUT

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            sock.settimeout(remaining)

            try:
                raw, _ = sock.recvfrom(2048)
            except socket.timeout:
                break

            result = decode_packet(raw)
            if result is None:
                continue

            header, _ = result

            if (
                header.type == ACK
                and header.seq_num == end_seq + 1
            ):
                break

    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip",
        help="IP address of the receiver",
    )
    parser.add_argument(
        "receiver_port",
        type=int,
        help="Port on which the receiver is listening",
    )
    parser.add_argument(
        "window_size",
        type=int,
        help="Maximum number of outstanding packets",
    )

    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
