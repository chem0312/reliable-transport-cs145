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

    try:
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
    except Exception:
        return None


def wait_for_start_ack(sock, destination):
    packet = build_packet(START, 0)

    while True:
        sock.sendto(packet, destination)
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
            if header.type == ACK and header.seq_num == 1:
                return


def send_data(sock, destination, chunks, window_size):
    packets = {
        seq: build_packet(DATA, seq, payload)
        for seq, payload in enumerate(chunks, start=1)
    }

    if not packets:
        return 1

    base = 1
    next_seq = 1
    last_seq = len(chunks)

    # True means that the corresponding DATA packet was ACKed.
    acknowledged = {seq: False for seq in packets}

    timer_started = None

    while base <= last_seq:
        # Fill the sending window.
        while next_seq <= last_seq and next_seq < base + window_size:
            sock.sendto(packets[next_seq], destination)

            if timer_started is None:
                timer_started = time.monotonic()

            next_seq += 1

        if timer_started is None:
            timer_started = time.monotonic()

        remaining = TIMEOUT - (time.monotonic() - timer_started)

        if remaining <= 0:
            # Retransmit only packets that have not been ACKed.
            for seq in range(base, next_seq):
                if not acknowledged[seq]:
                    sock.sendto(packets[seq], destination)

            timer_started = time.monotonic()
            continue

        sock.settimeout(remaining)

        try:
            raw, _ = sock.recvfrom(2048)
        except socket.timeout:
            continue

        result = decode_packet(raw)
        if result is None:
            continue

        header, _ = result

        if header.type != ACK:
            continue

        ack_seq = header.seq_num

        # In RTP-opt, each ACK confirms only its matching DATA packet.
        if base <= ack_seq < next_seq:
            acknowledged[ack_seq] = True

            old_base = base

            # Advance base only across consecutively ACKed packets.
            while base <= last_seq and acknowledged[base]:
                base += 1

            # Restart the single timer when the window advances.
            if base != old_base:
                timer_started = (
                    time.monotonic() if base <= last_seq else None
                )


def send_end(sock, destination, end_seq):
    packet = build_packet(END, end_seq)

    while True:
        sock.sendto(packet, destination)
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

            if header.type == ACK and header.seq_num == end_seq + 1:
                return


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

    try:
        wait_for_start_ack(sock, destination)
        send_data(sock, destination, chunks, window_size)

        end_seq = len(chunks) + 1
        send_end(sock, destination, end_seq)
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
