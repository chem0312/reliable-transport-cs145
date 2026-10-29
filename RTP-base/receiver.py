
import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16
MAX_PACKET_SIZE = 2048

START = 0
END = 1
DATA = 2
ACK = 3


def build_packet(packet_type, seq_num):
    header = PacketHeader(
        type=packet_type,
        seq_num=seq_num,
        length=0,
        checksum=0,
    )
    header.checksum = compute_checksum(header)
    return bytes(header)


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

        calculated = compute_checksum(header / payload)
        if calculated != received_checksum:
            return None

        header.checksum = received_checksum
        return header, payload
    except Exception:
        return None


def receiver(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))

    expected = 1
    buffer = {}
    active = False

    try:
        while True:
            raw, address = sock.recvfrom(MAX_PACKET_SIZE)
            result = decode_packet(raw)

            if result is None:
                continue

            header, payload = result

            if header.type == START:
                if active:
                    continue

                expected = 1
                buffer.clear()
                active = True
                sock.sendto(build_packet(ACK, 1), address)
                continue

            if not active:
                continue

            if header.type == DATA:
                seq = header.seq_num

                if expected <= seq < expected + window_size:
                    if seq not in buffer:
                        buffer[seq] = payload

                    while expected in buffer:
                        sys.stdout.buffer.write(buffer.pop(expected))
                        expected += 1

                    sys.stdout.buffer.flush()

                sock.sendto(build_packet(ACK, expected), address)

            elif header.type == END:
                if header.seq_num == expected:
                    sock.sendto(
                        build_packet(ACK, header.seq_num + 1),
                        address,
                    )
                    break

    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
