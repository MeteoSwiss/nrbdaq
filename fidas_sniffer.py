#!/usr/bin/env python3
"""
fidas_sniffer.py — minimal UDP reader for FIDAS

Usage examples:
  # print raw bytes to stdout
  python3 fidas_sniffer.py --bind-ip 192.168.2.114 --port 56790

  # append raw bytes to a file (recommended)
  python3 fidas_sniffer.py --bind-ip 192.168.2.114 --port 56790 --outfile fidas_raw.bin
"""
import argparse
import socket
import sys

def main() -> None:
    ap = argparse.ArgumentParser(description="Minimal UDP sniffer for FIDAS")
    ap.add_argument("--bind-ip", default="0.0.0.0",
                    help="Local IP/interface to bind (e.g. your Pi IP). Default: 0.0.0.0")
    ap.add_argument("--port", type=int, default=56790,
                    help="Local UDP port to listen on. Default: 56790")
    ap.add_argument("--outfile", default="-",
                    help="Output file path, or '-' for stdout. Default: '-'")
    ap.add_argument("--bufsize", type=int, default=65535,
                    help="recvfrom() buffer size. Default: 65535")
    ap.add_argument("--rcvbuf", type=int, default=4*1024*1024,
                    help="Kernel receive buffer (SO_RCVBUF). Default: 4 MiB")
    ap.add_argument("--timeout", type=float, default=1.0,
                    help="Socket timeout seconds to stay responsive. Default: 1.0")
    args = ap.parse_args()

    # Open output (binary)
    out = sys.stdout.buffer if args.outfile == "-" else open(args.outfile, "ab", buffering=0)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Be restart-friendly and give headroom for bursts
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except OSError:
        pass  # not available on all platforms
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.rcvbuf)

    # Bind and start receiving
    sock.bind((args.bind_ip, args.port))
    sock.settimeout(args.timeout)

    # Inform on stderr so stdout stays clean if piped
    print(f"[fidas_sniffer] listening on {args.bind_ip}:{args.port}, "
          f"rcvbuf={args.rcvbuf}, bufsize={args.bufsize}, "
          f"writing to {'stdout' if args.outfile == '-' else args.outfile}",
          file=sys.stderr)

    try:
        while True:
            try:
                data, addr = sock.recvfrom(args.bufsize)
            except socket.timeout:
                continue  # stay responsive to Ctrl+C
            # Write bytes exactly as received (no added newline or decoding)
            out.write(data)
            out.flush()
    except KeyboardInterrupt:
        print("\n[fidas_sniffer] stopped.", file=sys.stderr)
    finally:
        try:
            if out is not sys.stdout.buffer:
                out.close()
        finally:
            sock.close()

if __name__ == "__main__":
    main()
