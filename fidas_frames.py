#!/usr/bin/env python3
"""
fidas_frames.py — minimal FIDAS UDP frame reader

Examples:
  # print cleaned <sendVal ...> frames to stdout
  python3 fidas_frames.py --bind-ip 192.168.2.114 --port 56790

  # same, but write JSON lines to a file
  python3 fidas_frames.py --bind-ip 192.168.2.114 --port 56790 --json --outfile frames.jsonl
"""
import argparse, socket, sys, time, json, re

SOF = "<sendVal "          # start marker in your sample
EOF = ">"                  # end marker

PAIR_RE = re.compile(r"(\d+)=([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|NaN)")

def parse_pairs(core: str) -> dict[str, float | None]:
    """
    Turn '0=1.23;1=-4.0;60=NaN' into {"0":1.23,"1":-4.0,"60":None}
    Returns strings for keys (JSON-safe).
    """
    out: dict[str, float | None] = {}
    for m in PAIR_RE.finditer(core):
        k, v = m.group(1), m.group(2)
        if v.lower() == "nan":
            out[k] = None
        else:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = None
    return out

def main() -> None:
    ap = argparse.ArgumentParser(description="Read FIDAS UDP frames and print them")
    ap.add_argument("--bind-ip", default="0.0.0.0", help="Local IP/interface to bind")
    ap.add_argument("--port", type=int, default=56790, help="Local UDP port")
    ap.add_argument("--outfile", default="-", help="File path or '-' for stdout")
    ap.add_argument("--bufsize", type=int, default=65535, help="recvfrom() buffer size")
    ap.add_argument("--rcvbuf", type=int, default=4*1024*1024, help="SO_RCVBUF size")
    ap.add_argument("--timeout", type=float, default=1.0, help="socket timeout seconds")
    ap.add_argument("--json", action="store_true", help="Emit JSON lines instead of raw text")
    args = ap.parse_args()

    out = sys.stdout if args.outfile == "-" else open(args.outfile, "a", buffering=1)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except OSError:
        pass
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.rcvbuf)
    sock.bind((args.bind_ip, args.port))
    sock.settimeout(args.timeout)

    print(f"[fidas_frames] listening on {args.bind_ip}:{args.port} (rcvbuf={args.rcvbuf})",
          file=sys.stderr)

    carry = ""
    warmup = 2  # skip the first couple of fragments in case we attached mid-frame

    try:
        while True:
            try:
                data, _ = sock.recvfrom(args.bufsize)
            except socket.timeout:
                continue

            carry += data.decode("ascii", errors="ignore")

            # Keep only from the first '<sendVal ' onward (drop preamble like '6111')
            sof_idx = carry.find(SOF)
            if sof_idx == -1:
                # no start yet—trim runaway buffers
                carry = carry[-4096:]
                continue
            if sof_idx > 0:
                carry = carry[sof_idx:]

            # Extract complete frames delimited by '>'
            while True:
                end = carry.find(EOF)
                if end == -1:
                    break
                frame = carry[:end+1]     # includes '>'
                carry = carry[end+1:]     # remainder

                if warmup > 0:
                    warmup -= 1
                    continue

                # frame looks like: "<sendVal 0=1.0;1=0.0;...>"
                # strip markers to get the assignments payload
                core = frame[len(SOF):-1]  # drop '<sendVal ' and trailing '>'
                ts = time.time()

                if args.json:
                    obj = {"ts": ts, "pairs": parse_pairs(core)}
                    print(json.dumps(obj, separators=(",", ":")), file=out)
                else:
                    # raw cleaned line with timestamp prefix
                    print(f"{ts:.3f} <sendVal {core}>", file=out)
    except KeyboardInterrupt:
        print("\n[fidas_frames] stopped.", file=sys.stderr)
    finally:
        if out is not sys.stdout:
            out.close()
        sock.close()

if __name__ == "__main__":
    main()
