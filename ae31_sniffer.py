#!/usr/bin/env python3
# ae31_sniffer.py — quick check if AE31 is sending

import argparse, serial, sys, time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", default="/dev/ttyUSB0")             # e.g. /dev/ttyUSB0
    ap.add_argument("--baud", type=int, default=9600)
    ap.add_argument("--timeout", type=float, default=1.0)
    ap.add_argument("--seconds", type=int, default=60)   # how long to listen
    ap.add_argument("--outfile", default="-")            # '-' = stdout
    args = ap.parse_args()

    out = sys.stdout if args.outfile == "-" else open(args.outfile, "a", buffering=1)

    ser = serial.Serial(
        port=args.port,
        baudrate=args.baud,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=args.timeout,
        write_timeout=1.0,
        xonxoff=False, rtscts=False, dsrdtr=False,
        inter_byte_timeout=0.5,
        exclusive=False,           # don’t lock port, useful if another proc had it
    )

    # “Unblock”/reset lines once
    ser.reset_input_buffer(); ser.reset_output_buffer()
    ser.dtr = False; ser.rts = False; time.sleep(0.2)
    ser.dtr = True;  ser.rts = True

    print(f"[sniffer] listening on {args.port} @ {args.baud} for {args.seconds}s...", file=sys.stderr)
    n_lines, n_bytes = 0, 0
    t0 = time.time()
    try:
        while time.time() - t0 < args.seconds:
            line = ser.readline()  # returns b'' on timeout
            if not line:
                continue
            n_lines += 1; n_bytes += len(line)
            s = line.decode("ascii", errors="ignore").strip()
            print(s, file=out)
    finally:
        ser.close()
        if out is not sys.stdout:
            out.close()
    print(f"[sniffer] lines={n_lines}, bytes={n_bytes}", file=sys.stderr)

if __name__ == "__main__":
    main()
