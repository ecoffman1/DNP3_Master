"""
DNP3 raw probe — tries every meaningful frame variation to find what
the Typhoon HIL outstation responds to.

Usage: python dnp3_raw_probe.py <host> <port> <master_addr> <slave_addr>
"""
import socket, struct, time, sys

HOST   = sys.argv[1] if len(sys.argv) > 1 else "10.1.114.34"
PORT   = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
MASTER = int(sys.argv[3]) if len(sys.argv) > 3 else 1
SLAVE  = int(sys.argv[4]) if len(sys.argv) > 4 else 1024

_CRC_TABLE = []
def _build():
    for i in range(256):
        c = i
        for _ in range(8): c = (c>>1)^0xA6BC if c&1 else c>>1
        _CRC_TABLE.append(c)
_build()

def crc(data):
    c = 0
    for b in data: c = _CRC_TABLE[(c^b)&0xFF] ^ (c>>8)
    return struct.pack("<H", (~c)&0xFFFF)

def frame(dest, src, fc, app=b"", seq=0, ctrl_byte=0x44):
    """ctrl_byte: 0x44 = normal master, 0xC4 = DIR bit set"""
    tr  = bytes([0xC0])          # FIR+FIN transport
    ac  = bytes([0xC0 | (seq & 0xF)])  # FIR+FIN app ctrl
    ud  = tr + ac + bytes([fc]) + app
    blk = b""
    for i in range(0, len(ud), 16):
        c = ud[i:i+16]; blk += c + crc(c)
    hdr = b"\x05\x64" + struct.pack("<BBHH", len(ud)+5, ctrl_byte, dest, src)
    return hdr + crc(hdr) + blk

def listen(sock, secs=2.0, label=""):
    deadline = time.time() + secs
    got = b""
    while time.time() < deadline:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                print("  [connection closed by remote]")
                return got, True
            got += chunk
            print(f"  *** RX {len(chunk)} bytes: {chunk.hex()}")
        except socket.timeout:
            pass
    if not got:
        print(f"  (no response)")
    return got, False

print(f"Connecting to {HOST}:{PORT}  master={MASTER}  slave={SLAVE}")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(1.0)
sock.connect((HOST, PORT))
print(f"Connected from {sock.getsockname()}\n")

# Listen immediately for any unsolicited startup frame from the device
print("=== Listening 3s for unsolicited startup frame ===")
data, closed = listen(sock, 3.0)
if closed:
    print("Device closed connection immediately — restarting Typhoon simulation is required")
    sys.exit(1)

tests = [
    # label,                          dest,    src,    fc,    app,                        ctrl
    ("Integrity G60V1  ctrl=0x44",    SLAVE,  MASTER, 0x01, bytes([60,1,0x06]),           0x44),
    ("Integrity G60V1  ctrl=0xC4",    SLAVE,  MASTER, 0x01, bytes([60,1,0x06]),           0xC4),
    ("Integrity G60V1  master=0",     SLAVE,  0,      0x01, bytes([60,1,0x06]),           0x44),
    ("Integrity G60V1  master=3",     SLAVE,  3,      0x01, bytes([60,1,0x06]),           0x44),
    ("Integrity G60V1  master=1024",  SLAVE,  1024,   0x01, bytes([60,1,0x06]),           0x44),
    ("Read G1V0        ctrl=0x44",    SLAVE,  MASTER, 0x01, bytes([1, 0, 0x06]),           0x44),
    ("Read G30V0       ctrl=0x44",    SLAVE,  MASTER, 0x01, bytes([30,0, 0x06]),           0x44),
    ("Write G80V1 clr  ctrl=0x44",    SLAVE,  MASTER, 0x02, bytes([80,1,0x00,7,7,0x00]), 0x44),
    ("Write G80V1 clr  ctrl=0xC4",    SLAVE,  MASTER, 0x02, bytes([80,1,0x00,7,7,0x00]), 0xC4),
    ("Integrity slave=1023",          1023,   MASTER, 0x01, bytes([60,1,0x06]),           0x44),
    ("Integrity slave=1022",          1022,   MASTER, 0x01, bytes([60,1,0x06]),           0x44),
    ("Integrity slave=1021",          1021,   MASTER, 0x01, bytes([60,1,0x06]),           0x44),
    ("Broadcast 0xFFFF ctrl=0x44",    0xFFFF, MASTER, 0x01, bytes([60,1,0x06]),           0x44),
    ("Broadcast 0xFFFF ctrl=0xC4",    0xFFFF, MASTER, 0x01, bytes([60,1,0x06]),           0xC4),
]

for seq, (label, dest, src, fc, app, ctrl) in enumerate(tests):
    f = frame(dest, src, fc, app, seq=seq, ctrl_byte=ctrl)
    print(f"=== TX [{label}]: {f.hex()}")
    try:
        sock.sendall(f)
    except OSError as e:
        print(f"  Send failed: {e}")
        break
    data, closed = listen(sock, 2.0)
    if data:
        print(f"\n*** GOT RESPONSE TO: {label} ***\n")
    if closed:
        print("Device closed the connection after this frame.")
        break
    time.sleep(0.1)

sock.close()
print("\nDone.")
