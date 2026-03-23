"""
DNP3 TCP Master Client — Multi-Outstation, with Full Object Decoding
=====================================================================

Architecture:
  DNP3Master     — owns the TCP socket and receive thread; routes incoming
                   frames to the correct SlaveSession by SRC address.
  SlaveSession   — per-outstation state (independent sequence counters) and
                   all high-level request methods.
  decode_objects — parses application-layer object headers and data bytes into
                   a list of DNP3Point namedtuples: slave_addr, group,
                   variation, index, value, flags, timestamp_ms.

Supported object groups decoded to typed Python values:
  Group  1  Binary Input (static)
  Group  2  Binary Input Change (with/without time)
  Group  3  Double-Bit Binary Input
  Group 10  Binary Output Status
  Group 12  Control Relay Output Block (CROB)
  Group 20  Binary Counter
  Group 22  Counter Change
  Group 30  Analog Input  ← most commonly used
  Group 32  Analog Input Change
  Group 40  Analog Output Status
  Group 41  Analog Output Block
  Group 50  Time and Date
  Group 80  Internal Indications (IIN bits)

All other groups fall back to raw bytes.

Usage
-----
  python dnp3_client.py 192.168.1.100 20000 --slaves 10 20
  python dnp3_client.py 192.168.1.100 20000 --master 1 --slaves 10 --debug
"""

import socket
import struct
import threading
import time
import logging
import random
from collections import namedtuple
from enum import IntEnum
from typing import Callable, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Logging — always force our handler so basicConfig() conflicts don't silence us
# ---------------------------------------------------------------------------
log = logging.getLogger("dnp3")
log.setLevel(logging.INFO)

if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s"))
    log.addHandler(_handler)

log.propagate = False   # don't double-print if root logger is also configured


# ===========================================================================
# CRC-16/DNP
# ===========================================================================

_CRC_TABLE: List[int] = []


def _build_crc_table() -> None:
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA6BC if crc & 1 else crc >> 1
        _CRC_TABLE.append(crc)


_build_crc_table()


def crc16_dnp(data: bytes) -> int:
    crc = 0
    for b in data:
        crc = _CRC_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return (~crc) & 0xFFFF


def crc_bytes(data: bytes) -> bytes:
    return struct.pack("<H", crc16_dnp(data))


def verify_crc(data: bytes) -> bool:
    if len(data) < 3:
        return False
    return crc16_dnp(data[:-2]) == struct.unpack("<H", data[-2:])[0]


# ===========================================================================
# DNP3 constants
# ===========================================================================

DNP3_START = b"\x05\x64"


class FunctionCode(IntEnum):
    CONFIRM         = 0x00
    READ            = 0x01
    WRITE           = 0x02
    DIRECT_OPERATE  = 0x03
    FREEZE          = 0x07
    COLD_RESTART    = 0x0D
    WARM_RESTART    = 0x0E
    RESPONSE        = 0x81
    UNSOLICITED_RSP = 0x82


# ===========================================================================
# Decoded point
# ===========================================================================

DNP3Point = namedtuple(
    "DNP3Point",
    ["slave_addr", "group", "variation", "index", "value", "flags", "timestamp_ms"],
)


def _fmt_point(p: DNP3Point) -> str:
    """Compact human-readable string for one decoded point."""
    val = p.value.hex() if isinstance(p.value, (bytes, bytearray)) else str(p.value)
    parts = [f"slave={p.slave_addr}", f"G{p.group}V{p.variation}", f"idx={p.index}", f"value={val}"]
    if p.flags is not None:
        parts.append(f"flags=0x{p.flags:02X}")
    if p.timestamp_ms is not None:
        parts.append(f"ts={p.timestamp_ms}ms")
    return "  ".join(parts)


# ===========================================================================
# Object data decoders (group/variation → typed values)
# ===========================================================================

def _decode_dnp3_time(raw6: bytes) -> int:
    """6-byte DNP3 timestamp → milliseconds since Unix epoch."""
    lo = struct.unpack("<I", raw6[:4])[0]
    hi = struct.unpack("<H", raw6[4:6])[0]
    return lo | (hi << 32)


def _decode_objects(group: int, variation: int, indices: List[int], data: bytes) -> List[dict]:
    """
    Decode raw object data bytes for a given group/variation.
    Returns list of {index, value, flags, timestamp_ms}.
    """
    results: List[dict] = []
    pos = 0

    def emit(idx, value, flags=None, ts=None):
        results.append({"index": idx, "value": value, "flags": flags, "timestamp_ms": ts})

    # --- Group 1: Binary Input ---
    if group == 1:
        if variation == 1:  # packed bits, no flags
            for i, idx in enumerate(indices):
                byte_off = i // 8
                bit_off  = i % 8
                if byte_off < len(data):
                    emit(idx, bool((data[byte_off] >> bit_off) & 1))
        elif variation == 2:  # 1 byte: flags[6:0], value[7]
            for idx in indices:
                if pos < len(data):
                    b = data[pos]; pos += 1
                    emit(idx, bool(b >> 7), flags=b & 0x7F)

    # --- Group 2: Binary Input Change ---
    elif group == 2:
        if variation == 1:
            for idx in indices:
                if pos + 1 <= len(data):
                    b = data[pos]; pos += 1
                    emit(idx, bool(b >> 7), flags=b & 0x7F)
        elif variation == 2:
            for idx in indices:
                if pos + 7 <= len(data):
                    b = data[pos]; pos += 1
                    ts = _decode_dnp3_time(data[pos:pos+6]); pos += 6
                    emit(idx, bool(b >> 7), flags=b & 0x7F, ts=ts)
        elif variation == 3:
            for idx in indices:
                if pos + 3 <= len(data):
                    b = data[pos]; pos += 1
                    rt = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2
                    emit(idx, bool(b >> 7), flags=b & 0x7F, ts=rt)

    # --- Group 3: Double-Bit Binary Input ---
    elif group == 3:
        DBITS = {0: "INTERMEDIATE", 1: "FAILURE", 2: "OFF", 3: "ON"}
        if variation == 2:
            for idx in indices:
                if pos < len(data):
                    b = data[pos]; pos += 1
                    emit(idx, DBITS.get((b >> 6) & 3, "?"), flags=b & 0x3F)

    # --- Group 10: Binary Output Status ---
    elif group == 10:
        if variation == 2:
            for idx in indices:
                if pos < len(data):
                    b = data[pos]; pos += 1
                    emit(idx, bool(b >> 7), flags=b & 0x7F)

    # --- Group 12: CROB ---
    elif group == 12:
        if variation == 1:
            CODES = {0:"NUL",1:"PULSE_ON",2:"PULSE_OFF",3:"LATCH_ON",4:"LATCH_OFF"}
            TC    = ["","NUL","CLOSE","TRIP"]
            for idx in indices:
                if pos + 11 <= len(data):
                    cc  = data[pos]; cnt = data[pos+1]
                    on  = struct.unpack("<I", data[pos+2:pos+6])[0]
                    off = struct.unpack("<I", data[pos+6:pos+10])[0]
                    st  = data[pos+10]; pos += 11
                    emit(idx, {"code": CODES.get(cc&0xF,f"0x{cc:02X}"),
                               "trip_close": TC[(cc>>6)&3],
                               "count": cnt, "on_ms": on, "off_ms": off, "status": st})

    # --- Group 20: Binary Counter ---
    elif group == 20:
        specs = {1:("<I",4), 2:("<H",2), 5:("<I",4), 6:("<H",2)}
        if variation in specs:
            f, sz = specs[variation]
            for idx in indices:
                if pos + 1 + sz <= len(data):
                    fl = data[pos]; pos += 1
                    v  = struct.unpack(f, data[pos:pos+sz])[0]; pos += sz
                    emit(idx, v, flags=fl)

    # --- Group 22: Counter Change ---
    elif group == 22:
        base_specs = {1:("<I",4), 2:("<H",2), 5:("<I",4), 6:("<H",2)}
        ts_vars    = {3,4,7,8}
        bv = variation if variation <= 2 else (variation - 2 if variation <= 4 else
             variation - 4 if variation <= 6 else variation - 6)
        if bv in base_specs:
            f, sz = base_specs[bv]
            for idx in indices:
                if pos + 1 + sz <= len(data):
                    fl = data[pos]; pos += 1
                    v  = struct.unpack(f, data[pos:pos+sz])[0]; pos += sz
                    ts = None
                    if variation in ts_vars and pos + 6 <= len(data):
                        ts = _decode_dnp3_time(data[pos:pos+6]); pos += 6
                    emit(idx, v, flags=fl, ts=ts)

    # --- Group 30: Analog Input ---
    elif group == 30:
        specs = {1:("<i",4,True), 2:("<h",2,True), 3:("<i",4,False),
                 4:("<h",2,False), 5:("<f",4,True), 6:("<d",8,True)}
        if variation in specs:
            f, sz, has_flags = specs[variation]
            for idx in indices:
                fl = None
                if has_flags:
                    if pos >= len(data): break
                    fl = data[pos]; pos += 1
                if pos + sz <= len(data):
                    v = struct.unpack(f, data[pos:pos+sz])[0]; pos += sz
                    emit(idx, v, flags=fl)

    # --- Group 32: Analog Input Change ---
    elif group == 32:
        specs = {1:("<i",4,False), 2:("<h",2,False), 3:("<i",4,True),
                 4:("<h",2,True),  5:("<f",4,False), 6:("<f",4,True),
                 7:("<d",8,False), 8:("<d",8,True)}
        if variation in specs:
            f, sz, has_ts = specs[variation]
            for idx in indices:
                if pos >= len(data): break
                fl = data[pos]; pos += 1
                if pos + sz > len(data): break
                v  = struct.unpack(f, data[pos:pos+sz])[0]; pos += sz
                ts = None
                if has_ts and pos + 6 <= len(data):
                    ts = _decode_dnp3_time(data[pos:pos+6]); pos += 6
                emit(idx, v, flags=fl, ts=ts)

    # --- Group 40: Analog Output Status ---
    elif group == 40:
        specs = {1:("<i",4), 2:("<h",2), 3:("<f",4), 4:("<d",8)}
        if variation in specs:
            f, sz = specs[variation]
            for idx in indices:
                if pos + 1 + sz <= len(data):
                    fl = data[pos]; pos += 1
                    v  = struct.unpack(f, data[pos:pos+sz])[0]; pos += sz
                    emit(idx, v, flags=fl)

    # --- Group 41: Analog Output Block ---
    elif group == 41:
        specs = {1:("<i",4), 2:("<h",2), 3:("<f",4), 4:("<d",8)}
        if variation in specs:
            f, sz = specs[variation]
            for idx in indices:
                if pos + sz + 1 <= len(data):
                    v  = struct.unpack(f, data[pos:pos+sz])[0]; pos += sz
                    st = data[pos]; pos += 1
                    emit(idx, v, flags=st)

    # --- Group 50: Time and Date ---
    elif group == 50:
        if variation in (1, 3, 4) and pos + 6 <= len(data):
            emit(0, _decode_dnp3_time(data[pos:pos+6]))

    # --- Group 80: Internal Indications ---
    elif group == 80:
        IIN = {0:"BROADCAST",1:"CLASS1_EVENTS",2:"CLASS2_EVENTS",
               3:"CLASS3_EVENTS",4:"NEED_TIME",5:"LOCAL_CTRL",
               6:"DEVICE_TROUBLE",7:"DEVICE_RESTART",
               8:"FUNC_NOT_SUPPORTED",9:"OBJ_UNKNOWN",
               10:"PARAM_ERROR",11:"EVENT_BUFFER_OVERFLOW",
               12:"ALREADY_EXECUTING",13:"CONFIG_CORRUPT",
               14:"RESERVED_2",15:"RESERVED_1"}
        if variation == 1 and len(data) >= 2:
            word   = struct.unpack("<H", data[:2])[0]
            active = [IIN[b] for b in range(16) if word & (1 << b)]
            emit(0, active if active else "OK")

    # --- Fallback: raw bytes ---
    else:
        emit(indices[0] if indices else 0, data)

    return results


def _object_size(group: int, variation: int) -> Optional[int]:
    """Fixed byte-size per object instance, or None if variable/unknown."""
    TABLE = {
        (1,1):-1,  # packed bits: ceil(n/8) bytes total
        (3,1):-1,  # packed bits: ceil(n/8) bytes total
        (1,2):1,
        (2,1):1,(2,2):7,(2,3):3,
        (3,2):1,
        (10,2):1,
        (12,1):11,
        (20,1):5,(20,2):3,(20,5):5,(20,6):3,
        (22,1):5,(22,2):3,(22,3):11,(22,4):9,
        (22,5):5,(22,6):3,(22,7):11,(22,8):9,
        (30,1):5,(30,2):3,(30,3):4,(30,4):2,(30,5):5,(30,6):9,
        (32,1):5,(32,2):3,(32,3):11,(32,4):9,
        (32,5):5,(32,6):11,(32,7):9,(32,8):15,
        (40,1):5,(40,2):3,(40,3):5,(40,4):9,
        (41,1):5,(41,2):3,(41,3):5,(41,4):9,
        (50,1):6,
        (80,1):2,
    }
    return TABLE.get((group, variation))


# ===========================================================================
# Application-layer object header parser
# ===========================================================================

def decode_objects(slave_addr: int, app_data: bytes) -> List[DNP3Point]:
    """
    Parse DNP3 application-layer object data (everything after the FC byte).
    Returns a list of DNP3Point namedtuples.
    """
    points: List[DNP3Point] = []
    pos = 0

    def _append(group, variation, decoded_list):
        for d in decoded_list:
            points.append(DNP3Point(
                slave_addr   = slave_addr,
                group        = group,
                variation    = variation,
                index        = d["index"],
                value        = d["value"],
                flags        = d["flags"],
                timestamp_ms = d["timestamp_ms"],
            ))

    while pos + 3 <= len(app_data):
        group     = app_data[pos];   pos += 1
        variation = app_data[pos];   pos += 1
        qualifier = app_data[pos];   pos += 1

        # ---- resolve index list and advance pos past qualifier header ----
        indices: Optional[List[int]] = None

        if qualifier == 0x06:
            # All objects — consume the rest of app_data
            raw = app_data[pos:]
            _append(group, variation, _decode_objects(group, variation, list(range(256)), raw))
            break

        elif qualifier == 0x00:   # 8-bit start/stop
            if pos + 2 > len(app_data): break
            start = app_data[pos]; stop = app_data[pos+1]; pos += 2
            indices = list(range(start, stop + 1))

        elif qualifier == 0x01:   # 16-bit start/stop
            if pos + 4 > len(app_data): break
            start = struct.unpack("<H", app_data[pos:pos+2])[0]; pos += 2
            stop  = struct.unpack("<H", app_data[pos:pos+2])[0]; pos += 2
            indices = list(range(start, stop + 1))

        elif qualifier == 0x07:   # 8-bit count, no index prefix
            if pos + 1 > len(app_data): break
            count = app_data[pos]; pos += 1
            indices = list(range(count))

        elif qualifier == 0x08:   # 16-bit count, no index prefix
            if pos + 2 > len(app_data): break
            count = struct.unpack("<H", app_data[pos:pos+2])[0]; pos += 2
            indices = list(range(count))

        elif qualifier in (0x17, 0x28):   # count + per-object index prefix
            idx_sz = 1 if qualifier == 0x17 else 2
            cnt_sz = 1 if qualifier == 0x17 else 2
            if pos + cnt_sz > len(app_data): break
            count = (app_data[pos] if cnt_sz == 1
                     else struct.unpack("<H", app_data[pos:pos+2])[0])
            pos += cnt_sz
            obj_sz = _object_size(group, variation)
            for _ in range(count):
                if pos + idx_sz > len(app_data): break
                idx = (app_data[pos] if idx_sz == 1
                       else struct.unpack("<H", app_data[pos:pos+2])[0])
                pos += idx_sz
                raw_obj = app_data[pos : pos + obj_sz] if obj_sz else app_data[pos:]
                _append(group, variation, _decode_objects(group, variation, [idx], raw_obj))
                if obj_sz:
                    pos += obj_sz
            continue

        else:
            log.warning("Unknown qualifier 0x%02X for G%dV%d — skipping", qualifier, group, variation)
            break

        # ---- decode objects for range/count qualifiers ----
        obj_sz = _object_size(group, variation)
        if obj_sz == -1:
            # Packed bits: the whole range is stored in ceil(count/8) bytes
            import math
            packed_bytes = math.ceil(len(indices) / 8)
            raw = app_data[pos : pos + packed_bytes]
            _append(group, variation, _decode_objects(group, variation, indices, raw))
            pos += packed_bytes
        elif obj_sz:
            raw = app_data[pos : pos + obj_sz * len(indices)]
            _append(group, variation, _decode_objects(group, variation, indices, raw))
            pos += obj_sz * len(indices)
        else:
            # Unknown size — best effort, stops further parsing of this frame
            raw = app_data[pos:]
            _append(group, variation, _decode_objects(group, variation, indices, raw))
            break

    return points


# ===========================================================================
# Frame builder / parser
# ===========================================================================

def build_frame(
    dest: int, src: int, function_code: int,
    app_data: bytes = b"", app_seq: int = 0, transport_seq: int = 0,
) -> bytes:
    transport_byte = 0xC0 | (transport_seq & 0x3F)
    app_ctrl_byte  = 0xC0 | (app_seq & 0x0F)
    user_data      = bytes([transport_byte, app_ctrl_byte, function_code]) + app_data

    blocks = b""
    for i in range(0, len(user_data), 16):
        chunk = user_data[i:i+16]
        blocks += chunk + crc_bytes(chunk)

    header_raw = DNP3_START + struct.pack("<BBHH", len(user_data)+5, 0xC4, dest, src)  # 0xC4 = DIR bit set (required by Typhoon HIL)
    return header_raw + crc_bytes(header_raw) + blocks


def parse_frame(raw: bytes) -> Optional[dict]:
    if len(raw) < 10 or raw[:2] != DNP3_START:
        return None
    if not verify_crc(raw[:10]):
        log.warning("Header CRC mismatch")
        return None

    length, ctrl, dest, src = struct.unpack("<BBHH", raw[2:8])
    base = {"dest": dest, "src": src, "ctrl": ctrl, "length": length}

    user_data = b""
    offset    = 10
    bad_crc   = False
    while offset < len(raw):
        block_end      = min(offset + 16, len(raw) - 2)
        chunk          = raw[offset:block_end]
        block_with_crc = raw[offset:block_end+2]
        if len(block_with_crc) < 3:
            break
        if not verify_crc(block_with_crc):
            log.warning("Data block CRC mismatch at offset %d", offset)
            bad_crc = True; break
        user_data += chunk
        offset     = block_end + 2

    if bad_crc:
        return {**base, "valid_crc": False, "transport": None,
                "app_ctrl": None, "function_code": None, "app_data": b""}

    if len(user_data) < 2:
        return None

    return {
        **base,
        "valid_crc":     True,
        "transport":     user_data[0],
        "app_ctrl":      user_data[1],
        "function_code": user_data[2] if len(user_data) > 2 else None,
        "app_data":      user_data[3:] if len(user_data) > 3 else b"",
    }


# ===========================================================================
# SlaveSession
# ===========================================================================

class SlaveSession:
    """
    Per-outstation session with independent sequence counters.

    Callbacks (all optional — set after construction):
      on_points(List[DNP3Point])             decoded points from a RESPONSE
      on_unsolicited_points(List[DNP3Point]) decoded points from UNSOLICITED_RSP
      on_response(frame: dict)               raw frame dict from a RESPONSE
      on_unsolicited(frame: dict)            raw frame dict from UNSOLICITED_RSP
      on_error(slave_addr, reason: str)      CRC errors / decode failures
    """

    def __init__(self, master: "DNP3Master", slave_addr: int):
        self._master    = master
        self.slave_addr = slave_addr
        self._app_seq   = 0
        self._trans_seq = 0
        self._lock      = threading.Lock()

        self.on_points:             Optional[Callable[[List[DNP3Point]], None]] = None
        self.on_unsolicited_points: Optional[Callable[[List[DNP3Point]], None]] = None
        self.on_response:           Optional[Callable[[dict], None]] = None
        self.on_unsolicited:        Optional[Callable[[dict], None]] = None
        self.on_error:              Optional[Callable[[int, str], None]] = None

    def _next_app_seq(self) -> int:
        with self._lock:
            s = self._app_seq; self._app_seq = (s + 1) & 0x0F; return s

    def _next_trans_seq(self) -> int:
        with self._lock:
            s = self._trans_seq; self._trans_seq = (s + 1) & 0x3F; return s

    def _send(self, fc: int, app_data: bytes = b"") -> None:
        self._master._send_raw(build_frame(
            dest=self.slave_addr, src=self._master.master_addr,
            function_code=fc, app_data=app_data,
            app_seq=self._next_app_seq(), transport_seq=self._next_trans_seq(),
        ))

    # --- requests ---
    def send_integrity_poll(self)                              : self._send(FunctionCode.READ, bytes([60,1,0x06]))
    def send_read_request(self, group: int, variation: int)   : self._send(FunctionCode.READ, bytes([group,variation,0x06]))
    def send_cold_restart(self)                                : self._send(FunctionCode.COLD_RESTART)
    def send_warm_restart(self)                                : self._send(FunctionCode.WARM_RESTART)
    def send_freeze(self)                                      : self._send(FunctionCode.FREEZE, bytes([20,0,0x06]))

    def send_direct_operate(self, group, variation, index, data):
        self._send(FunctionCode.DIRECT_OPERATE,
                   bytes([group, variation, 0x28, 1]) + struct.pack("<H", index) + data)

    def send_write(self, group, variation, index, data):
        self._send(FunctionCode.WRITE,
                   bytes([group, variation, 0x28, 1]) + struct.pack("<H", index) + data)

    def send_confirm(self, seq: int) -> None:
        self._master._send_raw(build_frame(
            dest=self.slave_addr, src=self._master.master_addr,
            function_code=FunctionCode.CONFIRM, app_data=b"",
            app_seq=seq, transport_seq=self._next_trans_seq(),
        ))

    # --- inbound dispatch ---
    def _dispatch(self, frame: dict) -> None:
        if not frame["valid_crc"]:
            reason = f"CRC error in frame from slave {self.slave_addr}"
            log.warning(reason)
            if self.on_error: self.on_error(self.slave_addr, reason)
            return

        fc             = frame.get("function_code")
        app_data       = frame.get("app_data", b"")
        is_response    = (fc == FunctionCode.RESPONSE)
        is_unsolicited = (fc == FunctionCode.UNSOLICITED_RSP)

        # Always attempt to decode objects.
        # RESPONSE and UNSOLICITED_RSP frames prepend 2 IIN bytes before
        # the object headers — strip them before decoding.
        points: List[DNP3Point] = []
        obj_payload = app_data[2:] if (is_response or is_unsolicited) and len(app_data) >= 2 else app_data
        if obj_payload:
            try:
                points = decode_objects(self.slave_addr, obj_payload)
            except Exception as exc:
                log.warning("[slave %d] Object decode error: %s", self.slave_addr, exc)

        if is_response or is_unsolicited:
            # Auto-confirm when outstation sets the CON bit
            ac = frame.get("app_ctrl")
            if ac is not None and (ac >> 5) & 1:
                self.send_confirm(ac & 0x0F)

            label = "RESPONSE" if is_response else "UNSOLICITED"

            # --- Always print something so the user knows a frame arrived ---
            print(f"\n{'═'*62}")
            print(f"  {label} from slave {self.slave_addr}")
            print(f"{'─'*62}")
            if points:
                for p in points:
                    print(f"  {_fmt_point(p)}")
            else:
                raw_hex = app_data.hex() if app_data else "(empty)"
                print(f"  No decodable object data.  raw app_data={raw_hex}")
            print(f"{'═'*62}\n")

            if is_response:
                if self.on_points:    self.on_points(points)
                if self.on_response:  self.on_response(frame)
            else:
                if self.on_unsolicited_points: self.on_unsolicited_points(points)
                if self.on_unsolicited:        self.on_unsolicited(frame)

        else:
            fc_name = (FunctionCode(fc).name
                       if fc in FunctionCode._value2member_map_ else f"0x{fc:02X}")
            log.info("[slave %d] Received FC=%s", self.slave_addr, fc_name)


# ===========================================================================
# DNP3Master
# ===========================================================================

class DNP3Master:
    """
    DNP3 TCP master that shares one socket across many outstation addresses.

    Features
    --------
    - TCP SO_KEEPALIVE so the OS detects a silently dead connection
    - Auto-reconnect: if the socket drops, a background thread reconnects
      and re-sends integrity polls to all registered slaves
    - Periodic integrity poll (keepalive_interval seconds, default 30s) so
      the device never times the session out due to inactivity
    - Incoming frames routed by DNP3 SRC address to the correct SlaveSession
    """

    def __init__(self, host: str, port: int = 20000,
                 master_addr: int = 1, timeout: float = 5.0,
                 reconnect_delay: float = 5.0,
                 keepalive_interval: float = 60.0):
        self.host               = host
        self.port               = port
        self.master_addr        = master_addr
        self.timeout            = timeout
        self.reconnect_delay    = reconnect_delay    # seconds between reconnect attempts
        self.keepalive_interval = keepalive_interval # seconds between integrity polls
        self._sock: Optional[socket.socket] = None
        self._running    = False
        self._send_lock  = threading.Lock()
        self._recv_buf   = b""
        self._recv_thread:      Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None
        self._slaves: Dict[int, SlaveSession] = {}
        self._slaves_lock = threading.Lock()
        self.on_unknown_slave:  Optional[Callable[[dict], None]] = None
        self.on_reconnect:      Optional[Callable[[], None]] = None  # fired after each reconnect

    # ------------------------------------------------------------------
    # Slave registry
    # ------------------------------------------------------------------

    def add_slave(self, addr: int) -> SlaveSession:
        with self._slaves_lock:
            if addr not in self._slaves:
                self._slaves[addr] = SlaveSession(self, addr)
                log.info("Registered slave address %d", addr)
            return self._slaves[addr]

    def remove_slave(self, addr: int) -> None:
        with self._slaves_lock: self._slaves.pop(addr, None)

    def get_slave(self, addr: int) -> Optional[SlaveSession]:
        with self._slaves_lock: return self._slaves.get(addr)

    def slaves(self) -> Dict[int, SlaveSession]:
        with self._slaves_lock: return dict(self._slaves)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the TCP socket and start background threads."""
        self._running = True
        self._open_socket()
        self._recv_thread = threading.Thread(
            target=self._recv_loop, daemon=True, name="dnp3-recv")
        self._keepalive_thread = threading.Thread(
            target=self._keepalive_loop, daemon=True, name="dnp3-keepalive")
        self._recv_thread.start()
        self._keepalive_thread.start()
        # Give the recv thread a moment to start before the startup sequence
        time.sleep(0.1)

    def disconnect(self) -> None:
        """Shut down all threads and close the socket."""
        self._running = False
        self._close_socket()
        log.info("Disconnected.")

    def is_connected(self) -> bool:
        return self._sock is not None

    # ------------------------------------------------------------------
    # Internal socket open / close
    # ------------------------------------------------------------------

    def _open_socket(self) -> bool:
        """Create and connect the TCP socket. Returns True on success."""
        try:
            log.info("Connecting to %s:%d …", self.host, self.port)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # TCP keepalive — OS will probe and detect a dead link
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):   # Linux
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,  10)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT,   3)

            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
            self._recv_buf = b""        # discard stale bytes from old session
            self._sock = sock
            log.info("Connected to %s:%d", self.host, self.port)
            return True
        except OSError as exc:
            log.error("Connection failed: %s", exc)
            self._sock = None
            return False

    def _close_socket(self) -> None:
        sock = self._sock
        self._sock = None
        if sock:
            try: sock.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            try: sock.close()
            except OSError: pass

    # ------------------------------------------------------------------
    # Thread-safe send
    # ------------------------------------------------------------------

    def _send_raw(self, frame: bytes) -> None:
        sock = self._sock
        if not sock:
            log.warning("Send skipped — not connected")
            return
        try:
            with self._send_lock:
                log.debug("TX %d bytes: %s", len(frame), frame.hex())
                sock.sendall(frame)
        except OSError as exc:
            log.error("Send failed: %s — connection will be re-established", exc)
            self._close_socket()

    # ------------------------------------------------------------------
    # Broadcast helpers
    # ------------------------------------------------------------------

    def broadcast_integrity_poll(self):
        for s in self.slaves().values(): s.send_integrity_poll()

    def broadcast_cold_restart(self):
        for s in self.slaves().values(): s.send_cold_restart()

    # ------------------------------------------------------------------
    # Startup sequence — clear restart bit then poll
    # ------------------------------------------------------------------

    def _startup_sequence(self) -> None:
        """
        Perform the DNP3 startup handshake after connect/reconnect.

        Many outstations (including Typhoon HIL) set the Device Restart IIN
        bit on startup and send an UNSOLICITED_RSP to announce it. They will
        NOT respond to any READ until the master:
          1. Sends WRITE G80V1 to clear the restart bit, OR
          2. Confirms the unsolicited response that carries the restart bit.

        We do both to be safe:
          - Wait briefly for any immediate unsolicited startup frame (the
            recv loop will auto-confirm it via _dispatch).
          - Explicitly WRITE G80V1 idx=7 value=0 to clear Device Restart.
          - Then send integrity polls.
        """
        log.info("Running startup sequence …")

        # Step 1 — pause briefly so any immediate unsolicited startup frame
        # arrives and gets confirmed by the recv loop before we write
        time.sleep(0.5)

        # Step 2 — explicitly clear the Device Restart bit (G80V1, index 7)
        # on every registered slave.  This unblocks outstations that require
        # the master to write the restart bit clear before answering polls.
        for s in self.slaves().values():
            try:
                log.info("[slave %d] Writing G80V1 to clear Device Restart bit", s.slave_addr)
                # qualifier 0x00, start=7, stop=7, value=0x00
                app_data = bytes([80, 1, 0x00, 7, 7, 0x00])
                s._send(FunctionCode.WRITE, app_data)
                time.sleep(0.1)
            except Exception as exc:
                log.warning("[slave %d] Write G80V1 failed: %s", s.slave_addr, exc)

        # Step 3 — wait a moment for the outstation to process the write
        time.sleep(0.3)

        # Step 4 — send integrity polls
        log.info("Startup: sending integrity polls …")
        for s in self.slaves().values():
            try:
                s.send_integrity_poll()
                time.sleep(0.05)
            except Exception as exc:
                log.warning("[slave %d] Integrity poll failed: %s", s.slave_addr, exc)

    # ------------------------------------------------------------------
    # Receive loop — reconnects automatically on drop
    # ------------------------------------------------------------------

    def _recv_loop(self) -> None:
        while self._running:
            if self._sock is None:
                # Socket lost — wait then reconnect
                log.warning("Connection lost. Reconnecting in %.0fs …", self.reconnect_delay)
                time.sleep(self.reconnect_delay)
                if not self._running:
                    break
                if self._open_socket():
                    self._startup_sequence()
                    if self.on_reconnect:
                        try: self.on_reconnect()
                        except Exception: pass
                continue

            try:
                chunk = self._sock.recv(4096)
                if not chunk:
                    log.warning("Remote host closed the connection.")
                    self._close_socket()
                    continue
                log.info("RX %d bytes: %s", len(chunk), chunk.hex())
                self._recv_buf += chunk
                self._drain_buffer()
            except socket.timeout:
                continue
            except OSError as exc:
                if self._running:
                    log.error("Socket read error: %s", exc)
                self._close_socket()

    # ------------------------------------------------------------------
    # Keepalive loop — periodic integrity poll to prevent session timeout
    # ------------------------------------------------------------------

    def _keepalive_loop(self) -> None:
        """Send an integrity poll on every slave every keepalive_interval seconds."""
        while self._running:
            time.sleep(self.keepalive_interval)
            if not self._running:
                break
            if self._sock is None:
                continue   # reconnect in progress — recv_loop handles it
            log.info("Keepalive: sending integrity polls to all slaves")
            for s in self.slaves().values():
                try: s.send_integrity_poll()
                except Exception: pass

    # ------------------------------------------------------------------
    # Frame assembly and routing
    # ------------------------------------------------------------------

    def _drain_buffer(self) -> None:
        while True:
            idx = self._recv_buf.find(DNP3_START)
            if idx == -1:
                if self._recv_buf:
                    log.warning("No DNP3 start bytes in buffer (%d bytes): %s",
                                len(self._recv_buf), self._recv_buf.hex())
                self._recv_buf = b""; return

            if idx > 0:
                log.warning("Discarding %d non-DNP3 bytes: %s",
                            idx, self._recv_buf[:idx].hex())
                self._recv_buf = self._recv_buf[idx:]

            if len(self._recv_buf) < 10: return

            user_data_len = self._recv_buf[2] - 5
            if user_data_len < 0:
                log.warning("Bad LEN byte 0x%02X — skipping", self._recv_buf[2])
                self._recv_buf = self._recv_buf[2:]; continue

            frame_size = 10 + user_data_len + ((user_data_len + 15) // 16) * 2
            if len(self._recv_buf) < frame_size:
                return   # wait for the rest of the frame

            raw_frame      = self._recv_buf[:frame_size]
            self._recv_buf = self._recv_buf[frame_size:]
            parsed = parse_frame(raw_frame)
            if parsed:
                self._route(parsed)
            else:
                log.warning("Frame failed CRC check (%d bytes): %s",
                            len(raw_frame), raw_frame.hex())

    def _route(self, frame: dict) -> None:
        src = frame["src"]
        with self._slaves_lock: session = self._slaves.get(src)

        if session is None:
            fc = frame.get("function_code")
            registered = list(self._slaves.keys())
            log.warning(
                "Frame from unregistered src=%d (registered: %s) fc=0x%02X — "
                "auto-registering and confirming",
                src, registered, fc if fc is not None else 0xFF,
            )
            # Auto-register so we decode + print its data going forward
            session = self.add_slave(src)
            if self.on_unknown_slave:
                self.on_unknown_slave(frame)

        session._dispatch(frame)


# ===========================================================================
# CLI entry-point
# ===========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="DNP3 TCP master — decoded multi-slave",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("host",           help="Outstation / gateway IP address")
    parser.add_argument("port", type=int, help="TCP port (DNP3 default: 20000)")
    parser.add_argument("--master",  type=int,   default=1,    metavar="ADDR",
                        help="This master's DNP3 address")
    parser.add_argument("--slaves",  type=int,   nargs="+", default=[10], metavar="ADDR",
                        help="Outstation address(es) to communicate with")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="Socket connect/read timeout (seconds)")
    parser.add_argument("--reconnect-delay", type=float, default=5.0, metavar="SECS",
                        help="Seconds to wait before reconnecting after a drop")
    parser.add_argument("--keepalive", type=float, default=30.0, metavar="SECS",
                        help="Integrity poll interval to keep the session alive (seconds)")
    parser.add_argument("--debug",   action="store_true",
                        help="Enable DEBUG-level logging (show raw bytes)")
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    master = DNP3Master(
        args.host, args.port,
        master_addr        = args.master,
        timeout            = args.timeout,
        reconnect_delay    = args.reconnect_delay,
        keepalive_interval = args.keepalive,
    )

    for addr in args.slaves:
        master.add_slave(addr)

    try:
        master.connect()

        master._startup_sequence()

        log.info(
            "Running. Keepalive every %.0fs. Auto-reconnect after %.0fs. "
            "Press Ctrl+C to exit.",
            args.keepalive, args.reconnect_delay,
        )
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        master.disconnect()