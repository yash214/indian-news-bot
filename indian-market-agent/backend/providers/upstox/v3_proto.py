from __future__ import annotations

import struct


TYPE_NAMES = {
    0: "initial_feed",
    1: "live_feed",
    2: "market_info",
}

REQUEST_MODE_NAMES = {
    0: "ltpc",
    1: "full_d5",
    2: "option_greeks",
    3: "full_d30",
}

MARKET_STATUS_NAMES = {
    0: "PRE_OPEN_START",
    1: "PRE_OPEN_END",
    2: "NORMAL_OPEN",
    3: "NORMAL_CLOSE",
    4: "CLOSING_START",
    5: "CLOSING_END",
}


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError("Unexpected end of protobuf varint")
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, offset
        shift += 7
        if shift > 63:
            raise ValueError("Invalid protobuf varint")


def _iter_fields(data: bytes):
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 0:
            value, offset = _read_varint(data, offset)
        elif wire_type == 1:
            if offset + 8 > len(data):
                raise ValueError("Unexpected end of protobuf fixed64")
            value = data[offset:offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(data, offset)
            if offset + length > len(data):
                raise ValueError("Unexpected end of protobuf length-delimited field")
            value = data[offset:offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(data):
                raise ValueError("Unexpected end of protobuf fixed32")
            value = data[offset:offset + 4]
            offset += 4
        else:
            raise ValueError(f"Unsupported protobuf wire type: {wire_type}")
        yield field_number, wire_type, value


def _double(raw: bytes) -> float:
    return struct.unpack("<d", raw)[0]


def _text(raw: bytes) -> str:
    return raw.decode("utf-8", "ignore")


def _decode_ltpc(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 1:
            out["ltp"] = _double(value)
        elif field_number == 2 and wire_type == 0:
            out["ltt"] = int(value)
        elif field_number == 3 and wire_type == 0:
            out["ltq"] = int(value)
        elif field_number == 4 and wire_type == 1:
            out["cp"] = _double(value)
    return out


def _decode_quote(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 0:
            out["bidQ"] = int(value)
        elif field_number == 2 and wire_type == 1:
            out["bidP"] = _double(value)
        elif field_number == 3 and wire_type == 0:
            out["askQ"] = int(value)
        elif field_number == 4 and wire_type == 1:
            out["askP"] = _double(value)
    return out


def _decode_market_level(data: bytes) -> dict:
    quotes = []
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            quotes.append(_decode_quote(value))
    return {"bidAskQuote": quotes}


def _decode_option_greeks(data: bytes) -> dict:
    out = {}
    fields = {
        1: "delta",
        2: "theta",
        3: "gamma",
        4: "vega",
        5: "rho",
    }
    for field_number, wire_type, value in _iter_fields(data):
        name = fields.get(field_number)
        if name and wire_type == 1:
            out[name] = _double(value)
    return out


def _decode_ohlc(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            out["interval"] = _text(value)
        elif field_number == 2 and wire_type == 1:
            out["open"] = _double(value)
        elif field_number == 3 and wire_type == 1:
            out["high"] = _double(value)
        elif field_number == 4 and wire_type == 1:
            out["low"] = _double(value)
        elif field_number == 5 and wire_type == 1:
            out["close"] = _double(value)
        elif field_number == 6 and wire_type == 0:
            out["vol"] = int(value)
        elif field_number == 7 and wire_type == 0:
            out["ts"] = int(value)
    return out


def _decode_market_ohlc(data: bytes) -> dict:
    rows = []
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            rows.append(_decode_ohlc(value))
    return {"ohlc": rows}


def _decode_market_full_feed(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            out["ltpc"] = _decode_ltpc(value)
        elif field_number == 2 and wire_type == 2:
            out["marketLevel"] = _decode_market_level(value)
        elif field_number == 3 and wire_type == 2:
            out["optionGreeks"] = _decode_option_greeks(value)
        elif field_number == 4 and wire_type == 2:
            out["marketOHLC"] = _decode_market_ohlc(value)
        elif field_number == 5 and wire_type == 1:
            out["atp"] = _double(value)
        elif field_number == 6 and wire_type == 0:
            out["vtt"] = int(value)
        elif field_number == 7 and wire_type == 1:
            out["oi"] = _double(value)
        elif field_number == 8 and wire_type == 1:
            out["iv"] = _double(value)
        elif field_number == 9 and wire_type == 1:
            out["tbq"] = _double(value)
        elif field_number == 10 and wire_type == 1:
            out["tsq"] = _double(value)
    return out


def _decode_index_full_feed(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            out["ltpc"] = _decode_ltpc(value)
        elif field_number == 2 and wire_type == 2:
            out["marketOHLC"] = _decode_market_ohlc(value)
    return out


def _decode_full_feed(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            out["marketFF"] = _decode_market_full_feed(value)
        elif field_number == 2 and wire_type == 2:
            out["indexFF"] = _decode_index_full_feed(value)
    return out


def _decode_first_level_with_greeks(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            out["ltpc"] = _decode_ltpc(value)
        elif field_number == 2 and wire_type == 2:
            out["firstDepth"] = _decode_quote(value)
        elif field_number == 3 and wire_type == 2:
            out["optionGreeks"] = _decode_option_greeks(value)
        elif field_number == 4 and wire_type == 0:
            out["vtt"] = int(value)
        elif field_number == 5 and wire_type == 1:
            out["oi"] = _double(value)
        elif field_number == 6 and wire_type == 1:
            out["iv"] = _double(value)
    return out


def _decode_feed(data: bytes) -> dict:
    out = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 2:
            out["ltpc"] = _decode_ltpc(value)
        elif field_number == 2 and wire_type == 2:
            out["fullFeed"] = _decode_full_feed(value)
        elif field_number == 3 and wire_type == 2:
            out["firstLevelWithGreeks"] = _decode_first_level_with_greeks(value)
        elif field_number == 4 and wire_type == 0:
            out["requestMode"] = REQUEST_MODE_NAMES.get(int(value), int(value))
    return out


def _decode_market_info(data: bytes) -> dict:
    segment_status = {}
    for field_number, wire_type, value in _iter_fields(data):
        if field_number != 1 or wire_type != 2:
            continue
        key = None
        status = None
        for entry_field, entry_wire, entry_value in _iter_fields(value):
            if entry_field == 1 and entry_wire == 2:
                key = _text(entry_value)
            elif entry_field == 2 and entry_wire == 0:
                status = MARKET_STATUS_NAMES.get(int(entry_value), int(entry_value))
        if key is not None and status is not None:
            segment_status[key] = status
    return {"segmentStatus": segment_status}


def decode_feed_response(data: bytes) -> dict:
    payload = {
        "type": None,
        "feeds": {},
        "currentTs": None,
        "marketInfo": {},
    }
    for field_number, wire_type, value in _iter_fields(data):
        if field_number == 1 and wire_type == 0:
            payload["type"] = TYPE_NAMES.get(int(value), int(value))
        elif field_number == 2 and wire_type == 2:
            key = None
            feed_value = {}
            for entry_field, entry_wire, entry_value in _iter_fields(value):
                if entry_field == 1 and entry_wire == 2:
                    key = _text(entry_value)
                elif entry_field == 2 and entry_wire == 2:
                    feed_value = _decode_feed(entry_value)
            if key is not None:
                payload["feeds"][key] = feed_value
        elif field_number == 3 and wire_type == 0:
            payload["currentTs"] = int(value)
        elif field_number == 4 and wire_type == 2:
            payload["marketInfo"] = _decode_market_info(value)
    return payload
