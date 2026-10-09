"""Wire protocol: frame encoding/decoding with fragmentation support.

Frame layout (all integers big-endian)::

    magic       2 bytes   b"PL"
    version     1 byte    currently 1
    flags       1 byte    bit0: CHUNK_MORE -- more chunks follow for this message
    flag_len    1 byte    length of the protocol name
    flag        n bytes   utf-8 protocol name (e.g. b"game", b"@rpc")
    payload_len 4 bytes   length of this chunk
    payload     n bytes   msgpack payload chunk

Messages larger than ``chunk_size`` are transparently split into chunks;
the decoder reassembles them. ``pickle`` is never used on the wire.
"""

from __future__ import annotations

from dataclasses import dataclass

MAGIC = b"PL"
VERSION = 1
FLAG_CHUNK_MORE = 0x01
# F-78: only bit 0 of the flags byte is defined today. A peer setting any
# other bit is speaking a future/foreign protocol revision -- honouring the
# frame anyway would let that revision's messages be misinterpreted instead
# of loudly rejected, so the decoder refuses unknown bits.
KNOWN_FLAGS_MASK = FLAG_CHUNK_MORE
HEADER_BASE = 9  # magic(2) + version(1) + flags(1) + flag_len(1) + payload_len(4)
MAX_FLAG_LEN = 255
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_MAX_FRAME = 16 * 1024 * 1024


class ProtocolError(Exception):
    """Malformed frame: bad magic/version/lengths or oversized message."""


@dataclass(slots=True)
class Frame:
    flag: str
    payload: bytes
    chunk_more: bool = False


def encode_message(
    flag: str,
    payload: bytes,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> list[bytes]:
    """Encode one logical message, splitting into chunked frames as needed."""
    flag_bytes = flag.encode("utf-8")
    if not 0 < len(flag_bytes) <= MAX_FLAG_LEN:
        raise ProtocolError(f"flag length out of range (1..{MAX_FLAG_LEN}): {flag!r}")
    if chunk_size < 1:
        raise ProtocolError("chunk_size must be >= 1")
    frames: list[bytes] = []
    if len(payload) <= chunk_size:
        frames.append(_encode_frame(flag_bytes, payload, more=False))
        return frames
    offset = 0
    total = len(payload)
    while offset < total:
        chunk = payload[offset : offset + chunk_size]
        offset += len(chunk)
        frames.append(_encode_frame(flag_bytes, chunk, more=offset < total))
    return frames


def _encode_frame(flag: bytes, payload: bytes, *, more: bool) -> bytes:
    header = bytearray()
    header += MAGIC
    header.append(VERSION)
    header.append(FLAG_CHUNK_MORE if more else 0)
    header.append(len(flag))
    header += len(payload).to_bytes(4, "big")
    return bytes(header + flag + payload)


class FrameDecoder:
    """Incremental decoder: feed() raw socket bytes, receive decoded messages.

    F-78: consumed bytes are tracked with an offset cursor and dropped in one
    ``del`` per feed() call. The previous ``del buffer[:total]`` after every
    frame was O(remaining bytes) per frame -- quadratic behaviour on a stream
    of small frames arriving in large TCP segments (a busy gateway easily
    pushes memmoves of megabytes per read).
    """

    def __init__(self, *, max_frame: int = DEFAULT_MAX_FRAME) -> None:
        self._buffer = bytearray()
        self._offset = 0
        self._max_frame = max_frame
        self._pending_flag: str | None = None
        self._pending_data = bytearray()
        self._pending_size = 0

    def feed(self, data: bytes | bytearray | memoryview) -> list[Frame]:
        self._buffer += data
        frames: list[Frame] = []
        while True:
            frame = self._try_decode_one()
            if frame is None:
                break
            # Reassembly is a transport concern and applies to EVERY flag,
            # ``@``-prefixed control/RPC messages included (F-12): the
            # encoder chunks them like anything else, so bypassing
            # reassembly here used to corrupt >1MB RPC messages silently.
            if frame.chunk_more:
                self._accumulate_chunk(frame)
                continue
            if self._pending_flag is not None:
                self._accumulate_chunk(frame)
                frames.append(Frame(flag=self._pending_flag, payload=bytes(self._pending_data)))
                self._reset_pending()
            else:
                frames.append(frame)
        # F-78: single compaction per feed() instead of a memmove per frame.
        if self._offset:
            del self._buffer[: self._offset]
            self._offset = 0
        return frames

    def _try_decode_one(self) -> Frame | None:
        buffer = self._buffer
        offset = self._offset
        if len(buffer) - offset < HEADER_BASE:
            return None
        if buffer[offset] != MAGIC[0] or buffer[offset + 1] != MAGIC[1]:
            raise ProtocolError("bad frame magic; peer is not speaking the pyline protocol")
        version = buffer[offset + 2]
        if version != VERSION:
            raise ProtocolError(f"unsupported protocol version {version}")
        flags = buffer[offset + 3]
        if flags & ~KNOWN_FLAGS_MASK:
            # F-78: unknown flag bits are a protocol revision we cannot parse.
            raise ProtocolError(
                f"unknown frame flags 0x{flags:02x} (defined mask 0x{KNOWN_FLAGS_MASK:02x})"
            )
        flag_len = buffer[offset + 4]
        if flag_len == 0:
            raise ProtocolError("flag length is zero")
        payload_len = int.from_bytes(buffer[offset + 5 : offset + 9], "big")
        total = HEADER_BASE + flag_len + payload_len
        if payload_len > self._max_frame:
            raise ProtocolError(
                f"frame payload {payload_len} exceeds max frame size {self._max_frame}"
            )
        if self._pending_size + payload_len > self._max_frame:
            raise ProtocolError("reassembled message exceeds max frame size")
        if len(buffer) - offset < total:
            return None
        try:
            flag = bytes(buffer[offset + HEADER_BASE : offset + HEADER_BASE + flag_len]).decode(
                "utf-8"
            )
        except UnicodeDecodeError as exc:
            raise ProtocolError(f"flag is not valid utf-8: {exc}") from exc
        payload = bytes(buffer[offset + HEADER_BASE + flag_len : offset + total])
        self._offset = offset + total
        return Frame(flag=flag, payload=payload, chunk_more=bool(flags & FLAG_CHUNK_MORE))

    def _accumulate_chunk(self, frame: Frame) -> None:
        if self._pending_flag is None:
            self._pending_flag = frame.flag
            self._pending_data = bytearray()
        elif frame.flag != self._pending_flag:
            raise ProtocolError(
                f"interleaved chunked messages: {self._pending_flag!r} vs {frame.flag!r}"
            )
        self._pending_data += frame.payload
        self._pending_size = len(self._pending_data)

    def _reset_pending(self) -> None:
        self._pending_flag = None
        self._pending_data = bytearray()
        self._pending_size = 0
