"""Frame protocol: round-trip, chunking, malformed input."""

from __future__ import annotations

import pytest

from pyline.net.protocol import (
    FrameDecoder,
    ProtocolError,
    encode_message,
)


class TestEncodeDecode:
    def test_round_trip(self) -> None:
        frames = encode_message("game", b"\x01\x02\x03")
        decoder = FrameDecoder()
        out = decoder.feed(b"".join(frames))
        assert len(out) == 1
        assert out[0].flag == "game"
        assert out[0].payload == b"\x01\x02\x03"
        assert out[0].chunk_more is False

    def test_split_feeding(self) -> None:
        blob = b"".join(encode_message("rpc", b"x" * 100))
        decoder = FrameDecoder()
        out: list = []
        for i in range(len(blob)):
            out.extend(decoder.feed(blob[i : i + 1]))
        assert len(out) == 1
        assert out[0].payload == b"x" * 100

    def test_multiple_messages_in_one_feed(self) -> None:
        blob = b"".join(encode_message("a", b"1")) + b"".join(encode_message("b", b"22"))
        out = FrameDecoder().feed(blob)
        assert [(f.flag, f.payload) for f in out] == [("a", b"1"), ("b", b"22")]

    def test_chunking_round_trip(self) -> None:
        payload = b"z" * (1024 * 1024 + 17)  # over the default chunk size
        frames = encode_message("big", payload, chunk_size=1024)
        assert len(frames) > 2
        # All but the last frame carry CHUNK_MORE.
        decoder = FrameDecoder(max_frame=10 * 1024 * 1024)
        out = decoder.feed(b"".join(frames))
        assert len(out) == 1
        assert out[0].payload == payload
        assert out[0].flag == "big"

    def test_empty_flag_rejected(self) -> None:
        with pytest.raises(ProtocolError):
            encode_message("", b"")


class TestMalformed:
    def test_bad_magic(self) -> None:
        with pytest.raises(ProtocolError, match="magic"):
            FrameDecoder().feed(b"XX" + b"\x00" * 20)

    def test_bad_version(self) -> None:
        header = b"PL" + bytes([99]) + bytes([0]) + bytes([1]) + (1).to_bytes(4, "big")
        with pytest.raises(ProtocolError, match="version"):
            FrameDecoder().feed(header + b"g")

    def test_oversized_frame(self) -> None:
        header = b"PL" + bytes([1]) + bytes([0]) + bytes([1]) + (10**9).to_bytes(4, "big")
        with pytest.raises(ProtocolError, match="exceeds"):
            FrameDecoder().feed(header)

    def test_partial_header_waits(self) -> None:
        assert FrameDecoder().feed(b"PL\x01") == []
