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


class TestChunkingF12:
    def test_at_flag_chunk_round_trip(self) -> None:
        """F-12: >1MB @-flag messages reassemble like any other flag."""
        payload = b"q" * (128 * 1024)  # force chunking with a small chunk size
        frames = encode_message("@rpc", payload, chunk_size=16 * 1024)
        assert len(frames) > 1
        out = FrameDecoder().feed(b"".join(frames))
        assert len(out) == 1
        assert out[0].flag == "@rpc"
        assert out[0].payload == payload

    def test_at_flag_default_chunk_size_round_trip(self) -> None:
        payload = b"r" * (1024 * 1024 + 5)  # just over the 1MB default
        blob = b"".join(encode_message("@fwd", payload))
        out = FrameDecoder().feed(blob)
        assert len(out) == 1 and out[0].payload == payload

    def test_non_utf8_flag_raises_protocol_error(self) -> None:
        from pyline.net.protocol import MAGIC, VERSION

        blob = bytearray()
        blob += MAGIC
        blob.append(VERSION)
        blob.append(0)
        blob.append(2)  # flag length
        blob += (0).to_bytes(4, "big")
        blob += b"\xff\xfe"  # invalid utf-8 flag bytes
        with pytest.raises(ProtocolError, match="utf-8"):
            FrameDecoder().feed(bytes(blob))


class TestDecoderCursorAndFlagsF78:
    def test_unknown_flag_bits_rejected(self) -> None:
        """F-78: only bit 0 of the flags byte is defined. A frame setting any
        other bit is a protocol revision this decoder cannot interpret --
        rejecting it beats silently misreading the message."""
        from pyline.net.protocol import MAGIC, VERSION

        for bad in (0x02, 0x80, 0x03, 0xFE):
            blob = bytearray()
            blob += MAGIC
            blob.append(VERSION)
            blob.append(bad)  # unknown flag bits
            blob.append(1)
            blob += (1).to_bytes(4, "big")
            blob += b"g"
            blob += b"x"
            with pytest.raises(ProtocolError, match="flags"):
                FrameDecoder().feed(bytes(blob))

    def test_known_flags_still_accepted(self) -> None:
        from pyline.net.protocol import MAGIC, VERSION

        for good in (0x00, 0x01):
            decoder = FrameDecoder()
            blob = bytearray()
            blob += MAGIC
            blob.append(VERSION)
            blob.append(good)
            blob.append(1)
            blob += (1).to_bytes(4, "big")
            blob += b"g"
            blob += b"x"
            frames = decoder.feed(bytes(blob))
            if good == 0x00:
                assert [(f.flag, f.payload) for f in frames] == [("g", b"x")]
            else:
                # bit0 = CHUNK_MORE: the frame is held for reassembly and
                # nothing is emitted until the terminating chunk arrives
                assert frames == []
                tail = bytearray()
                tail += MAGIC
                tail.append(VERSION)
                tail.append(0)  # final chunk
                tail.append(1)
                tail += (1).to_bytes(4, "big")
                tail += b"g"
                tail += b"y"
                frames = decoder.feed(bytes(tail))
                assert [(f.flag, f.payload) for f in frames] == [("g", b"xy")]

    def test_offset_cursor_survives_partial_frames_and_compaction(self) -> None:
        """F-78: the offset-cursor rewrite must behave exactly like the old
        per-frame ``del buffer[:total]`` -- several complete frames, a
        partial frame, more bytes, then chunked reassembly across feeds."""
        decoder = FrameDecoder(max_frame=64 * 1024)
        first = b"".join(encode_message("a", b"1"))
        second = b"".join(encode_message("b", b"22"))
        chunked = encode_message("c", b"z" * 3000, chunk_size=1024)

        blob = first + second + chunked[0][:5]  # 5 stray bytes of frame 3
        out = decoder.feed(blob)
        assert [(f.flag, f.payload) for f in out] == [("a", b"1"), ("b", b"22")]
        # feed the remainder of chunk frame 0 plus the rest in odd splits
        rest = chunked[0][5:] + b"".join(chunked[1:])
        for i in range(0, len(rest), 7):
            out = decoder.feed(rest[i : i + 7])
            if i + 7 >= len(rest):
                assert len(out) == 1
                assert out[0].flag == "c"
                assert out[0].payload == b"z" * 3000
            else:
                assert out == []
        # decoder is empty again: nothing retained after compaction
        assert decoder.feed(b"") == []

    def test_many_frames_in_one_feed_all_decode(self) -> None:
        """F-78: stress the cursor across a large single feed (the old code
        re-memmoved the whole tail once per frame -- quadratic)."""
        decoder = FrameDecoder()
        blob = b"".join(b"".join(encode_message("s", bytes([i % 256]) * 8)) for i in range(500))
        frames = decoder.feed(blob)
        assert len(frames) == 500
        assert frames[0].payload == b"\x00" * 8
        assert frames[-1].payload == bytes([499 % 256]) * 8
        assert decoder._offset == 0  # fully compacted after feed


class TestDecodeCapsF125:
    """decode_payload bounds msgpack expansion: a small frame must not be
    able to inflate into gigabytes of Python objects."""

    def test_oversized_array_rejected(self) -> None:
        import msgpack
        import pytest as _pytest

        from pyline.net.protocol import decode_payload

        bomb = msgpack.packb([0] * (1_048_576 + 1), use_bin_type=True)
        with _pytest.raises(ValueError):
            decode_payload(bomb)

    def test_oversized_map_rejected(self) -> None:
        import msgpack
        import pytest as _pytest

        from pyline.net.protocol import decode_payload

        bomb = msgpack.packb({i: 0 for i in range(1_048_576 + 1)}, use_bin_type=True)
        with _pytest.raises(ValueError):
            decode_payload(bomb)

    def test_normal_payloads_unaffected(self) -> None:
        from pyline.net.protocol import decode_payload

        assert decode_payload(msgpack_packb(["ok", {"k": 1}])) == ["ok", {"k": 1}]

    def test_set_max_frame_only_widens(self) -> None:
        decoder = FrameDecoder(max_frame=1024)
        decoder.set_max_frame(4096)
        assert decoder._max_frame == 4096
        # narrowing is a no-op: authenticated peers must not have later legal
        # frames rejected by accounting only
        decoder.set_max_frame(512)
        assert decoder._max_frame == 4096


def msgpack_packb(value: object) -> bytes:
    import msgpack

    return msgpack.packb(value, use_bin_type=True)
