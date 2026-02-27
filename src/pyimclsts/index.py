"""
Fast IMC log indexing utilities.

- Scan IMC log frames without deserializing payload fields.
- Create a compact index file with one fixed-size record per message:
  offset (uint64), timestamp (float64), msg_id (uint16), payload_size (uint16).
"""

from dataclasses import dataclass
import argparse
import os
import struct
from typing import Iterable, Iterator, Optional
import pyimc_generated as pg

from . import core as _core

# IMC header layout in pyimclsts core:
# sync (uint16), mgid (uint16), size (uint16), timestamp (fp64), src (uint16),
# src_ent (uint8), dst (uint16), dst_ent (uint8)
_HEADER_STRUCT_LE = struct.Struct("<HHHdHBHB")
_HEADER_STRUCT_BE = struct.Struct(">HHHdHBHB")

# Index record layout:
# offset (uint64), timestamp (float64), msg_id (uint16), payload_size (uint16)
_INDEX_RECORD_STRUCT = struct.Struct("<QdHH")


def _generated_frame_sizes() -> tuple[int, int]:
    header = pg._base.header_data(
        sync=pg._base._sync_number,
        mgid=0,
        size=0,
        timestamp=0.0,
        src=0,
        src_ent=0,
        dst=0,
        dst_ent=0,
    )
    header_size = len(_core.pack_functions_big["header"](*header))
    crc_size = len(_core.pack_functions_big["uint16_t"](0))
    return header_size, crc_size


_HEADER_SIZE, _CRC_SIZE = _generated_frame_sizes()
if _HEADER_STRUCT_LE.size != _HEADER_SIZE or _HEADER_STRUCT_BE.size != _HEADER_SIZE:
    raise RuntimeError(
        "Generated IMC header size does not match index parser header struct format."
    )


@dataclass
class IndexBuildResult:
    index_path: str
    records: int
    scanned_bytes: int
    invalid_sync_resyncs: int
    invalid_frames: int


@dataclass(frozen=True)
class IndexRecord:
    offset: int
    timestamp: float
    msg_id: int
    payload_size: int


def _default_index_path(log_path: str) -> str:
    return f"{log_path}.idx"


def _decode_header(header_bytes: bytes, sync_number: int):
    """
    Decode IMC header if sync matches; returns:
    (is_valid, is_little_endian, msg_id, payload_size, timestamp)
    """
    sync_le = sync_number.to_bytes(2, byteorder="little")
    sync_be = sync_number.to_bytes(2, byteorder="big")
    prefix = header_bytes[:2]

    if prefix == sync_le:
        fields = _HEADER_STRUCT_LE.unpack(header_bytes)
        return True, True, fields[1], fields[2], fields[3]
    if prefix == sync_be:
        fields = _HEADER_STRUCT_BE.unpack(header_bytes)
        return True, False, fields[1], fields[2], fields[3]
    return False, None, None, None, None


def build_index(
    log_path: str,
    index_path: Optional[str] = None,
    *,
    validate_crc: bool = False,
    sync_number: Optional[int] = None,
) -> IndexBuildResult:
    """
    Build an index from an IMC .lsf log file.

    Notes:
    - This function expects an uncompressed .lsf file.
    - When validate_crc=False (default), indexing is faster and trusts frame sizes.
    """
    if not os.path.isfile(log_path):
        raise FileNotFoundError(f"Log file not found: {log_path}")
    if log_path.endswith(".gz"):
        raise ValueError("Compressed logs are not supported for indexing. Use an uncompressed .lsf file.")
    if sync_number is None:
        sync_number = pg._base._sync_number

    if index_path is None:
        index_path = _default_index_path(log_path)

    file_size = os.path.getsize(log_path)
    records = 0
    invalid_sync_resyncs = 0
    invalid_frames = 0

    with open(log_path, "rb") as log_f, open(index_path, "wb") as idx_f:
        while True:
            offset = log_f.tell()
            if offset + _HEADER_SIZE > file_size:
                break

            header = log_f.read(_HEADER_SIZE)
            if len(header) < _HEADER_SIZE:
                break

            ok, is_little, msg_id, payload_size, timestamp = _decode_header(header, sync_number)
            if not ok:
                invalid_sync_resyncs += 1
                log_f.seek(offset + 1)
                continue

            frame_size = _HEADER_SIZE + payload_size + _CRC_SIZE
            next_offset = offset + frame_size

            # Likely false-positive sync inside payload; resync forward by one byte.
            if frame_size <= _HEADER_SIZE or next_offset > file_size:
                invalid_frames += 1
                log_f.seek(offset + 1)
                continue

            if validate_crc:
                payload_and_crc = log_f.read(payload_size + _CRC_SIZE)
                if len(payload_and_crc) < payload_size + _CRC_SIZE:
                    invalid_frames += 1
                    log_f.seek(offset + 1)
                    continue

                payload = payload_and_crc[:-2]
                crc_bytes = payload_and_crc[-2:]
                crc_value = int.from_bytes(crc_bytes, byteorder="little" if is_little else "big")
                if _core.CRC16IMB(header + payload) != crc_value:
                    invalid_frames += 1
                    log_f.seek(offset + 1)
                    continue
            else:
                # Skip payload + CRC without reading/parsing.
                log_f.seek(payload_size + _CRC_SIZE, os.SEEK_CUR)

            idx_f.write(_INDEX_RECORD_STRUCT.pack(offset, timestamp, msg_id, payload_size))
            records += 1

    return IndexBuildResult(
        index_path=index_path,
        records=records,
        scanned_bytes=file_size,
        invalid_sync_resyncs=invalid_sync_resyncs,
        invalid_frames=invalid_frames,
    )


def iter_index(index_path: str) -> Iterator[IndexRecord]:
    """
    Iterate index records from a binary .idx file.
    """
    if not os.path.isfile(index_path):
        raise FileNotFoundError(f"Index file not found: {index_path}")

    rec_size = _INDEX_RECORD_STRUCT.size
    with open(index_path, "rb") as idx_f:
        while True:
            chunk = idx_f.read(rec_size)
            if chunk == b"":
                break
            if len(chunk) != rec_size:
                raise ValueError("Corrupted index file: trailing partial record.")
            offset, timestamp, msg_id, payload_size = _INDEX_RECORD_STRUCT.unpack(chunk)
            yield IndexRecord(
                offset=offset,
                timestamp=timestamp,
                msg_id=msg_id,
                payload_size=payload_size,
            )


def load_index(index_path: str) -> Iterable[IndexRecord]:
    """
    Load all records from index file into memory.
    """
    return list(iter_index(index_path))


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="Build a fast IMC message index from an uncompressed .lsf log."
    )
    parser.add_argument("log_path", help="Path to .lsf file")
    parser.add_argument(
        "-o",
        "--output",
        dest="index_path",
        help="Output index path. Default: <log_path>.idx",
    )
    parser.add_argument(
        "--validate-crc",
        action="store_true",
        help="Validate CRC for each frame (safer, slower).",
    )
    args = parser.parse_args()

    result = build_index(
        log_path=args.log_path,
        index_path=args.index_path,
        validate_crc=args.validate_crc,
    )
    print(f"Index written to: {result.index_path}")
    print(f"Records: {result.records}")
    print(f"Scanned bytes: {result.scanned_bytes}")
    print(f"Resync events (invalid sync): {result.invalid_sync_resyncs}")
    print(f"Invalid frames: {result.invalid_frames}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
