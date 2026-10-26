"""Raw pages, exactly as the API served them, one file per read.

Layout (spec §9), under a local folder (`RawWriter`) or the raw bucket
(`aws.S3RawWriter`), the same either way:

    source=erp-api/entity=<Entity>/ingest_date=<YYYY-MM-DD>/<run>-<nnn>-<read>.jsonl

`run` is the ERP's time when the run signed in (`20260706T050000Z`), `nnn`
the read's place in the run, and `read` what kind of read it was. Names sort
in the order the reads happened. Each line is one page, wrapped with what is
needed to replay it: the entity, the run, the kind of read, the page's
number, the URL, when it was asked for (the extractor's clock), when the ERP
says it answered (its `Date` header), and the body untouched.

A file is written only once its read has finished: under a temporary name
first, then renamed. So a file's presence means its read was complete, which
is what lets a sweep (a full read) stand for everything the ERP held.
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path

SOURCE = "source=erp-api"


@dataclass(frozen=True)
class Page:
    entity: str
    run: str
    read: str  # full, incremental, sequence, or sweep
    page: int
    url: str
    requested_at: str  # the extractor's clock, ISO 8601 UTC
    server_time: str  # the ERP's Date header, ISO 8601 UTC
    body: dict


def raw_key(pages: list[Page], ordinal: int, ingest_date: str) -> str:
    """Where a read goes, relative to the folder or the bucket."""
    first = pages[0]
    return (
        f"{SOURCE}/entity={first.entity}/ingest_date={ingest_date}/"
        f"{first.run}-{ordinal:03d}-{first.read}.jsonl"
    )


def ingest_date_of(read: str) -> str:
    """The partition a read is filed under, from its name: its run's date.

    `20260806T061351Z-001-sweep` -> `2026-08-06`.
    """
    return f"{read[0:4]}-{read[4:6]}-{read[6:8]}"


def encode(pages: list[Page]) -> bytes:
    """A read's pages, one JSON line each."""
    lines = (json.dumps(asdict(page), separators=(",", ":")) for page in pages)
    return "".join(line + "\n" for line in lines).encode("utf-8")


class RawWriter:
    """Raw reads into a local folder."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, pages: list[Page], ordinal: int, ingest_date: str) -> Path:
        """Write one completed read; returns the file."""
        path = self.root / raw_key(pages, ordinal, ingest_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_suffix(".partial")
        partial.write_bytes(encode(pages))
        partial.rename(path)
        return path


def read_files(root: Path, entity: str) -> list[Path]:
    """An entity's completed reads, in the order they were made."""
    folder = root / SOURCE / f"entity={entity}"
    return sorted(folder.glob("ingest_date=*/*.jsonl"), key=lambda path: path.name)


def pages_in(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]
