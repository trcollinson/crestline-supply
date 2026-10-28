"""Fire drill: plant a sweep cut short in the raw bucket, and watch the merge refuse it.

The extractor files a read that stops early as `shortsweep`, which deletes
nothing. This drill skips that check, standing in for a bug that fools it:
it copies an entity's newest sweep, keeps only its first page, and uploads it
as the entity's newest read under a plain `sweep` name. Then

    uv run crestline curate Jobs

should refuse to merge it: the 5% check sees a MERGE that would delete every
row after page 1, writes `audit/merges/entity=Jobs/<read>-refused.json` to
the curated bucket (the keys, the table's snapshot, the read), emails the
alarm topic, and fails, with the table untouched. The poller's next run
heals it on its own: its sweep is a newer complete read, so the next merge
window starts there and the planted one is never merged.

    uv run python eval/fire_drill.py [ENTITY]           # plant (default Jobs)
    uv run python eval/fire_drill.py [ENTITY] --remove  # take the planted read out

AWS account and region come from `AWS_PROFILE` and `AWS_REGION`; the raw
bucket from FoundationStack's outputs.
"""

import argparse
import json

from erp_extract.curate import kind_of, new_reads, raw_read_key

# Planted reads take this place in their run, after any read the poller makes.
PLANTED = 900


def plant(s3, bucket: str, entity: str = "Jobs") -> str:
    """Upload the entity's newest sweep cut to its first page; returns its read name."""
    reads = new_reads(s3, bucket, entity, None)
    sweeps = [read for read in reads if kind_of(read) == "sweep"]
    if not sweeps:
        raise SystemExit(f"{entity} has no sweep to cut short")
    source = sweeps[-1]
    body = s3.get_object(Bucket=bucket, Key=raw_read_key(entity, source))["Body"]
    pages = body.read().decode("utf-8").splitlines()
    if len(pages) < 2:
        raise SystemExit(f"{source} is one page; a drill needs a sweep of several")
    # After the newest read, so it is the newest complete read the merge sees.
    run = reads[-1].split("-", 1)[0]
    planted = f"{run}-{PLANTED}-sweep"
    first = json.loads(pages[0]) | {"run": run, "read": "sweep"}
    s3.put_object(
        Bucket=bucket,
        Key=raw_read_key(entity, planted),
        Body=(json.dumps(first, separators=(",", ":")) + "\n").encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return planted


def remove(s3, bucket: str, entity: str = "Jobs") -> list[str]:
    """Delete every planted read of the entity; returns their names."""
    planted = [
        read
        for read in new_reads(s3, bucket, entity, None)
        if read.split("-")[1] == str(PLANTED)
    ]
    for read in planted:
        s3.delete_object(Bucket=bucket, Key=raw_read_key(entity, read))
    return planted


def main(argv: list[str] | None = None) -> None:
    import boto3

    from crestline.stack import foundation_outputs

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("entity", nargs="?", default="Jobs")
    parser.add_argument(
        "--remove", action="store_true", help="take the planted read out"
    )
    args = parser.parse_args(argv)

    s3 = boto3.client("s3")
    bucket = foundation_outputs()["RawBucketName"]
    if args.remove:
        removed = remove(s3, bucket, args.entity)
        print(f"Removed: {', '.join(removed) or 'nothing (none planted)'}")
        return
    planted = plant(s3, bucket, args.entity)
    print(f"Planted {planted}: one page of a sweep, filed as complete.")
    print(f"Now: uv run crestline curate {args.entity}  (expect it to refuse)")


if __name__ == "__main__":
    main()
