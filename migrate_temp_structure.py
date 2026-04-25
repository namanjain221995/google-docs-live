"""
migrate_temp_structure.py
-------------------------
One-time migration script.

Moves all existing flat temp folders:
  FROM: temp/live-doc-history/<meeting_id>/
  TO:   temp/live-doc-history/<YYYY>/<Month-M>/<YYYY-MM-DD>/<meeting_id>/

Date is taken from state.json initialized_at field.
If no state.json found, uses today's date as fallback.

Run ONCE on EC2:
  cd /home/ec2-user/google-docs-live
  source env
  python3.11 migrate_temp_structure.py
"""

import os
import sys
import json
import logging
import boto3
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET  = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
DRY_RUN    = os.environ.get("DRY_RUN", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("migrate")

s3 = boto3.client("s3", region_name=AWS_REGION)


def build_new_prefix(meeting_id: str, initialized_at: str) -> str:
    """
    Build new organized prefix from initialized_at datetime string.
    Example: 2026-04-24T13:27:34+00:00
    Result:  temp/live-doc-history/2026/Month-4/2026-04-24/<meeting_id>
    """
    try:
        dt = datetime.fromisoformat(initialized_at.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)

    year     = dt.strftime("%Y")
    month    = f"Month-{int(dt.strftime('%m'))}"
    date_str = dt.strftime("%Y-%m-%d")
    return f"temp/live-doc-history/{year}/{month}/{date_str}/{meeting_id}"


def get_all_flat_meetings() -> list[dict]:
    """
    Find all meetings in OLD flat structure:
    temp/live-doc-history/<meeting_id>/
    Skip any that are already in new structure (YYYY/Month-M/...)
    """
    paginator  = s3.get_paginator("list_objects_v2")
    found      = {}   # meeting_id -> list of keys

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
        for obj in page.get("Contents", []):
            key   = obj["Key"]
            parts = key.split("/")
            # OLD flat: temp/live-doc-history/<meeting_id>/file
            # parts[0]=temp, parts[1]=live-doc-history, parts[2]=meeting_id, parts[3]=file
            if len(parts) == 4 and parts[2].isdigit():
                meeting_id = parts[2]
                if meeting_id not in found:
                    found[meeting_id] = []
                found[meeting_id].append(key)

    result = []
    for meeting_id, keys in found.items():
        result.append({
            "meeting_id": meeting_id,
            "old_prefix": f"temp/live-doc-history/{meeting_id}",
            "keys":       keys
        })

    log.info(f"Found {len(result)} meetings in old flat structure")
    return result


def get_state_json(old_prefix: str, meeting_id: str) -> dict:
    """Read state.json to get initialized_at date."""
    try:
        obj  = s3.get_object(Bucket=S3_BUCKET, Key=f"{old_prefix}/state.json")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        log.warning(f"[{meeting_id}] No state.json found, using today's date")
        return {}


def copy_object(src_key: str, dst_key: str):
    """Copy one S3 object."""
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": src_key},
        Key=dst_key
    )


def delete_object(key: str):
    """Delete one S3 object."""
    s3.delete_object(Bucket=S3_BUCKET, Key=key)


def migrate_one(item: dict) -> str:
    meeting_id = item["meeting_id"]
    old_prefix = item["old_prefix"]
    keys       = item["keys"]

    # Read state.json for the date
    state          = get_state_json(old_prefix, meeting_id)
    initialized_at = state.get("initialized_at", datetime.now(timezone.utc).isoformat())
    new_prefix     = build_new_prefix(meeting_id, initialized_at)

    # Skip if already in new location
    if old_prefix == new_prefix:
        return f"SKIP  {meeting_id} — already in correct location"

    # Check if already migrated
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=f"{new_prefix}/state.json")
        log.info(f"[{meeting_id}] Already migrated to {new_prefix}")
        # Delete old keys
        if not DRY_RUN:
            for key in keys:
                delete_object(key)
        return f"CLEAN {meeting_id} — already in new location, deleted old"
    except Exception:
        pass  # not yet migrated

    if DRY_RUN:
        log.info(f"[DRY RUN] Would migrate {meeting_id}:")
        log.info(f"  FROM: {old_prefix}/")
        log.info(f"  TO:   {new_prefix}/")
        return f"DRY   {meeting_id} → {new_prefix}"

    # Copy all files to new location
    copied = 0
    for old_key in keys:
        filename = old_key[len(old_prefix)+1:]  # get relative path after prefix
        new_key  = f"{new_prefix}/{filename}"
        try:
            copy_object(old_key, new_key)
            copied += 1
        except Exception as e:
            log.error(f"[{meeting_id}] Failed to copy {old_key}: {e}")

    # Update state.json with new prefix
    if state:
        state["temp_s3_prefix"] = new_prefix
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"{new_prefix}/state.json",
                Body=json.dumps(state, indent=2),
                ContentType="application/json"
            )
        except Exception as e:
            log.error(f"[{meeting_id}] Failed to update state.json: {e}")

    # Delete old files
    deleted = 0
    for old_key in keys:
        try:
            delete_object(old_key)
            deleted += 1
        except Exception as e:
            log.warning(f"[{meeting_id}] Failed to delete {old_key}: {e}")

    return f"OK    {meeting_id} | copied={copied} deleted={deleted} → {new_prefix}"


def main():
    log.info("=" * 60)
    log.info("migrate_temp_structure.py starting")
    if DRY_RUN:
        log.info("DRY RUN MODE — no changes will be made")
    log.info("=" * 60)

    meetings = get_all_flat_meetings()

    if not meetings:
        log.info("No meetings to migrate. All already in new structure.")
        return

    log.info(f"Migrating {len(meetings)} meetings with 20 parallel workers...")
    results = {"ok": 0, "skip": 0, "clean": 0, "dry": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=20, thread_name_prefix="migrate") as executor:
        futures = {executor.submit(migrate_one, item): item["meeting_id"] for item in meetings}
        for future in as_completed(futures):
            mid = futures[future]
            try:
                result = future.result()
                log.info(result)
                if result.startswith("OK"):
                    results["ok"] += 1
                elif result.startswith("SKIP"):
                    results["skip"] += 1
                elif result.startswith("CLEAN"):
                    results["clean"] += 1
                elif result.startswith("DRY"):
                    results["dry"] += 1
            except Exception as e:
                log.error(f"[{mid}] Migration error: {e}")
                results["error"] += 1

    log.info("=" * 60)
    log.info("MIGRATION COMPLETE")
    log.info(f"  Migrated:  {results['ok']}")
    log.info(f"  Skipped:   {results['skip']}")
    log.info(f"  Cleaned:   {results['clean']}")
    log.info(f"  Dry run:   {results['dry']}")
    log.info(f"  Errors:    {results['error']}")
    log.info("=" * 60)
    log.info("""
New structure:
  temp/live-doc-history/
      2026/
          Month-4/
              2026-04-24/
                  <meeting_id>/
                      state.json
                      doc.txt
                      done.json
""")


if __name__ == "__main__":
    main()