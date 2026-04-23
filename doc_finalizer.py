"""
doc_finalizer.py
----------------
Called by zoom-s3-salesforce-linker (or as standalone) after the final
Interview-Success S3 folder is known.

Given a meeting_id and the final S3 prefix (e.g.
  Interview-Success/John_Doe/2026/April/Rahul_Sharma/89423156782/Infosys/2026-04-23/Round_1/10-04-AM-IST/
), it:
  1. Reads temp/live-doc-history/<meeting_id>/doc.txt  from S3
  2. Copies it to  <final_prefix>/docs/doc.txt
  3. Copies all   temp/live-doc-history/<meeting_id>/images/* to <final_prefix>/docs/images/
  4. Copies       temp/live-doc-history/<meeting_id>/snapshots/* to <final_prefix>/docs/snapshots/
  5. Updates tracked_docs status to 'finalized'
  6. Optionally deletes temp prefix (configurable)

This module can be imported and called from zoom-s3-salesforce-linker Lambda,
OR run standalone from EC2.
"""

import os
import sys
import json
import logging
import boto3
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET    = os.environ.get("S3_BUCKET", "zoom-automation-bucket")
DB_HOST      = os.environ.get("DB_HOST", "localhost")
DB_NAME      = os.environ.get("DB_NAME", "dochistory")
DB_USER      = os.environ.get("DB_USER", "postgres")
DB_PASS      = os.environ.get("DB_PASS", "")
DB_PORT      = int(os.environ.get("DB_PORT", "5432"))
DELETE_TEMP  = os.environ.get("DELETE_TEMP_AFTER_FINALIZE", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("doc_finalizer")

s3 = boto3.client("s3", region_name=AWS_REGION)


# ──────────────────────────────────────────────
# DB
# ──────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


# ──────────────────────────────────────────────
# S3 HELPERS
# ──────────────────────────────────────────────
def list_s3_keys(prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def copy_s3_object(src_key: str, dst_key: str):
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": src_key},
        Key=dst_key
    )
    log.info(f"Copied s3://{S3_BUCKET}/{src_key}  →  s3://{S3_BUCKET}/{dst_key}")


def delete_s3_prefix(prefix: str):
    keys = list_s3_keys(prefix)
    if not keys:
        return
    s3.delete_objects(
        Bucket=S3_BUCKET,
        Delete={"Objects": [{"Key": k} for k in keys]}
    )
    log.info(f"Deleted {len(keys)} temp objects under s3://{S3_BUCKET}/{prefix}")


# ──────────────────────────────────────────────
# MAIN FINALIZE FUNCTION
# ──────────────────────────────────────────────
def finalize_docs(meeting_id: str, final_prefix: str) -> bool:
    """
    final_prefix example (NO trailing slash needed, we add it):
      Interview-Success/John_Doe/2026/April/Rahul_Sharma/89423156782/Infosys/2026-04-23/Round_1/10-04-AM-IST

    Docs will land at:
      Interview-Success/.../10-04-AM-IST/docs/doc.txt
      Interview-Success/.../10-04-AM-IST/docs/images/...
      Interview-Success/.../10-04-AM-IST/docs/snapshots/...
    """
    final_prefix = final_prefix.rstrip("/")
    temp_prefix  = f"temp/live-doc-history/{meeting_id}"
    docs_prefix  = f"{final_prefix}/docs"

    log.info(f"Finalizing docs for meeting_id={meeting_id}")
    log.info(f"  Temp:  s3://{S3_BUCKET}/{temp_prefix}/")
    log.info(f"  Final: s3://{S3_BUCKET}/{docs_prefix}/")

    # ── 1. Copy doc.txt ──
    doc_txt_src = f"{temp_prefix}/doc.txt"
    doc_txt_dst = f"{docs_prefix}/doc.txt"
    try:
        copy_s3_object(doc_txt_src, doc_txt_dst)
    except Exception as e:
        log.error(f"doc.txt not found in temp for meeting_id={meeting_id}: {e}")
        return False

    # ── 2. Copy images ──
    image_keys = list_s3_keys(f"{temp_prefix}/images/")
    for src_key in image_keys:
        filename  = src_key.split("/")[-1]
        copy_s3_object(src_key, f"{docs_prefix}/images/{filename}")

    # ── 3. Copy snapshots ──
    snapshot_keys = list_s3_keys(f"{temp_prefix}/snapshots/")
    for src_key in snapshot_keys:
        filename = src_key.split("/")[-1]
        copy_s3_object(src_key, f"{docs_prefix}/snapshots/{filename}")

    # ── 4. Update final doc.txt with correct S3 location ──
    try:
        obj      = s3.get_object(Bucket=S3_BUCKET, Key=doc_txt_dst)
        content  = obj["Body"].read().decode("utf-8")
        # Replace temp S3 Location line with final location
        content  = content.replace(
            f"s3://{S3_BUCKET}/{temp_prefix}/doc.txt",
            f"s3://{S3_BUCKET}/{doc_txt_dst}"
        )
        finalized_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        content += f"\n\n==================================================\nFINALIZED AT: {finalized_at}\nFinal S3 Path: s3://{S3_BUCKET}/{doc_txt_dst}\n"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=doc_txt_dst,
            Body=content.encode("utf-8"),
            ContentType="text/plain"
        )
        log.info(f"Updated final doc.txt S3 location line")
    except Exception as e:
        log.warning(f"Could not update S3 location in doc.txt: {e}")

    # ── 5. Mark finalized in DB ──
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE tracked_docs SET
                    status = 'finalized',
                    is_active = FALSE,
                    updated_at = NOW()
                WHERE meeting_id = %s
            """, (meeting_id,))
            conn.commit()
        conn.close()
        log.info(f"Marked meeting_id={meeting_id} as finalized in DB")
    except Exception as e:
        log.warning(f"DB update failed (non-fatal): {e}")

    # ── 6. Optionally delete temp ──
    if DELETE_TEMP:
        delete_s3_prefix(f"{temp_prefix}/")

    log.info(f"Finalization complete for meeting_id={meeting_id}")
    log.info(f"  doc.txt → s3://{S3_BUCKET}/{doc_txt_dst}")
    return True


# ──────────────────────────────────────────────
# STANDALONE CLI
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--meeting-id",    required=True)
    parser.add_argument("--final-prefix",  required=True,
        help='e.g. "Interview-Success/John/2026/April/Rahul/89423156782/Infosys/2026-04-23/Round_1/10-04-AM-IST"')
    args = parser.parse_args()
    ok = finalize_docs(args.meeting_id, args.final_prefix)
    sys.exit(0 if ok else 1)
