"""
cleanup_llm.py
Deletes ALL LLM output files:
  Step 1: Delete everything inside llm/ folder from Interview-Success
          (llm.txt, LLM.txt, LLM_result.json, any file inside llm/)
  Step 2: Delete llm-done.json from temp/
"""

import os
import boto3

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET  = os.environ.get("S3_BUCKET", "zoom-automation-bucket")

s3        = boto3.client("s3", region_name=AWS_REGION)
paginator = s3.get_paginator("list_objects_v2")

# ── STEP 1: Delete ALL files inside llm/ folders ──
print("=" * 60)
print("STEP 1: Deleting all files inside llm/ from Interview-Success...")
print("=" * 60)

llm_files_deleted = 0
for dept in ["Interview-Success", "Training", "Customer-Success", "Marketing"]:
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{dept}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Match anything inside a llm/ folder
            if "/llm/" in key:
                s3.delete_object(Bucket=S3_BUCKET, Key=key)
                print(f"  Deleted: {key}")
                llm_files_deleted += 1

print(f"\nStep 1 complete: {llm_files_deleted} llm files deleted")

# ── STEP 2: Delete llm-done.json from temp ──
print()
print("=" * 60)
print("STEP 2: Deleting llm-done.json from temp/...")
print("=" * 60)

llm_done_deleted = 0
for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="temp/live-doc-history/"):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith("llm-done.json"):
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            print(f"  Deleted: {key}")
            llm_done_deleted += 1

print(f"\nStep 2 complete: {llm_done_deleted} llm-done.json deleted")

print()
print("=" * 60)
print("CLEANUP COMPLETE")
print(f"  LLM files deleted:     {llm_files_deleted}")
print(f"  llm-done.json deleted: {llm_done_deleted}")
print("=" * 60)
