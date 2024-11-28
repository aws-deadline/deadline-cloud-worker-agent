# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import time
import os
import boto3

from deadline.job_attachments import api

"""
A small script to upload job output. 

Example usage:

python attachment_upload.py \
    -pm /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/path_mapping.json \
    -s3 s3://test-job-attachment/DeadlineCloud \
    -m /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/manifests/0bb7eb91fdf8780c4a7e6174de6dfc5e_manifest \
    -m /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/manifests/0bb7eb91fdf8780c4a7e6174de6dfc5e_manifest
"""


def upload(s3_root_uri: str, path_mapping_rules: str, manifests: list[str]) -> None:
    s3_path = f"{os.environ.get('DEADLINE_FARM_ID')}/{os.environ.get('DEADLINE_QUEUE_ID')}/{os.environ.get('DEADLINE_JOB_ID')}/{os.environ.get('DEADLINE_STEP_ID')}/{os.environ.get('DEADLINE_TASK_ID')}/{os.environ.get('DEADLINE_SESSIONACTION_ID')}"
    api.attachment_upload(
        manifests=manifests,
        s3_root_uri=s3_root_uri,
        boto3_session=boto3.session.Session(profile_name=os.environ.get("AWS_PROFILE")),
        path_mapping_rules=path_mapping_rules,
        upload_manifest_path=s3_path,
    )


if __name__ == "__main__":
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument("-pm", "--path-mapping", type=str, help="", required=True)
    parser.add_argument("-s3", "--s3-uri", type=str, help="", required=True)
    parser.add_argument("-m", "--manifests", nargs="*", type=str, help="", required=True)

    args = parser.parse_args()
    path_mapping = args.path_mapping
    s3_uri = args.s3_uri
    manifests = args.manifests

    print("\nStarting upload...")
    upload(manifests=manifests, s3_root_uri=s3_uri, path_mapping_rules=path_mapping)

    total = time.perf_counter() - start_time
    print(f"Finished uploading after {total} seconds")
