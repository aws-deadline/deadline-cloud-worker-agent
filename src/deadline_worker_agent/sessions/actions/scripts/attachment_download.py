# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import time
import boto3

from deadline.job_attachments import api

"""
A small script to download job output using attachment download.
This is available in deadline-cloud as python API and AWS Deadline Cloud CLI.

Example usage:

python attachment_download.py \
    -pm /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/path_mapping.json \
    -s3 s3://test-job-attachment/DeadlineCloud \
    -m /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/manifests/0bb7eb91fdf8780c4a7e6174de6dfc5e_manifest \
    -m /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/manifests/0bb7eb91fdf8780c4a7e6174de6dfc5e_manifest
"""


def download(s3_root_uri: str, path_mapping_rules: str, manifests: list[str]) -> None:
    api.attachment_download(
        manifests=manifests,
        s3_root_uri=s3_root_uri,
        boto3_session=boto3.session.Session(),
        path_mapping_rules=path_mapping_rules,
    )


if __name__ == "__main__":
    start_time = time.perf_counter()

    parser = argparse.ArgumentParser()
    parser.add_argument("-pm", "--path-mapping", type=str, help="", required=True)
    parser.add_argument("-s3", "--s3-uri", type=str, help="", required=True)
    parser.add_argument("-m", "--manifests", nargs="*", type=str, help="", required=True)

    args = parser.parse_args()
    path_mapping = args.path_mapping
    manifests = args.manifests
    s3_uri = args.s3_uri

    print("\nStarting download...")
    download(s3_root_uri=s3_uri, path_mapping_rules=path_mapping, manifests=manifests)

    total = time.perf_counter() - start_time
    print(f"Finished downloading after {total} seconds")
