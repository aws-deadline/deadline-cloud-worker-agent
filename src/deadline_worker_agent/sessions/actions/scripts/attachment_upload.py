# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import sys
import time
import os
import boto3
import json
from typing import Optional

from deadline.job_attachments import api
from deadline.job_attachments.api.manifest import _manifest_snapshot, _manifest_merge
from deadline.job_attachments.models import ManifestSnapshot, ManifestMerge, JobAttachmentS3Settings

"""
A small script to
1. capture the difference since the given base input manifest to generate manifests via manifest snapshot
2. upload job output based on the diff manifests using attachment upload

The manifest snapshot and attachment upload commands are available in deadline-cloud as python API and AWS Deadline Cloud CLI.

Example usage:

python attachment_upload.py \
    -pm /sessions/session-f63c206fb5f04c04aa17821001aa3847fajfm5x4/path_mapping.json \
    -s3 s3://test-job-attachment/DeadlineCloud \
    -mp '{"/sessions/session-e0317487a6cd470084b1c6fd85c789e6ank4lmh5/assetroot-a7714e87e776d9f1c179": "/sessions/session-e0317487a6cd470084b1c6fd85c789e6ank4lmh5/manifests/0bb7eb91fdf8780c4a7e6174de6dfc5e_manifest"}'
    -od '{"/sessions/session-e0317487a6cd470084b1c6fd85c789e6ank4lmh5/assetroot-a7714e87e776d9f1c179": ["out_dir"]}'
"""


def upload(s3_root_uri: str, path_mapping_rules: str, manifests: list[str]) -> None:
    # Helper function to get environment variable or raise error if missing
    def get_env_or_raise(name):
        value = os.environ.get(name)
        if value is None:
            raise ValueError(f"Required environment variable '{name}' is not set")
        return value

    s3_path = JobAttachmentS3Settings.partial_session_action_manifest_prefix(
        farm_id=get_env_or_raise("DEADLINE_FARM_ID"),
        queue_id=get_env_or_raise("DEADLINE_QUEUE_ID"),
        job_id=get_env_or_raise("DEADLINE_JOB_ID"),
        step_id=get_env_or_raise("DEADLINE_STEP_ID"),
        task_id=get_env_or_raise("DEADLINE_TASK_ID"),
        session_action_id=get_env_or_raise("DEADLINE_SESSIONACTION_ID"),
        time=time.time(),
    )

    api.attachment_upload(
        manifests=manifests,
        s3_root_uri=s3_root_uri,
        boto3_session=boto3.session.Session(),
        path_mapping_rules=path_mapping_rules,
        upload_manifest_path=s3_path,
    )


def merge(
    manifest_paths_by_root: dict[str, list[str]], path_mapping_rules_file: str
) -> dict[str, str]:
    manifest_path = os.path.join(os.getcwd(), "manifest")
    merged_manifests = dict()
    with open(path_mapping_rules_file, "r") as file:
        path_mapping_rules = json.load(file).get("path_mapping_rules", [])

    path_mapping_dest_to_src = dict()
    for rule in path_mapping_rules:
        path_mapping_dest_to_src[rule["destination_path"]] = rule["source_path"]

    for root, paths in manifest_paths_by_root.items():
        if len(paths) == 1:
            merged_manifests[root] = paths[0]
        else:
            output: Optional[ManifestMerge] = _manifest_merge(
                # map to the source path so the upload can correspond the correct path hash
                root=path_mapping_dest_to_src[root],
                # paths to manifest files to be merged
                manifest_files=paths,
                # directory to put the generated merged manifests
                destination=str(manifest_path),
                name="merge",
            )
            if output:
                # use to the local destination path to snapshot the correct diff
                merged_manifests[root] = output.local_manifest_path

    return merged_manifests


def snapshot(
    manifest_path_by_root: dict[str, str], out_rel_dirs_by_root: dict[str, list[str]]
) -> list[str]:
    output_path = os.path.join(os.getcwd(), "diff")
    manifests = list()

    for root, path in manifest_path_by_root.items():
        # TODO - use the public api for manifest snapshot once that's final and made public
        include_dirs = [subdir + "/**" for subdir in out_rel_dirs_by_root.get(root, [])]
        manifest: Optional[ManifestSnapshot] = _manifest_snapshot(
            root=root,
            # directory to put the generated diff manifests
            destination=str(output_path),
            # `output` is used for job download to discover output manifests
            # manifest file name need to contain the hash of root path for attachment CLI path mapping
            name=f"output-{os.path.basename(path)}",
            # this path to manifest servers as a base for the snapshot, generate only difference since this manifest
            diff=path,
            include=include_dirs,
        )
        if manifest:
            manifests.append(manifest.manifest)
            print(f"ja_snapshot: {json.dumps({'root': root, 'manifest': manifest.manifest})}")

    return manifests


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-pm", "--path-mapping", type=str, help="", required=True)
    parser.add_argument("-s3", "--s3-uri", type=str, help="", required=True)
    parser.add_argument("-mp", "--manifest-paths-by-root", type=json.loads, required=True)
    parser.add_argument("-od", "--out-rel-dirs-by-root", type=json.loads, required=False)
    return parser.parse_args(args)


def main(args=None):
    start_time = time.perf_counter()

    if args is None:
        args = sys.argv[1:]

    parsed_args = parse_args(args)

    merged_manifest_path_by_root = merge(
        manifest_paths_by_root=parsed_args.manifest_paths_by_root,
        path_mapping_rules_file=parsed_args.path_mapping,
    )

    manifests = snapshot(
        manifest_path_by_root=merged_manifest_path_by_root,
        out_rel_dirs_by_root=parsed_args.out_rel_dirs_by_root,
    )

    if manifests:
        print("\nStarting upload...")
        upload(
            manifests=manifests,
            s3_root_uri=parsed_args.s3_uri,
            path_mapping_rules=parsed_args.path_mapping,
        )

        total = time.perf_counter() - start_time
        print(f"Finished uploading after {total} seconds")
    else:
        print("\nNo output to upload")


if __name__ == "__main__":
    main()
