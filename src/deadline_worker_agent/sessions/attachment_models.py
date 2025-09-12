# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Worker-specific data structures for job attachment operations.
This module contains data structures and utilities specifically designed for
worker agent operations, providing an alternative to hash-based correlation
while maintaining backward compatibility for CLI operations.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Any

from deadline.job_attachments.models import ManifestProperties, PathMappingRule
from deadline.job_attachments.asset_manifests import hash_data
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import AssetManifest


class WorkerManifestProperties:
    """
    Worker-specific manifest properties that extend ManifestProperties with local paths.
    This class contains the original manifest properties along with worker-specific
    information about local file system paths for manifest files and root directories.
    """

    def __init__(
        self,
        manifest_properties: ManifestProperties,
        local_root_path: str,
        local_manifest_paths: Optional[List[str]] = None,
    ):
        """
        Initialize WorkerManifestProperties.
        Args:
            manifest_properties: The original manifest properties
            local_root_path: Local root path for attachment files
            local_manifest_paths: Optional list of local paths for manifest files (supports step dependencies)
        """
        self.manifest_properties = manifest_properties
        self.local_root_path = local_root_path
        self.local_manifest_paths = list(local_manifest_paths) if local_manifest_paths else []

    @property
    def root_path(self) -> str:
        """Get the original root path from manifest properties."""
        return self.manifest_properties.rootPath

    @property
    def root_path_format(self):
        """Get the root path format from manifest properties."""
        return self.manifest_properties.rootPathFormat

    @property
    def file_system_location_name(self) -> Optional[str]:
        """Get the file system location name from manifest properties."""
        return self.manifest_properties.fileSystemLocationName

    @property
    def input_manifest_path(self) -> Optional[str]:
        """Get the input manifest path from manifest properties."""
        return self.manifest_properties.inputManifestPath

    @property
    def input_manifest_hash(self) -> Optional[str]:
        """Get the input manifest hash from manifest properties."""
        return self.manifest_properties.inputManifestHash

    @property
    def output_relative_directories(self) -> Optional[List[str]]:
        """Get the output relative directories from manifest properties."""
        return self.manifest_properties.outputRelativeDirectories

    def to_path_mapping_rule(self) -> PathMappingRule:
        """
        Convert to an OpenJD-compatible path mapping rule.
        Returns:
            PathMappingRule: A path mapping rule using local root path as destination
        """
        return PathMappingRule(
            source_path_format=self.manifest_properties.rootPathFormat.value,
            source_path=self.manifest_properties.rootPath,
            destination_path=self.local_root_path,
        )

    def get_hashed_source_path(self) -> str:
        """
        Generate a hashed string from file system location name and root path.
        Returns:
            str: A hash string generated from the combination of file_system_location_name
                 and root_path using the manifest's default hash algorithm
        Note:
            This follows the same pattern used in asset_sync.py for manifest name generation.
        """
        # TODO - add doc and/or define this formally as a data contract in job attachment
        hash_alg = AssetManifest.get_default_hash_alg()
        return hash_data(
            f"{self.file_system_location_name or ''}{self.root_path}".encode(), hash_alg
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert WorkerManifestProperties to a dictionary for JSON serialization.
        Returns:
            Dict[str, Any]: A dictionary representation of the worker manifest properties
        """

        return {
            "manifestProperties": self.manifest_properties.to_dict(),
            "localManifestPaths": self.local_manifest_paths,
            "localRootPath": self.local_root_path,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkerManifestProperties":
        """
        Create WorkerManifestProperties from a dictionary (JSON deserialization).
        Args:
            data: Dictionary containing the worker manifest properties data
        Returns:
            WorkerManifestProperties: A new worker manifest properties instance
        Raises:
            KeyError: If required keys are missing from the data
            ValueError: If the data contains invalid values
        """
        # Manually deserialize ManifestProperties to avoid dependency on unreleased from_dict method
        manifest_props_data = data["manifestProperties"]
        from deadline.job_attachments.models import PathFormat

        manifest_properties = ManifestProperties(
            rootPath=manifest_props_data["rootPath"],
            rootPathFormat=PathFormat(manifest_props_data["rootPathFormat"]),
            fileSystemLocationName=manifest_props_data.get("fileSystemLocationName"),
            inputManifestPath=manifest_props_data.get("inputManifestPath"),
            inputManifestHash=manifest_props_data.get("inputManifestHash"),
            outputRelativeDirectories=manifest_props_data.get("outputRelativeDirectories"),
        )

        return cls(
            manifest_properties=manifest_properties,
            local_manifest_paths=data.get("localManifestPaths", []),
            local_root_path=data["localRootPath"],
        )

    def __eq__(self, other: object) -> bool:
        """
        Check equality with another WorkerManifestProperties instance.
        Args:
            other: Object to compare with
        Returns:
            bool: True if objects are equal, False otherwise
        """
        if not isinstance(other, WorkerManifestProperties):
            return False

        return (
            self.manifest_properties == other.manifest_properties
            and self.local_root_path == other.local_root_path
            and self.local_manifest_paths == other.local_manifest_paths
        )

    def __hash__(self) -> int:
        """
        Generate hash for WorkerManifestProperties instance.
        Returns:
            int: Hash value based on immutable attributes
        """
        return hash(
            (
                self.manifest_properties,
                self.local_root_path,
                tuple(self.local_manifest_paths),  # Convert list to tuple for hashing
            )
        )
