# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
import shutil
from pathlib import Path

from deadline_worker_agent.config import config_file


def test(
    input_path: str,
    output_path: str,
    setting_name: str,
    value_to_set: str | bool,
    worker_config_path: Path,
) -> None:
    # GIVEN
    modifiable_setting = getattr(config_file.ModifiableSetting, setting_name.upper())
    settings_to_modify = [
        config_file.SettingModification(
            setting=modifiable_setting,
            value=value_to_set,
        )
    ]
    with open(output_path, "r") as f:
        expected_output = f.read()
    shutil.copyfile(input_path, worker_config_path)

    # WHEN
    config_file.ConfigFile.modify_config_file_settings(
        config_path=worker_config_path,
        settings_to_modify=settings_to_modify,
        backup=False,
    )

    # THEN
    written_config = worker_config_path.read_text()
    assert expected_output == written_config
