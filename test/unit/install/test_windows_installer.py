# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys
import pytest

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

import pytest
import os
import win32security
import ntsecuritycon as con


from deadline_worker_agent.installer.win_installer import (
    configure_farm_and_fleet,
    set_directory_permissions,
)


@pytest.fixture
def setup_example_config(tmp_path):
    # Create an example config file similar to 'worker.toml.windows.example' in the tmp_path
    example_config_path = os.path.join(tmp_path, "worker.toml")
    with open(example_config_path, "w") as f:
        f.write('# farm_id = "REPLACE-WITH-WORKER-FARM-ID"\n')
        f.write('# fleet_id = "REPLACE-WITH-WORKER-FLEET-ID"')
    return str(tmp_path)


def test_configure_farm_and_fleet_replaces_placeholders(setup_example_config):
    deadline_config_sub_directory = setup_example_config

    farm_id = "123"
    fleet_id = "456"
    configure_farm_and_fleet(deadline_config_sub_directory, farm_id, fleet_id)

    # Verify that the configuration file was created and placeholders were replaced
    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")
    assert os.path.isfile(worker_config_file), "Worker config file was not created"

    with open(worker_config_file, "r") as file:
        content = file.read()

    # Check if the farm_id and fleet_id have been correctly replaced
    assert f'farm_id = "{farm_id}"' in content, "farm_id placeholder was not replaced"
    assert f'fleet_id = "{fleet_id}"' in content, "fleet_id placeholder was not replaced"
    assert "#" not in content, "Comment placeholders were not removed"


def test_configure_farm_and_fleet_creates_backup(setup_example_config):
    deadline_config_sub_directory = setup_example_config

    # Call the function under test with some IDs
    configure_farm_and_fleet(deadline_config_sub_directory, "test_farm", "test_fleet")

    # Check that both the original and backup files exist
    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")
    backup_worker_config = worker_config_file + ".bak"

    assert os.path.isfile(worker_config_file), "Worker config file was not created"
    assert os.path.isfile(backup_worker_config), "Backup of worker config file was not created"


def check_directory_permissions(path, user_sid, expected_permission_flags):
    sd = win32security.GetFileSecurity(path, win32security.DACL_SECURITY_INFORMATION)
    dacl = sd.GetSecurityDescriptorDacl()
    for i in range(dacl.GetAceCount()):
        ace = dacl.GetAce(i)
        ace_sid = ace[2]
        ace_mask = ace[1]
        if ace_sid == user_sid:
            return ace_mask == expected_permission_flags
    return False


@pytest.fixture
def current_user_sid():
    username = os.getlogin()
    sid, _, _ = win32security.LookupAccountName("", username)
    return sid


@pytest.mark.integration
def test_set_directory_permissions(tmp_path, current_user_sid):
    directory_path = os.path.join(tmp_path, "testdir")
    os.mkdir(directory_path)

    permission_flags = con.FILE_GENERIC_READ
    username = os.getlogin()

    set_directory_permissions(directory_path, username, permission_flags)

    # Verify the permissions are set correctly
    assert check_directory_permissions(
        directory_path, current_user_sid, permission_flags
    ), "Permissions not set as expected"

    # check for administrator permissions as well
    admins_sid = win32security.CreateWellKnownSid(win32security.WinBuiltinAdministratorsSid, None)
    assert check_directory_permissions(
        directory_path, admins_sid, permission_flags
    ), "Admin permissions not set as expected"
