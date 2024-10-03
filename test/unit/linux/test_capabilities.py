# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Generator, Any

import pytest
import sys
from unittest.mock import MagicMock, patch, call, ANY

from deadline_worker_agent.linux import capabilities as test_mod


pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux-specific tests")


@pytest.fixture
def libcap() -> MagicMock:
    return MagicMock()


@pytest.fixture
def caps() -> MagicMock:
    return MagicMock()


@pytest.fixture(autouse=True)
def mock_get_libcap(
    libcap: MagicMock,
) -> Generator[MagicMock, None, None]:
    with patch.object(test_mod, "_get_libcap", return_value=libcap) as mock_get_libcap:
        yield mock_get_libcap


@pytest.fixture
def mock_module_logger() -> Generator[MagicMock, None, None]:
    with patch.object(test_mod, "logger") as mock_module_logger:
        yield mock_module_logger


class TestGetCapsStr:
    """Tests for _get_caps_str"""

    def test_success_case(
        self,
        libcap: MagicMock,
        caps: MagicMock,
    ) -> None:
        # GIVEN
        mock_cap_to_text: MagicMock = libcap.cap_to_text
        mock_cap_to_text_return: MagicMock = mock_cap_to_text.return_value
        mock_cap_to_text_return_decode: MagicMock = mock_cap_to_text_return.decode

        # WHEN
        result = test_mod._get_caps_str(libcap=libcap, caps=caps)

        # THEN
        mock_cap_to_text.assert_called_once_with(caps, None)
        mock_cap_to_text_return_decode.assert_called_once_with()
        assert result == mock_cap_to_text_return_decode.return_value

    def test_exception(
        self,
        libcap: MagicMock,
        caps: MagicMock,
    ) -> None:
        """When libcap.cap_to_text raises an OSError it should not be handled"""
        # GIVEN
        mock_cap_to_text: MagicMock = libcap.cap_to_text
        error_raised = OSError(5, "some error")
        mock_cap_to_text.side_effect = [error_raised]

        # WHEN
        def when() -> None:
            test_mod._get_caps_str(libcap=libcap, caps=caps)

        # THEN
        with pytest.raises(OSError) as raise_ctx:
            when()
        assert raise_ctx.value is error_raised


class TestHasCapKillInheritable:
    """Test cases for _has_cap_kill_inheritable"""

    @pytest.mark.parametrize(
        argnames="cap_get_flag_return_value",
        argvalues=(
            True,
            False,
        ),
    )
    def test_behaviour(
        self,
        libcap: MagicMock,
        cap_get_flag_return_value: bool,
        caps: MagicMock,
    ) -> None:
        """Tests that _has_cap_kill_inheritable returns the correct value"""
        # GIVEN
        mock_cap_get_flag: MagicMock = libcap.cap_get_flag
        with (
            patch.object(test_mod.ctypes, "byref") as mock_ctypes_byref,
            patch.object(test_mod, "cap_flag_value_t") as mock_cap_flag_value_t,
        ):

            def cap_get_flag_side_effect(
                caps: test_mod.cap_t,
                cap: int,
                cap_set: int,
                flag_value: Any,
            ) -> None:
                mock_cap_flag_value_t.return_value.value = cap_get_flag_return_value

            mock_cap_get_flag.side_effect = cap_get_flag_side_effect

            # WHEN
            result = test_mod._has_cap_kill_inheritable(
                libcap=libcap,
                caps=caps,
            )

        # THEN
        mock_cap_flag_value_t.assert_called_once_with()
        mock_ctypes_byref.assert_called_once_with(mock_cap_flag_value_t.return_value)
        mock_cap_get_flag.assert_called_once_with(
            caps,
            # Value for CAP_KILL
            # See https://github.com/torvalds/linux/blob/28eb75e178d389d325f1666e422bc13bbbb9804c/include/uapi/linux/capability.h#L147
            5,
            # Value for CAP_INHERITABLE
            # See https://ddnet.org/codebrowser/include/sys/capability.h.html#CAP_INHERITABLE
            2,
            mock_ctypes_byref.return_value,
        )
        assert result == cap_get_flag_return_value

    def test_exception(
        self,
        libcap: MagicMock,
        caps: MagicMock,
    ) -> None:
        """Tests that when cap_get_flag returns an exception the exception is unhandled and
        propagated to the caller"""

        # GIVEN
        mock_cap_get_flag: MagicMock = libcap.cap_get_flag
        exception_to_raise = OSError(3, "error msg")
        mock_cap_get_flag.side_effect = [exception_to_raise]

        # WHEN
        def when() -> None:
            test_mod._has_cap_kill_inheritable(
                libcap=libcap,
                caps=caps,
            )

        # THEN
        with pytest.raises(OSError) as raise_ctx:
            when()
        assert raise_ctx.value is exception_to_raise


class TestDropKillCapFromInheritable:
    """Tests for drop_kill_cap_from_inheritable()"""

    def test_no_libcap_warns_and_continues(
        self,
        mock_get_libcap: MagicMock,
        mock_module_logger: MagicMock,
    ) -> None:
        """Tests that when libcap is not found, the drop_kill_cap_from_inheritable function logs a
        warning and continues"""

        # GIVEN
        mock_get_libcap.return_value = None
        module_logger_warning_mock: MagicMock = mock_module_logger.warning

        # WHEN
        test_mod.drop_kill_cap_from_inheritable()

        # THEN
        module_logger_warning_mock.assert_called_once_with(
            "Unable to locate libcap. The worker agent will run without Linux capability awareness."
        )

    def test_has_cap_kill_inheritable(
        self,
        libcap: MagicMock,
        caps: MagicMock,
        mock_module_logger: MagicMock,
    ) -> None:
        """Tests that when CAP_KILL is in the thead's inheritable set, the
        drop_kill_cap_from_inheritable() removes it"""

        # GIVEN
        mock_cap_get_proc: MagicMock = libcap.cap_get_proc
        mock_cap_set_flag: MagicMock = libcap.cap_set_flag
        mock_cap_set_proc: MagicMock = libcap.cap_set_proc
        mock_cap_get_proc.return_value = caps
        cap_str_before = "before"
        cap_str_after = "after"
        module_logger_info_mock: MagicMock = mock_module_logger.info
        with (
            patch.object(
                test_mod, "_has_cap_kill_inheritable", return_value=True
            ) as mock_has_cap_kill_inheritable,
            patch.object(
                test_mod, "_get_caps_str", side_effect=[cap_str_before, cap_str_after]
            ) as mock_get_caps_str,
        ):
            # WHEN
            test_mod.drop_kill_cap_from_inheritable()

        # THEN
        mock_cap_get_proc.assert_called_once_with()
        mock_get_caps_str.assert_has_calls(
            [
                # cap str before
                call(libcap=libcap, caps=caps),
                # cap str after
                call(libcap=libcap, caps=caps),
            ]
        )
        mock_has_cap_kill_inheritable.assert_called_once_with(libcap=libcap, caps=caps)
        module_logger_info_mock.assert_has_calls(
            [
                call(
                    "CAP_KILL was found in the thread's inheritable capability set (%s). Dropping CAP_KILL from the thread's inheritable capability set",
                    cap_str_before,
                ),
                call("Capabilites are: %s", cap_str_after),
            ]
        )
        mock_cap_set_flag.assert_called_once_with(
            caps,
            # CAP_INHERITABLE, see https://ddnet.org/codebrowser/include/sys/capability.h.html#cap_flag_t
            2,
            # Number of caps cleared
            1,
            ANY,
            # CAP_CLEAR, see # See https://ddnet.org/codebrowser/include/sys/capability.h.html#cap_flag_value_t
            0,
        )
        mock_cap_set_proc.assert_called_once_with(caps)
        # Third arg is cap_value_arr_t (a C struct) containing the list of capabilities to  clear from the capability set
        assert len(mock_cap_set_flag.call_args.args[3]) == 1
        # CAP_KILL is 5, see https://github.com/torvalds/linux/blob/28eb75e178d389d325f1666e422bc13bbbb9804c/include/uapi/linux/capability.h#L147
        assert mock_cap_set_flag.call_args.args[3][0] == 5

    def test_does_not_have_cap_kill_inheritable(
        self,
        libcap: MagicMock,
        caps: MagicMock,
        mock_module_logger: MagicMock,
    ) -> None:
        """Test that when drop_kill_cap_from_inheritable() does not detect CAP_KILL in the
        inheritable capability set, it does not attempt to remove it and logs the capability
        str"""
        # GIVEN
        mock_cap_get_proc: MagicMock = libcap.cap_get_proc
        mock_cap_get_proc.return_value = caps
        mock_cap_set_flag: MagicMock = libcap.cap_set_flag
        mock_cap_set_proc: MagicMock = libcap.cap_set_proc
        cap_str = "before"
        module_logger_info_mock: MagicMock = mock_module_logger.info

        with (
            patch.object(
                test_mod, "_has_cap_kill_inheritable", return_value=False
            ) as mock_has_cap_kill_inheritable,
            patch.object(
                test_mod,
                "_get_caps_str",
                return_value=cap_str,
            ) as mock_get_caps_str,
        ):
            # WHEN
            test_mod.drop_kill_cap_from_inheritable()

        # THEN
        mock_cap_get_proc.assert_called_once_with()
        mock_get_caps_str.assert_called_once_with(libcap=libcap, caps=caps)
        mock_has_cap_kill_inheritable.assert_called_once_with(libcap=libcap, caps=caps)
        module_logger_info_mock.assert_called_once_with(
            "CAP_KILL was not found in the thread's inheritable capability set (%s)", cap_str
        )
        mock_cap_set_flag.assert_not_called()
        mock_cap_set_proc.assert_not_called()
