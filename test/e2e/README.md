# Cross OS E2E Tests

Tests designed to work with both Linux and Windows workers should use the `operating_system` parameter to differentiate differences between operating systems.

Export the `OPERATING_SYSTEM` environment variable to either "linux" or "windows", depending on the OS of the worker that you would like to test.

# OS Specific Tests
Use the `mark.skipif` parameter to differentiate between operating systems for os specific tests.
```
@pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific test",
    )
def test_linux_behaviour() -> None:
    ...

@pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "linux",
        reason="Windows specific test",
    )
def test_windows_behaviour() -> None:
    ...
```

# Test Scoping
**Session** - Tests that do not need to perform host or worker configuration, or will not impact the outcome of another test within session scope. It is beneficial to use session scope to keep test time low as it will not require another instance startup and configuration. Use the `session_worker` fixture (defined in `conftest.py`) to specify this scope.

**Class** - Tests defined as methods of a class that require modification(s) to the host or worker configuration. These modifications would impact tests using the session scoped worker fixture so instead we use a separate worker. Use the `class_worker` fixture (defined in `conftest.py`) to specify this scope.

**Function** - Tests that modify the host or worker configuration in a way that cannot be grouped with other tests. The worker and its associated EC2 instance are not shared with other tests. Use the `function_worker` fixture (defined in `conftest.py`) to specify this scope.