# Telemetry

The AWS Deadline Cloud worker agent collects telemetry data by default. Telemetry events contain non-personally-identifiable information that helps us understand how users interact with our
software so we know what features our customers use, and/or what existing pain points are.

You can opt out of telemetry data collection by either:

1. Setting the environment variable: `DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true`
2. Providing the installer flag: `--telemetry-opt-out`
3. Setting `opt_out = true` in the `[telemetry]` section of `worker.toml`:

```toml
[telemetry]
opt_out = true
```

The priority order is: environment variable > worker agent config file (`worker.toml`) > default (enabled).

Note: The worker agent will also check the legacy Deadline client config file (`~/.deadline/config`)
as a fallback if the setting is not present in the environment or `worker.toml`.
