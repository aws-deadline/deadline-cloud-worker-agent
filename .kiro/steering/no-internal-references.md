# No Internal References

This is a public repository. Never include references to internal (non-public)
systems in code, comments, docstrings, commit messages, PR titles or
descriptions, or issue text. This includes:

- Internal ticket or issue tracker IDs and links
- Internal package, pipeline, or service codenames
- URLs that only resolve on an internal network (code hosting, wikis,
  dashboards)
- Cloud account IDs and internal team or personal aliases

Describe such context in generic terms instead (for example, "tracked
internally" rather than naming a ticket). Scan your changes for anything in
this list before pushing or posting.
