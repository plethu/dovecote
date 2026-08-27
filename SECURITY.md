# Security reporting

Please do not put an unpatched vulnerability, credentials, personal data, or
exploit details in a public issue. Dovecote accepts private vulnerability
reports through GitHub's Security Advisory flow.

## Private reports

The repository has private vulnerability reporting enabled. Before each
release, a repository maintainer must:

1. Verify that the private advisory form is available at the route below to a
   reporter, and that reports are not visible as public issues.
2. Record that verification in the release review and keep the route monitored.

If the route is disabled or cannot be verified, stop the release. Do not
replace it with a public issue, and do not claim that security reporting is
covered.

Report suspected vulnerabilities privately through the repository's GitHub
Security Advisory flow:

<https://github.com/plethu/dovecote/security/advisories/new>

Include the affected crate/version or commit, backend and deployment context,
the smallest reproducible description, and a safe contact route for follow-up.
Do not include secrets that are not needed to reproduce the report. Maintainers
will use the private report to coordinate scope, fix, and disclosure. There is
no fallback email or public disclosure route in this project.

Dovecote's storage contract does not provide tenant authorization, encryption,
secret management, transport authentication, or retention policy. Reports
about those boundaries should identify the application integration as well as
the Dovecote crate so the right owner can respond.
