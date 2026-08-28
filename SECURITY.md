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

## Temporary MySQL RSA advisory exception

The MySQL SQLx adapter enables SQLx's `mysql-rsa` feature so a non-TLS
connection can complete `sha256_password` or full `caching_sha2_password`
authentication. SQLx encrypts the password with the server's public key; this
adapter does not accept private keys or perform RSA decryption or signing.

RustSec `RUSTSEC-2023-0071` concerns timing leakage in RSA private-key
operations, and no patched `rsa` release is currently available. The
repository therefore carries a targeted cargo-deny exception for this one
transitive SQLx path, rather than removing authentication support or ignoring
the entire advisory set. Prefer TLS for deployed MySQL/MariaDB connections.
Review this exception by 2026-12-31, and immediately when SQLx or `rsa` ships a
replacement/fix, the authentication policy changes, or private-key RSA use is
introduced; remove the exception at that review.
