# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 0.x (latest minor) | yes |
| older 0.x minors | no |

Before v1.0 only the latest minor release receives security fixes.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub private vulnerability reporting:
<https://github.com/thnhdatle/wake/security/advisories/new>

Include the affected version, a description of the impact and, if possible, steps to reproduce.
We aim to acknowledge a report within 72 hours and to share a remediation plan after triage.
Coordinated disclosure is appreciated: we will credit reporters unless they prefer to stay anonymous.

## Supply chain

Every commit and pull request is scanned with gitleaks, `govulncheck` and `go-licenses`; release
images are scanned with trivy and run as a non-root user on a distroless base.
