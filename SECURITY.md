# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in NexusDB, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities.
2. Email the maintainers at **saikiranreddy2710@github.com** with a detailed description of the vulnerability.
3. Include steps to reproduce, affected components, and potential impact.

### What to expect

- **Acknowledgement**: We will acknowledge receipt of your report within **48 hours**.
- **Assessment**: We will assess the vulnerability and provide an initial severity rating within **5 business days**.
- **Resolution**: Critical and high-severity issues will be prioritized for a fix. We aim to release patches within **14 days** of confirmation.
- **Disclosure**: We will coordinate with you on public disclosure timing after a fix is available.

## Security Architecture

NexusDB implements a Zero Trust security model:

- **Authentication**: PBKDF2-HMAC-SHA256 with 600,000 iterations, per-user salts
- **Authorization**: Role-Based Access Control (RBAC) with `admin`, `readwrite`, and `readonly` roles
- **Audit**: Comprehensive audit logging of all authentication and authorization events
- **Transport**: gRPC with TLS support

For details, see the `nexus-security` crate documentation.
