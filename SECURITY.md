# Security policy

Typra is an embedded database intended to operate on **local files** that may be corrupted or maliciously crafted. We treat **panics**, **infinite loops**, and **excessive resource consumption** from untrusted input as security-relevant bugs.

## Supported versions

Typra is currently **1.x**. Security fixes are released on the active stable line.

## Reporting a vulnerability

- Please **do not** open a public GitHub issue for suspected vulnerabilities.
- Instead, report privately by emailing: **security@typra.dev** (preferred), or by contacting the maintainers via a private channel if you already have one.

Include:
- Typra version(s) affected
- A minimal reproduction (ideally a `.typra` file and a short program/script)
- Expected vs actual behavior
- Any crash logs / backtraces

## Scope

In addition to classic memory-safety issues, we treat the following as security-relevant:

- Crashes/panics triggered by opening untrusted `.typra` files
- Infinite loops or unbounded resource consumption (CPU, memory, disk) from untrusted input

## Coordinated disclosure

- We will acknowledge receipt within **7 days**.
- We will work with you on a fix and a release timeline.
- We aim to ship fixes quickly and publish release notes describing impact and mitigations.

