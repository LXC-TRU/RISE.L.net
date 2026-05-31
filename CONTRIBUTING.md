# Contributing to rise-l-net

Thank you for considering contributing to rise-l-net! This document will help you get started.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [How to Contribute](#how-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Features](#suggesting-features)
  - [Submitting Code](#submitting-code)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Style Guide & Testing](#style-guide--testing)
- [Pull Request Process](#pull-request-process)
- [Finding Tasks](#finding-tasks)

## Code of Conduct
This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you agree to its terms.

## How to Contribute

### Reporting Bugs
1. Search the [Issues](https://github.com/LXC-TRU/RISE.L.net/issues) first to avoid duplicates.
2. If none found, open a new issue and describe:
   - Environment (OS, Python version, MicroPython version, etc.)
   - Steps to reproduce
   - Expected vs actual behavior
   - Relevant logs or error messages (you can enable debug logging for the library)

### Suggesting Features
- Search existing issues first.
- Open a feature request issue explaining the problem you want to solve and any ideas for implementation.
- Wait for discussion before starting work.

### Submitting Code
- All code changes happen through Pull Requests (PRs).
- Link a PR to an existing issue whenever possible.
- Small fixes (typos, documentation) can be submitted directly without an issue.

## Development Setup
```bash
# Clone the repository
git clone https://github.com/LXC-TRU/RISE.L.net.git
cd RISE.L.net

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

An editable install means your changes take effect immediately.

## Project Structure
```
src/rise_l_net/          # Core library
    client/              # Device client (sync + async)
    server/              # Server (sync + async)
    common/              # Shared utilities, transport, middleware interfaces
tests/                   # Tests mirroring the source layout
docs/                    # Documentation sources (if any)
examples/                # Usage examples
```

## Style Guide & Testing
- We use `ruff` for linting and formatting.
- Run `ruff check src tests` to check for issues.
- Type checking with `mypy src` is optional but appreciated.
- Tests are written with `pytest`. Run them from the project root:
  ```bash
  pytest
  ```
- Please add tests for new features or fixes. No real hardware is required; you can mock the transport layer.

## Pull Request Process
1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/your-feature-name`.
3. Make your changes, keeping tests and linting passing.
4. Commit with clear messages (describe *what* and *why*).
5. Push to your fork: `git push origin feature/your-feature-name`.
6. Open a Pull Request against the `main` branch, linking the relevant issue.
7. The maintainer will review your code. You may need to make additional changes.
8. Once approved and merged, your contribution will be included in the next release and acknowledged in the release notes.

## Finding Tasks
- Look for issues labeled [good first issue](https://github.com/LXC-TRU/RISE.L.net/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) or `help wanted`.
- Browse the [Issues](https://github.com/LXC-TRU/RISE.L.net/issues) page for something that interests you.
- You’re also welcome to propose your own ideas.