"""Copy the parity report produced by the job into the README.

The job writes `parity_report.md` to a Volume; the workflow downloads it to
`docs/parity-report.md` and this script replaces the block between the
`<!-- parity:start -->` and `<!-- parity:end -->` markers in the README.

Usage:
    python scripts/publish_parity.py --report docs/parity-report.md --readme README.md
"""

from __future__ import annotations

import argparse
from pathlib import Path

START = "<!-- parity:start -->"
END = "<!-- parity:end -->"


def replace_block(readme: str, report: str) -> str:
    if START not in readme or END not in readme:
        raise ValueError(f"README must contain {START} and {END}")
    head, rest = readme.split(START, 1)
    _, tail = rest.split(END, 1)
    return f"{head}{START}\n{report.strip()}\n{END}{tail}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--report", type=Path, default=Path("docs/parity-report.md"))
    parser.add_argument("--readme", type=Path, default=Path("README.md"))
    args = parser.parse_args()
    updated = replace_block(
        args.readme.read_text(encoding="utf-8"), args.report.read_text(encoding="utf-8")
    )
    args.readme.write_text(updated, encoding="utf-8", newline="\n")
    print(f"Updated {args.readme} from {args.report}")


if __name__ == "__main__":
    main()
