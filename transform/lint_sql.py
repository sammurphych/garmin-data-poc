"""Lint the SQLMesh models with SQLFluff.

    uv run python transform/lint_sql.py                 # report problems in every model
    uv run python transform/lint_sql.py path/to/x.sql   # some models
    uv run python transform/lint_sql.py --fix           # fix what SQLFluff can, then report the rest

SQLFluff can't parse SQLMesh's MODEL (...) header or its @macros, so each file is linted with the
header turned into comment lines and macros such as @start_ds replaced by a date, which keeps line
numbers matching the file. --fix puts the header and macros back afterwards, and leaves a file
alone if it can't do that exactly. Don't run `sqlfluff fix` directly: it can't parse the files.
The rules are in .sqlfluff at the repo root.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import sqlfluff

TRANSFORM = Path(__file__).resolve().parent
CONFIG = TRANSFORM.parent / ".sqlfluff"
MACRO = re.compile(r"@\w+")
MACRO_STAND_IN = "'2000-01-01'"  # every macro in the model bodies is a date (@start_ds, @end_ds)


def body_only(sql: str) -> str:
    """The file with its MODEL (...) header commented out, and macros replaced by a date."""
    lines = sql.split("\n")
    header_end = -1
    if lines and lines[0].startswith("MODEL ("):
        header_end = next((i for i, line in enumerate(lines) if line.rstrip() == ");"), None)
        if header_end is None:
            raise ValueError("MODEL ( header has no closing ');' line")
    header = ["-- " + line for line in lines[: header_end + 1]]
    body = [MACRO.sub(MACRO_STAND_IN, line) for line in lines[header_end + 1:]]
    return "\n".join(header + body)


def restore(original: str, fixed: str) -> str | None:
    """Put the MODEL header and macros back into SQLFluff's fixed text; None if they can't be matched."""
    lines = original.split("\n")
    header_end = next((i for i, line in enumerate(lines) if line.rstrip() == ");"), -1) \
        if lines and lines[0].startswith("MODEL (") else -1
    header = lines[: header_end + 1]
    fixed_lines = fixed.split("\n")
    if fixed_lines[: len(header)] != ["-- " + line for line in header]:
        return None  # SQLFluff changed the header lines
    body = "\n".join(fixed_lines[len(header):])
    macros = MACRO.findall("\n".join(lines[header_end + 1:]))
    if body.count(MACRO_STAND_IN) != len(macros) or "\n".join(lines[header_end + 1:]).count(MACRO_STAND_IN):
        return None  # a stand-in was added, removed or already in the file
    for macro in macros:
        body = body.replace(MACRO_STAND_IN, macro, 1)
    return "\n".join(header + [body]) if header else body


def main(args: list[str]) -> int:
    fix = "--fix" in args
    paths = [a for a in args if a != "--fix"]
    files = [Path(p).resolve() for p in paths] or sorted((TRANSFORM / "models").rglob("*.sql"))
    problems = 0
    for path in files:
        try:
            original = path.read_text()
            sql = body_only(original)
        except ValueError as e:
            print(f"{path.relative_to(TRANSFORM.parent)}: {e}")
            problems += 1
            continue
        if fix:
            restored = restore(original, sqlfluff.fix(sql, config_path=str(CONFIG)))
            if restored is None:
                print(f"{path.relative_to(TRANSFORM.parent)}: not fixed (couldn't restore the header or macros)")
            elif restored != original:
                path.write_text(restored)
                print(f"{path.relative_to(TRANSFORM.parent)}: fixed")
                sql = body_only(restored)
        for v in sqlfluff.lint(sql, config_path=str(CONFIG)):
            print(f"{path.relative_to(TRANSFORM.parent)}:{v['start_line_no']}:{v['start_line_pos']}: "
                  f"{v['code']} {v['description']}")
            problems += 1
    print(f"{problems} problem(s) in {len(files)} file(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
