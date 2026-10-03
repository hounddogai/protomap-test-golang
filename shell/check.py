"""Check formatting and parse the Python and shell development tools without writing bytecode."""

import ast
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
go_files = sorted(str(path) for folder in ("cmd", "internal", "services", "gen")
                  for path in (ROOT / folder).rglob("*.go"))
unformatted = subprocess.check_output(["gofmt", "-l", *go_files], text=True).strip()
if unformatted:
    raise SystemExit(f"Unformatted Go files:\n{unformatted}")
for path in (ROOT / "shell").glob("*.py"):
    ast.parse(path.read_text(), filename=str(path))
for path in (ROOT / "shell/scripts").iterdir():
    subprocess.run(["sh", "-n", str(path)], check=True)
print("Go formatting, Python syntax, and shell syntax verified")
