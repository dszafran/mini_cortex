"""Build the web UI and ship it inside the package, so participants never need Node.

    uv run python scripts/build_ui.py            # npm ci (if needed) + npm run build in ../web, copy dist -> static
    uv run python scripts/build_ui.py --no-build # only copy an existing web/dist

Commit app/mini_cortex/static afterwards. While developing, a fresher web/dist is served anyway (server.static_dir).
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from mini_cortex.server import STATIC, WEB_DIST

WEB = WEB_DIST.parent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--no-build", action="store_true", help="copy web/dist as it is")
    args = ap.parse_args()
    if not args.no_build:
        npm = shutil.which("npm")
        if not npm:
            sys.exit("npm not found: install Node 20+ to rebuild the UI (participants do not need this)")
        if not (WEB / "node_modules").is_dir():
            subprocess.run([npm, "ci"], cwd=WEB, check=True)
        subprocess.run([npm, "run", "build"], cwd=WEB, check=True)
    if not (WEB_DIST / "index.html").exists():
        sys.exit(f"no build in {WEB_DIST}")
    shutil.rmtree(STATIC, ignore_errors=True)
    shutil.copytree(WEB_DIST, STATIC)
    size = sum(f.stat().st_size for f in STATIC.rglob("*") if f.is_file())
    print(f"copied {WEB_DIST} -> {STATIC} ({size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
