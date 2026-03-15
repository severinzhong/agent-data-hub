from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


HUB_ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = HUB_ROOT.parent
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from core.discovery import discover_source_modules


CAPABILITY_ORDER = (
    "channel.search",
    "content.search",
    "content.update",
    "content.query",
    "content.interact",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sources.json for agent-data-hub")
    parser.add_argument("--workspace", default=str(HUB_ROOT), help="Source workspace root")
    parser.add_argument("--output", default=str(HUB_ROOT / "sources.json"), help="Output JSON path")
    parser.add_argument("--repo-url", required=True, help="Repository URL used in generated entries")
    parser.add_argument("--docs-url", default="", help="Docs URL used in generated entries; defaults to repo URL")
    parser.add_argument("--version", default="official", help="Version label written into generated entries")
    parser.add_argument(
        "--exclude",
        action="append",
        default=["data_hub"],
        help="Source directory name to exclude; may be repeated",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    output = Path(args.output).resolve()
    docs_url = args.docs_url or args.repo_url
    excluded = set(args.exclude)

    rows: list[dict[str, object]] = []
    for discovered in discover_source_modules(workspace):
        if discovered.name in excluded:
            continue
        manifest = discovered.manifest
        source_dir = workspace / discovered.name
        capabilities = [
            capability
            for capability in CAPABILITY_ORDER
            if capability == "content.query"
            and manifest.query is not None
            or capability == "content.interact"
            and ("content.interact" in manifest.source_actions or bool(manifest.interaction_verbs))
            or capability != "content.query"
            and capability != "content.interact"
            and capability in manifest.source_actions
        ]
        rows.append(
            {
                "source_name": manifest.identity.name,
                "display_name": manifest.identity.display_name,
                "summary": manifest.identity.summary,
                "repo_url": args.repo_url,
                "repo_subdir": discovered.name,
                "docs_url": docs_url,
                "version": args.version,
                "install_strategy": "git_clone_subdir",
                "init_script": "init.sh" if (source_dir / "init.sh").is_file() else "",
                "capabilities": capabilities,
            }
        )

    rows.sort(key=lambda row: str(row["source_name"]))
    output.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
