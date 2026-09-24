#!/usr/bin/env python3
"""Strip detail lines from mermaid node labels.

Convention: inside a mermaid markdown-string label ("`...`"), a line that is
entirely italic (*like this*) is a detail line. This script removes those lines
so the same source renders as a headings-only chart. Everything outside mermaid
code fences is passed through unchanged.

    tools/strip-mermaid-detail.py docs/orbae/overall_data_flow.md > /tmp/plain.md
    tools/strip-mermaid-detail.py chart.mmd > plain.mmd
"""
import re
import sys

# *text*, not **bold**; optionally followed by the label's closing `"]
ITALIC_LINE = re.compile(r"^\s*\*[^*].*?\*\s*(?P<close>`\"\]\s*)?$")


def strip(text: str) -> str:
    out, in_mermaid = [], text.lstrip().startswith(("%%", "flowchart", "graph"))
    for line in text.splitlines():
        if line.startswith("```"):
            in_mermaid = line.startswith("```mermaid")
            out.append(line)
            continue
        m = ITALIC_LINE.match(line) if in_mermaid else None
        if m:
            if m.group("close"):            # detail was the last label line:
                out[-1] = out[-1].rstrip() + m.group("close").rstrip()
            continue
        out.append(line)
    return "\n".join(out) + ("\n" if text.endswith("\n") else "")


if __name__ == "__main__":
    src = open(sys.argv[1]).read() if len(sys.argv) > 1 else sys.stdin.read()
    sys.stdout.write(strip(src))
