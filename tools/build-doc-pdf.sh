#!/usr/bin/env bash
# Render a Markdown doc to PDF via pandoc -> HTML -> headless Chrome.
#
# Chrome is the engine because it handles what a LaTeX route does not: colour emoji,
# GitHub-flavoured tables, and Mermaid diagrams rendered by mermaid.js. It also matches
# how the other PDFs in docs/orbae/ were produced (Skia/PDF in their metadata).
#
# Usage: tools/build-doc-pdf.sh docs/orbae/foo.md [out.pdf]
set -euo pipefail

SRC="${1:?usage: build-doc-pdf.sh <markdown> [out.pdf]}"
OUT="${2:-${SRC%.md}.pdf}"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
HTML="$WORK/doc.html"

CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
[ -x "$CHROME" ] || CHROME="/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"
[ -x "$CHROME" ] || { echo "no Chrome/Brave found" >&2; exit 1; }

cat > "$WORK/style.css" <<'CSS'
@page { size: A4 portrait; margin: 16mm 15mm 18mm; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body { font: 9.6pt/1.5 Georgia,"Times New Roman",serif; color:#1b1b1b; margin:0; }
h1 { font:600 20pt/1.2 -apple-system,"Helvetica Neue",sans-serif; margin:0 0 3mm; letter-spacing:-.3px; }
h2 { font:600 14pt/1.25 -apple-system,"Helvetica Neue",sans-serif; margin:9mm 0 2.5mm;
     padding-bottom:1.4mm; border-bottom:1.4px solid #1b1b1b; break-after:avoid; }
h3 { font:600 11pt/1.3 -apple-system,"Helvetica Neue",sans-serif; margin:6mm 0 2mm;
     color:#111; break-after:avoid; }
h4 { font:600 9.6pt/1.3 -apple-system,"Helvetica Neue",sans-serif; margin:4.5mm 0 1.5mm;
     color:#3a3a3a; break-after:avoid; }
p, li { orphans:2; widows:2; }
code { font:8.6pt ui-monospace,Menlo,monospace; background:#f4f4f4; padding:.5px 2.5px;
       border-radius:2px; overflow-wrap:break-word; }
pre { background:#f7f7f7; border:.6px solid #e2e2e2; border-radius:3px; padding:2.5mm 3mm;
      overflow:hidden; break-inside:avoid; }
pre code { background:none; padding:0; font-size:8pt; line-height:1.4; white-space:pre-wrap; }
blockquote { margin:2.5mm 0; padding:1.5mm 0 1.5mm 4mm; border-left:2.4px solid #c8c8c8;
             color:#454545; break-inside:avoid; }
blockquote p { margin:.8mm 0; }
table { border-collapse:collapse; width:100%; font:8.3pt/1.38 -apple-system,"Helvetica Neue",sans-serif;
        margin:2.5mm 0 4mm; table-layout:auto; }
th { background:#f0f0f0; text-align:left; font-weight:600; }
th, td { border:.6px solid #d6d6d6; padding:1.4mm 1.8mm; vertical-align:top; }
tr { break-inside:avoid; }
thead { display:table-header-group; }
td code, th code { font-size:7.6pt; background:#efefef; }
a { color:#14507d; text-decoration:none; }
hr { border:0; border-top:.6px solid #d6d6d6; margin:6mm 0; }
.mermaid { break-inside:avoid; margin:3mm 0 5mm; text-align:center; }
.mermaid svg { max-width:100%; height:auto; }
CSS

pandoc --from=gfm --to=html5 --standalone \
       --metadata title="" \
       --css=style.css \
       --output="$HTML" "$SRC"
cp "$WORK/style.css" "$WORK/style.css.bak"

# pandoc emits ```mermaid as <pre class="mermaid"><code>...; mermaid.js wants <pre class="mermaid">text
python3 - "$HTML" <<'PY'
import re, sys
p = sys.argv[1]; s = open(p).read()
s = re.sub(r'<pre class="mermaid"><code>(.*?)</code></pre>',
           lambda m: '<pre class="mermaid">' + m.group(1)
                      .replace("&quot;", '"').replace("&lt;", "<")
                      .replace("&gt;", ">").replace("&amp;", "&") + "</pre>",
           s, flags=re.S)
if 'class="mermaid"' in s:
    s = s.replace("</body>",
      '<script type="module">\n'
      'import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";\n'
      'import elkLayouts from "https://cdn.jsdelivr.net/npm/@mermaid-js/layout-elk@0/dist/mermaid-layout-elk.esm.min.mjs";\n'
      'mermaid.registerLayoutLoaders(elkLayouts);\n'
      'mermaid.initialize({startOnLoad:true,theme:"neutral"});\n'
      "</script>\n</body>")
open(p, "w").write(s)
PY

"$CHROME" --headless=new --disable-gpu --no-sandbox --no-pdf-header-footer \
          --virtual-time-budget=20000 --run-all-compositor-stages-before-draw \
          --print-to-pdf="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")" \
          "file://$HTML" 2>/dev/null

echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
