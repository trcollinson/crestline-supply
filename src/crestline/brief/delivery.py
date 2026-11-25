"""The brief as it is delivered (M5 step 6): a page and an email.

The brief is Markdown with a fact ID after every number, which is what lets
the number check (check.py) refuse a number nobody measured. A plant manager
reading it on a phone at 05:30 doesn't need the IDs inline, so:

- the **page** (`<morning>.html`, next to the Markdown in S3) keeps them,
  small and grey, so anyone can trace a number to its fact;
- the **email** (SNS, plain text) leaves them out and says where the cited
  copy is.

Only the Markdown the template and the prompt produce is converted:
headings, bullets, paragraphs, bold, italics, and a rule. Anything else
(a table, which the prompt forbids) is shown as written.
"""

import html
import re

# A citation: [family:subject:metric], one ID or several, in brackets.
CITATION = re.compile(r"\s*\[(?:[a-z]+:[^\]\[:]+:[a-z_]+(?:,\s*|\]\s*\[)?)+\]")
HEADING = re.compile(r"^(#{1,6})\s+(.*)$")
BULLET = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+(.*)$")
BOLD = re.compile(r"\*\*(.+?)\*\*")
ITALIC = re.compile(r"(?<![*\w])\*(?!\s)(.+?)(?<!\s)\*(?![*\w])")

STYLE = """
body { font: 16px/1.5 system-ui, sans-serif; max-width: 42rem; margin: 2rem auto;
       padding: 0 1rem; color: #1d1d1f; background: #fff; }
h1 { font-size: 1.4rem; } h2 { font-size: 1.15rem; margin-top: 1.6rem; }
.cite { color: #8a8a8e; font-size: 0.75em; }
footer { color: #6e6e73; font-size: 0.85rem; margin-top: 2rem; }
@media (prefers-color-scheme: dark) {
  body { color: #f5f5f7; background: #1c1c1e; } .cite { color: #98989d; }
}
"""


def plain_text(markdown: str, cited_copy: str) -> str:
    """The email: the brief without its fact IDs, pointing at the cited copy."""
    lines = [
        BOLD.sub(r"\1", CITATION.sub("", line)).rstrip()
        for line in markdown.splitlines()
    ]
    return (
        "\n".join(lines).strip() + f"\n\nEvery number, with its source: {cited_copy}\n"
    )


def page(markdown: str, title: str) -> str:
    """The brief as a small HTML page, its citations kept."""
    body: list[str] = []
    in_list = False
    paragraph: list[str] = []

    def close() -> None:
        nonlocal in_list
        if paragraph:
            body.append(f"<p>{' '.join(paragraph)}</p>")
            paragraph.clear()
        if in_list:
            body.append("</ul>")
            in_list = False

    for raw in markdown.splitlines():
        line = raw.rstrip()
        if not line.strip():
            close()
        elif heading := HEADING.match(line):
            close()
            level = len(heading[1])
            body.append(f"<h{level}>{_inline(heading[2])}</h{level}>")
        elif line.strip() in ("---", "***"):
            close()
            body.append("<hr>")
        elif line.lstrip().startswith("|"):
            close()
            body.append(f"<pre>{html.escape(line)}</pre>")
        elif bullet := BULLET.match(line):
            if paragraph:
                body.append(f"<p>{' '.join(paragraph)}</p>")
                paragraph.clear()
            if not in_list:
                body.append("<ul>")
                in_list = True
            body.append(f"<li>{_inline(bullet[1])}</li>")
        elif in_list and raw.startswith((" ", "\t")):
            body[-1] = body[-1].removesuffix("</li>") + f" {_inline(line.strip())}</li>"
        else:
            if in_list:
                close()
            paragraph.append(_inline(line.strip()))
    close()
    return (
        '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html.escape(title)}</title>\n<style>{STYLE}</style>\n</head>\n"
        "<body>\n" + "\n".join(body) + "\n</body>\n</html>\n"
    )


def _inline(text: str) -> str:
    """Escape, then bold, italics, and citations as small grey text."""
    escaped = html.escape(text, quote=False)
    escaped = CITATION.sub(
        lambda m: f' <span class="cite">{m[0].strip()}</span>', escaped
    )
    escaped = BOLD.sub(r"<strong>\1</strong>", escaped)
    return ITALIC.sub(r"<em>\1</em>", escaped)
