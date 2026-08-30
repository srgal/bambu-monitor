"""Webtop system messages ("הודעות מערכת") for the e-ink dashboard.

Drop-in module for eink_dashboard.py. Two public entry points:

    fetch_messages(login=_webtop_login)  -> list[Message]   (network)
    render_messages_region(...)          -> int             (height consumed)

Both are defensive: fetch_messages() never raises, and the render region
returns 0 height when there are no messages, so the section disappears
entirely -- the same dynamic-layout contract the AC row uses.

Integration into eink_dashboard.py
----------------------------------
    from eink_messages import fetch_messages, render_messages_region

    messages = fetch_messages(login=_webtop_login)   # [] if Webtop is down

    # ...after the school section, wherever the AC row is handled:
    h = render_messages_region(
        draw, X, cursor_y, CONTENT_W, messages,
        title_font=FONT_BOLD_14, body_font=FONT_12, header_font=FONT_BOLD_16,
    )
    if h:
        cursor_y += h + SECTION_GAP

Substitute your own font objects and layout variables. When `messages` is
empty `h` is 0, nothing is drawn, and cursor_y is untouched.

First run against the live portal
---------------------------------
Webtop's markup was not available when this was written, so the selectors are
ordered guesses with a structural fallback. Capture the real page once:

    fetch_messages(login=_webtop_login, dump_to="/tmp/webtop.html")

then check parse_messages(open("/tmp/webtop.html").read()). If it returns [],
the row/title/body class names in _ROW_SELECTORS need one edit to match.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

MAX_MESSAGES = 3
BODY_CHARS = 90

# layout knobs for the messages region
HEADER_H = 20
PAD = 4
ROW_GAP = 5

# The endpoint is relative; only the path was recovered from the earlier
# investigation. Override WEBTOP_BASE if the portal host differs.
WEBTOP_BASE = "https://webtopserver.smartschool.co.il"
MESSAGES_PATH = (
    "/shotefView.aspx?view=changesAndMessages&institutionCode=415315"
    "&item=1$1@226691079@72f83ea657799249d60f18c4a8d3e69186bd6a9cd279cb30d3979d7e80bc6323"
)
TIMEOUT = 12


@dataclass
class Message:
    title: str
    body: str


# --------------------------------------------------------------------------
# fetching
# --------------------------------------------------------------------------

def _coerce_session(obj):
    """Accept whatever _webtop_login() hands back and return a live session.

    Handles the shapes a login helper realistically returns: a requests
    .Session, a (session, ...) tuple, or a cookie jar / cookie dict that we
    wrap in a fresh Session.
    """
    import requests

    if obj is None:
        return None
    if isinstance(obj, requests.Session):
        return obj
    if isinstance(obj, (tuple, list)):
        for item in obj:
            got = _coerce_session(item)
            if got is not None:
                return got
        return None
    # a cookie jar or a plain {name: value} dict of cookies
    if hasattr(obj, "get_dict") or isinstance(obj, dict):
        session = requests.Session()
        cookies = obj.get_dict() if hasattr(obj, "get_dict") else obj
        session.cookies.update(cookies)
        return session
    return None


def fetch_messages(login=None, limit=MAX_MESSAGES, dump_to=None):
    """Log into Webtop, pull changesAndMessages, return up to `limit` messages.

    `login` is the existing _webtop_login callable. Any failure -- bad
    credentials, portal down, DNS, a markup change -- yields [] rather than
    an exception, so the dashboard degrades to "no messages section".

    Pass dump_to="/tmp/webtop.html" once to capture the raw page; that is the
    fastest way to tighten the selectors below against the real DOM.
    """
    try:
        if login is None:
            return []
        session = _coerce_session(login())
        if session is None:
            return []

        url = WEBTOP_BASE.rstrip("/") + MESSAGES_PATH
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        html = resp.text

        if dump_to:
            with open(dump_to, "w", encoding="utf-8") as fh:
                fh.write(html)

        return parse_messages(html, limit=limit)
    except Exception:
        return []


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------

# Webtop is ASP.NET; class names are the stable-ish handle. Ordered most
# specific first -- the generic fallback runs only if none of these hit.
_ROW_SELECTORS = (
    "div.messageRow",
    "div.message-row",
    "tr.messageRow",
    "div.messageItem",
    "li.message",
    "[class*='essage'][class*='ow']",
    "[class*='essageItem']",
)
_TITLE_SELECTORS = (
    "[class*='itle']", "[class*='ubject']", "[class*='eader']",
    "h1", "h2", "h3", "h4", "b", "strong",
)
_BODY_SELECTORS = (
    "[class*='ontent']", "[class*='ody']", "[class*='ext']",
    "[class*='escription']", "p",
)


def _clean(text):
    """Collapse whitespace and strip the nbsp/bidi marks ASP.NET litters."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("‎", "").replace("‏", "").replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


def _shorten(text, limit=BODY_CHARS):
    text = _clean(text)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _first_text(node, selectors):
    for sel in selectors:
        found = node.select_one(sel)
        if found:
            text = _clean(found.get_text(" "))
            if text:
                return text, found
    return "", None


def parse_messages(html, limit=MAX_MESSAGES):
    """Extract messages from the changesAndMessages page.

    Kept separate from the network call so it can be unit-tested against
    saved fixtures without touching the portal.
    """
    from bs4 import BeautifulSoup

    if not html or not html.strip():
        return []

    soup = BeautifulSoup(html, "html.parser")
    for junk in soup(["script", "style", "noscript"]):
        junk.decompose()

    rows = []
    for sel in _ROW_SELECTORS:
        rows = soup.select(sel)
        if rows:
            break

    messages = []
    for row in rows:
        title, title_node = _first_text(row, _TITLE_SELECTORS)
        body, _ = _first_text(row, _BODY_SELECTORS)

        if not title and not body:
            continue
        if not title:
            title, body = body, ""
        elif not body:
            # No dedicated body element: use whatever text follows the title.
            whole = _clean(row.get_text(" "))
            if title_node is not None and whole.startswith(title):
                body = whole[len(title):]
            else:
                body = whole.replace(title, "", 1)

        title, body = _clean(title), _shorten(body)
        if title and body == title:
            body = ""
        if title:
            messages.append(Message(title=title, body=body))
        if len(messages) >= limit:
            break

    if not messages:
        messages = _parse_fallback(soup, limit)
    return messages[:limit]


def _parse_fallback(soup, limit):
    """Last resort: any repeated block that looks like a titled entry.

    Webtop's markup shifts between portal versions; this keeps the feature
    alive through a class rename instead of silently going blank.
    """
    out = []
    for node in soup.find_all(["tr", "li", "article", "div"]):
        if node.find(["tr", "li", "article"]):
            continue  # container, not a leaf row
        heading = node.find(["h1", "h2", "h3", "h4", "b", "strong"])
        if not heading:
            continue
        title = _clean(heading.get_text(" "))
        whole = _clean(node.get_text(" "))
        if not title or len(title) > 120:
            continue
        body = _shorten(whole[len(title):] if whole.startswith(title) else whole)
        if body == title:
            body = ""
        out.append(Message(title=title, body=body))
        if len(out) >= limit:
            break
    return out


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

# 16x16 1-bit bell. Inline so the dashboard needs no asset file alongside it.
BELL_ICON = (
    "................",
    ".......##.......",
    "......####......",
    ".....######.....",
    ".....######.....",
    ".....######.....",
    "....########....",
    "....########....",
    "....########....",
    "...##########...",
    "..############..",
    "################",
    "................",
    "......####......",
    ".......##.......",
    "................",
)


def draw_bell(draw, x, y, scale=1, fill=0):
    """Blit the bell at (x, y). Nearest-neighbour scaling keeps edges crisp."""
    for row, line in enumerate(BELL_ICON):
        for col, pixel in enumerate(line):
            if pixel != "#":
                continue
            x0, y0 = x + col * scale, y + row * scale
            if scale == 1:
                draw.point((x0, y0), fill=fill)
            else:
                draw.rectangle([x0, y0, x0 + scale - 1, y0 + scale - 1], fill=fill)


def _pillow_has_raqm():
    """True when Pillow is built against libraqm, which applies bidi itself."""
    try:
        from PIL import features
        return bool(features.check("raqm"))
    except Exception:
        return False


_HAS_RAQM = _pillow_has_raqm()


def shape_rtl(text):
    """Return `text` in the visual order PIL should draw it in.

    Pillow built with libraqm runs FriBidi internally, so the logical string
    is already correct and reordering it here would render Hebrew backwards.
    Without raqm, PIL draws codepoints left-to-right verbatim and we have to
    reorder ourselves. Verified both ways by glyph-position comparison.
    """
    if _HAS_RAQM:
        return text
    try:
        from bidi.algorithm import get_display
        return get_display(text)
    except Exception:
        return text


def _fit(draw, text, font, max_width):
    """Trim to fit max_width, appending an ellipsis. Operates on logical text."""
    if draw.textlength(shape_rtl(text), font=font) <= max_width:
        return text
    while text and draw.textlength(shape_rtl(text + "…"), font=font) > max_width:
        text = text[:-1]
    return (text.rstrip() + "…") if text else ""


def messages_region_height(messages, title_font_h=16, body_font_h=13):
    """Height this region will consume -- 0 when there is nothing to show.

    Call this while measuring the layout so the sections below shift up when
    Webtop has no messages, exactly like the AC row.
    """
    if not messages:
        return 0
    total = HEADER_H + PAD
    for msg in messages:
        total += title_font_h + (body_font_h + 1 if msg.body else 0) + ROW_GAP
    return total - ROW_GAP + PAD


def render_messages_region(draw, x, y, width, messages, title_font,
                           body_font, header_font, rtl=True):
    """Draw the "הודעות" region at (x, y) and return the height consumed.

    Returns 0 without drawing anything when `messages` is empty, so the
    caller can lay out unconditionally:

        h = render_messages_region(draw, X, cursor_y, W, msgs, ...)
        cursor_y += h + (SECTION_GAP if h else 0)
    """
    if not messages:
        return 0

    right = x + width
    cursor = y

    # header: bell + "הודעות", right-aligned for RTL
    label = "הודעות"
    shaped = shape_rtl(label) if rtl else label
    label_w = draw.textlength(shaped, font=header_font)
    if rtl:
        draw.text((right - label_w, cursor), shaped, font=header_font, fill=0)
        draw_bell(draw, int(right - label_w - 20), cursor + 1, scale=1)
    else:
        draw_bell(draw, x, cursor + 1, scale=1)
        draw.text((x + 20, cursor), shaped, font=header_font, fill=0)
    cursor += HEADER_H
    draw.line([(x, cursor), (right, cursor)], fill=0, width=1)
    cursor += PAD

    for i, msg in enumerate(messages):
        title = _fit(draw, msg.title, title_font, width)
        shaped_title = shape_rtl(title) if rtl else title
        tx = right - draw.textlength(shaped_title, font=title_font) if rtl else x
        draw.text((tx, cursor), shaped_title, font=title_font, fill=0)
        cursor += title_font.size + 1 if hasattr(title_font, "size") else 16

        if msg.body:
            body = _fit(draw, msg.body, body_font, width)
            shaped_body = shape_rtl(body) if rtl else body
            bx = right - draw.textlength(shaped_body, font=body_font) if rtl else x
            draw.text((bx, cursor), shaped_body, font=body_font, fill=0)
            cursor += body_font.size if hasattr(body_font, "size") else 13

        if i != len(messages) - 1:
            cursor += ROW_GAP

    return cursor - y + PAD
