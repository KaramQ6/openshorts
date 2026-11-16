"""Where a Twitch VOD's chat exploded, as a signal for the moment picker.

Viewer clips (``twitch_signals``) only exist on streams big enough for people
to clip them, and need app credentials. Chat replay exists on every VOD and is
readable anonymously: a chase, a crash or a heist finale is twelve seconds of
nobody talking on stream and a wall of messages in chat, a few seconds late.

How it reads chat: Twitch's public GQL answers ``comments(contentOffsetSeconds)``
with one page (~60 messages) around that second; cursor paging is refused
without an integrity token, so the VOD is sampled by offset instead. At most
one page per ``BUCKET_SECONDS`` bucket, fewer where chat is quiet (one page
then spans several buckets). Each bucket's rate is the number of distinct
chatters per second over the seconds the pages actually covered, so one user
spamming or a dense chat sampled for two seconds both read correctly.

A burst is a bucket well above its rolling median (``find_bursts``). It becomes
a moment that opens ``LEAD_SECONDS`` before the bucket (chat lags the screen)
in the ``{"start", "duration", "views"}`` shape of a viewer clip, ``views``
being the distinct chatters of the bucket, so every later stage handles both.

Best effort, like viewer clips: off with ``TWITCH_CHAT_SIGNAL=0``; any failure
returns [] and the job carries on without it. The pure helpers are
standard-library only and unit-tested.
"""
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor
from statistics import median
from typing import Dict, List, Optional, Set, Tuple

GQL_URL = "https://gql.twitch.tv/gql"
# The public client id twitch.tv itself sends; it reads public VOD data only.
GQL_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"

BUCKET_SECONDS = 10
BASELINE_BUCKETS = 30     # rolling median over +-5 min
MIN_Z = 3.0               # burst: this many sigmas over the baseline...
MIN_RATIO = 2.0           # ...and at least this multiple of it
MIN_SPACING_BUCKETS = 6   # one burst per minute at most
MAX_BURSTS = 25
LEAD_SECONDS = 20         # chat reacts after the moment
MOMENT_SECONDS = 30
FETCH_WORKERS = 8
MAX_PAGES = 6000          # ~16 h of dense chat at one page per bucket
MAX_CONSECUTIVE_FAILURES = 3

# Bots post on timers and on events, not on what happens on screen.
BOT_LOGINS = frozenset({
    "nightbot", "streamelements", "streamlabs", "moobot", "fossabot",
    "wizebot", "soundalerts", "sery_bot", "pokemoncommunitygame", "botrixoficial",
})

Comment = Tuple[int, str]        # (second in the VOD, chatter login)
Span = Tuple[int, int]           # [first, last] seconds a page covered, inclusive


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def bucket_rates(comments: List[Comment], spans: List[Span], duration: int,
                 bucket: int = BUCKET_SECONDS) -> List[Optional[float]]:
    """Distinct chatters per second for each bucket, None where no page looked.

    O(duration + comments): coverage and chatters are indexed per second once.
    """
    n = max(1, math.ceil(duration / bucket))
    covered = bytearray(n * bucket)
    for first, last in spans:
        lo, hi = max(0, first), min(len(covered) - 1, last)
        if lo <= hi:
            covered[lo:hi + 1] = b"\x01" * (hi - lo + 1)
    chatters: List[Set[str]] = [set() for _ in range(n)]
    for sec, login in comments:
        if 0 <= sec < len(covered) and covered[sec] and login not in BOT_LOGINS:
            chatters[sec // bucket].add(login)
    rates: List[Optional[float]] = []
    for i in range(n):
        seen = sum(covered[i * bucket:(i + 1) * bucket])
        rates.append(len(chatters[i]) / seen if seen else None)
    return rates


def find_bursts(rates: List[Optional[float]], bucket: int = BUCKET_SECONDS,
                max_bursts: int = MAX_BURSTS) -> List[Tuple[int, float]]:
    """``[(bucket index, chatters in the bucket)]``, loudest first.

    A burst beats its rolling median by ``MIN_Z`` Poisson sigmas and by
    ``MIN_RATIO``; louder bursts suppress others within ``MIN_SPACING_BUCKETS``.
    apex: O(n * BASELINE_BUCKETS) median scan, n = duration / 10 s (~6k for 16 h).
    """
    counts = [None if r is None else r * bucket for r in rates]
    known = [i for i, c in enumerate(counts) if c is not None]
    candidates = []
    for i in known:
        lo, hi = i - BASELINE_BUCKETS, i + BASELINE_BUCKETS
        around = [counts[j] for j in range(max(0, lo), min(len(counts), hi + 1))
                  if j != i and counts[j] is not None]
        if not around:
            continue
        base = median(around)
        c = counts[i]
        if (c - base) / math.sqrt(base + 1) >= MIN_Z and c >= MIN_RATIO * max(base, 1.0):
            candidates.append((i, c))
    candidates.sort(key=lambda ic: -ic[1])
    kept: List[Tuple[int, float]] = []
    for i, c in candidates:
        if all(abs(i - k) >= MIN_SPACING_BUCKETS for k, _ in kept):
            kept.append((i, c))
            if len(kept) == max_bursts:
                break
    return kept


def bursts_to_moments(bursts: List[Tuple[int, float]], duration: int,
                      bucket: int = BUCKET_SECONDS) -> List[dict]:
    """Viewer-clip-shaped moments in VOD seconds: ``{"start", "duration", "views"}``."""
    out = []
    for i, c in bursts:
        start = max(0.0, float(i * bucket - LEAD_SECONDS))
        length = min(float(MOMENT_SECONDS), max(1.0, duration - start))
        out.append({"start": start, "duration": length, "views": int(round(c))})
    return out


def chunk_bounds(duration: int, parts: int, bucket: int = BUCKET_SECONDS) -> List[Span]:
    """Split ``[0, duration)`` into ``parts`` bucket-aligned ``[start, end)`` chunks."""
    n = max(1, math.ceil(duration / bucket))
    parts = max(1, min(parts, n))
    edges = [round(k * n / parts) * bucket for k in range(parts + 1)]
    return [(edges[k], min(duration, edges[k + 1])) for k in range(parts)
            if edges[k] < min(duration, edges[k + 1])]


# --------------------------------------------------------------------------- #
# GQL
# --------------------------------------------------------------------------- #
def _gql(client, query: str) -> dict:
    r = client.post(GQL_URL, headers={"Client-Id": GQL_CLIENT_ID},
                    content=json.dumps({"query": query}))
    r.raise_for_status()
    body = r.json() or {}
    if body.get("errors"):
        raise RuntimeError(body["errors"][0].get("message", "GQL error"))
    return body.get("data") or {}


def _vod_length(client, vod_id: str) -> int:
    data = _gql(client, 'query{video(id:"%s"){lengthSeconds}}' % vod_id)
    return int((data.get("video") or {}).get("lengthSeconds") or 0)


def _page(client, vod_id: str, offset: int) -> List[Comment]:
    data = _gql(client, 'query{video(id:"%s"){comments(contentOffsetSeconds:%d)'
                        '{edges{node{contentOffsetSeconds commenter{login}}}}}}'
                % (vod_id, offset))
    edges = (((data.get("video") or {}).get("comments") or {}).get("edges")) or []
    out = []
    for e in edges:
        node = e.get("node") or {}
        login = ((node.get("commenter") or {}).get("login")) or ""
        if node.get("contentOffsetSeconds") is not None and login:
            out.append((int(node["contentOffsetSeconds"]), login.lower()))
    return out


def _walk(client, vod_id: str, chunk: Span, budget: int):
    """Sample one chunk. Returns ``(comments, spans)``; each call owns its lists."""
    start, end = chunk
    comments: List[Comment] = []
    spans: List[Span] = []
    t, failures = start, 0
    while t < end and len(spans) < budget:
        try:
            page = _page(client, vod_id, t)
            failures = 0
        except Exception as e:
            # One slow or refused page must not cost the whole signal: skip
            # its bucket (left unknown, not zero). Give the chunk up after a run.
            failures += 1
            if failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"   ℹ️ Twitch chat: stopped a chunk at {t}s ({type(e).__name__}).")
                break
            t += BUCKET_SECONDS
            continue
        if not page:
            spans.append((t, t + BUCKET_SECONDS - 1))   # looked, nobody talked
            t += BUCKET_SECONDS
            continue
        first, last = min(s for s, _ in page), max(s for s, _ in page)
        comments.extend(page)
        spans.append((first, last))
        # Quiet chat: the page reached far, jump past it. Dense chat: one
        # sample per bucket keeps the page count bounded by duration / 10 s.
        t = max(last + 1, (t // BUCKET_SECONDS + 1) * BUCKET_SECONDS)
    return comments, spans


def fetch_chat_moments(vod_id: str, timeout: float = 15.0) -> List[dict]:
    """Chat-burst moments in VOD seconds, loudest first. [] on any failure."""
    if not vod_id or os.environ.get("TWITCH_CHAT_SIGNAL", "1").strip() == "0":
        return []
    import httpx
    try:
        with httpx.Client(timeout=timeout) as client:
            duration = _vod_length(client, vod_id)
            if duration <= 0:
                return []
            chunks = chunk_bounds(duration, FETCH_WORKERS)
            budget = max(1, MAX_PAGES // len(chunks))
            with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
                parts = list(pool.map(lambda ch: _walk(client, vod_id, ch, budget), chunks))
    except Exception as e:  # best effort: never fail a job over a hint
        print(f"   ℹ️ Twitch chat replay unavailable ({type(e).__name__}: {e}).")
        return []
    comments = [c for part, _ in parts for c in part]
    spans = [s for _, part in parts for s in part]
    moments = bursts_to_moments(find_bursts(bucket_rates(comments, spans, duration)),
                                duration)
    print(f"   💬 Twitch chat: {len(moments)} burst(s) from {len(comments)} messages "
          f"sampled over {len(spans)} page(s).")
    return moments
