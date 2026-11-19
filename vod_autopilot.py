"""Unattended Twitch VOD -> clips -> scheduled posts, for a self-hosted OpenShorts.

One run is one idempotent tick (run it from Task Scheduler / cron every ~30 min):

  1. a VOD from the list that has no job yet is submitted to ``/api/process``,
     on the stretch where its chat burst most (``chat_signals`` + ``best_stretch``);
  2. a finished job's best clips (``predicted_score``) are scheduled on Upload-Post
     through ``/api/social/post``, at fixed hours, ``--per-vod`` clips per VOD;
  3. progress lives in a JSON state file, so a crash or a reboot repeats nothing.

Keys are read from the environment of this process (``GEMINI_API_KEY``,
``UPLOAD_POST_API_KEY``) and sent as the headers the self-host API already takes.
Submitting needs ``--i-have-rights``: that flag is the content-rights attestation
the API asks for, and it is yours to make, not the script's.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

import httpx

import chat_signals
import twitch_signals

TERMINAL_BAD = {"failed", "cancelled"}


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def parse_vods(text: str) -> List[str]:
    """Twitch VOD urls, one per line; blanks, ``#`` comments and non-VODs dropped."""
    out = []
    for line in text.splitlines():
        url = line.split("#", 1)[0].strip()
        if url and twitch_signals.parse_vod_id(url) and url not in out:
            out.append(url)
    return out


def next_slots(now: datetime, hours: List[int], count: int,
               after: Optional[datetime] = None) -> List[datetime]:
    """``count`` publish times at ``hours`` (tz-aware, ``now``'s zone), each later
    than ``now`` and than ``after`` (the last slot already used)."""
    floor = max(now, after) if after else now
    out, day = [], floor.replace(hour=0, minute=0, second=0, microsecond=0)
    while len(out) < count:
        for h in sorted(hours):
            slot = day.replace(hour=h)
            if slot > floor and len(out) < count:
                out.append(slot)
        day += timedelta(days=1)
    return out


def top_clips(clips: List[dict], n: int) -> List[int]:
    """Indices of the ``n`` best clips by ``predicted_score``; unscored ones last."""
    def score(i):
        try:
            return float(clips[i].get("predicted_score"))
        except (TypeError, ValueError, AttributeError):
            return float("-inf")
    return sorted(range(len(clips)), key=lambda i: (-score(i), i))[:n]


def tagged(text: str, tag: str) -> str:
    """``text`` ending with ``tag`` (campaign hashtags / mentions), once."""
    text = (text or "").strip()
    return text if not tag or tag in text else f"{text} {tag}".strip()


def pick_channel_vods(nodes: List[dict], live: bool, min_minutes: int = 20) -> List[str]:
    """Ids of a channel's finished VODs, oldest first.

    ``nodes`` come newest first (Twitch ``videos`` by time). While the channel is
    live its newest VOD is the broadcast in progress: skipped until it ends.
    A VOD's own ``game`` is only its first category, so the game is read per
    chapter (``game_ranges``), not here.
    """
    if live and nodes:
        nodes = nodes[1:]
    return [str(n["id"]) for n in reversed(nodes)
            if int(n.get("lengthSeconds") or 0) >= min_minutes * 60]


def game_ranges(chapters: List[dict], game: str) -> List[tuple]:
    """``[(start, end)]`` seconds of a VOD spent in ``game`` (its chapter markers)."""
    out = []
    for ch in chapters:
        name = (((ch.get("details") or {}).get("game")) or {}).get("displayName", "")
        if name.lower() == game.lower():
            start = int(ch.get("positionMilliseconds") or 0) / 1000
            out.append((start, start + int(ch.get("durationMilliseconds") or 0) / 1000))
    return out


def in_ranges(moments: List[dict], ranges: Optional[List[tuple]]) -> List[dict]:
    """Moments lying wholly inside one of ``ranges`` (all when ``ranges`` is None)."""
    if ranges is None:
        return moments
    return [m for m in moments if any(a <= m["start"] and m["start"] + m["duration"] <= b
                                      for a, b in ranges)]


def posts_left(state: dict, month: str, cap: int) -> int:
    return max(0, cap - sum(1 for t in state.get("posted_at", []) if t.startswith(month)))


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #
def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"vods": {}, "posted_at": [], "last_slot": None}


def save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
    tmp.replace(path)          # a crash mid-write never leaves half a file


# --------------------------------------------------------------------------- #
# API
# --------------------------------------------------------------------------- #
def _headers() -> dict:
    h = {}
    if os.environ.get("GEMINI_API_KEY"):
        h["X-Gemini-Key"] = os.environ["GEMINI_API_KEY"]
    if os.environ.get("UPLOAD_POST_API_KEY"):
        h["X-Upload-Post-Key"] = os.environ["UPLOAD_POST_API_KEY"]
    return h


def channel_vods(login: str, game: str, skip=()) -> dict:
    """``{vod url: [(start, end), ...]}`` of the channel's finished VODs that spent
    time in ``game``, oldest first; the seconds are where it was played."""
    q = ('query{user(login:"%s"){stream{id} videos(first:30,type:ARCHIVE,sort:TIME)'
         '{edges{node{id lengthSeconds}}}}}' % login)
    cq = ('query{video(id:"%s"){moments(momentRequestType:VIDEO_CHAPTER_MARKERS)'
          '{edges{node{positionMilliseconds durationMilliseconds '
          'details{...on GameChangeMomentDetails{game{displayName}}}}}}}}')
    out = {}
    with httpx.Client(timeout=15) as c:
        user = (chat_signals._gql(c, q).get("user")) or {}
        nodes = [e["node"] for e in (user.get("videos") or {}).get("edges") or []]
        for vid in pick_channel_vods(nodes, bool(user.get("stream"))):
            url = f"https://www.twitch.tv/videos/{vid}"
            if url in skip:
                continue
            video = chat_signals._gql(c, cq % vid).get("video") or {}
            chapters = [e["node"] for e in (video.get("moments") or {}).get("edges") or []]
            ranges = game_ranges(chapters, game)
            if ranges:
                out[url] = ranges
    return out


def pick_stretch(url: str, minutes: int, ranges=None):
    """``(start, end)`` seconds of the VOD where chat burst most, else None."""
    vod_id = twitch_signals.parse_vod_id(url)
    moments = in_ranges(chat_signals.fetch_chat_moments(vod_id), ranges)
    with httpx.Client(timeout=15) as c:
        total = chat_signals._vod_length(c, vod_id)
    return twitch_signals.best_stretch(moments, total, minutes * 60)


def submit(api, url, args, ranges=None) -> Optional[str]:
    body = {"url": url, "acknowledged": True, "target_clips": args.per_vod + 2}
    stretch = pick_stretch(url, args.minutes, ranges)
    if stretch:
        body["source_start"], body["source_end"] = stretch
    else:
        body["max_minutes"] = args.minutes
    r = api.post("/api/process", json=body)
    if r.status_code == 429:           # job limit: try again next tick
        return None
    r.raise_for_status()
    return r.json()["job_id"]


def schedule_posts(api, job_id, clips, args, state, now) -> int:
    last = state.get("last_slot")
    slots = next_slots(now, args.hours, args.per_vod,
                       datetime.fromisoformat(last) if last else None)
    budget = posts_left(state, now.strftime("%Y-%m"), args.monthly_cap)
    done = 0
    for idx, slot in zip(top_clips(clips, args.per_vod), slots[:budget]):
        clip = clips[idx]
        title = tagged(clip.get("title") or clip.get("video_title_for_youtube_short"), args.tag)
        desc = tagged(clip.get("video_description_for_tiktok") or clip.get("title"), args.tag)
        r = api.post("/api/social/post", json={
            "job_id": job_id, "clip_index": idx, "platforms": args.platforms,
            "title": title, "description": desc,
            "user_id": args.profile, "timezone": args.tz,
            "scheduled_date": slot.replace(tzinfo=None).isoformat(timespec="seconds")})
        if r.status_code >= 400:
            print(f"   post failed ({r.status_code}): {r.text[:200]}")
            break
        state["last_slot"] = slot.isoformat()
        state.setdefault("posted_at", []).append(now.isoformat())
        done += 1
    return done


def tick(args) -> None:
    now = datetime.now(ZoneInfo(args.tz))
    path = Path(args.state)
    state = load_state(path)
    if args.channel:
        found = channel_vods(args.channel, args.game, skip=set(state["vods"]))
    else:
        found = {u: None for u in parse_vods(Path(args.vods).read_text(encoding="utf-8"))}
    urls = list(found)
    with httpx.Client(base_url=args.api, headers=_headers(), timeout=60) as api:
        for url, v in list(state["vods"].items()):
            if v["status"] != "processing":
                continue
            s = api.get(f"/api/status/{v['job_id']}").json()
            if s["status"] == "completed":
                clips = (s.get("result") or {}).get("clips") or []
                if not args.profile:       # clip-only: review in the dashboard first
                    v["status"] = "ready"
                    print(f"READY {url}: {len(clips)} clip(s), job {v['job_id']} (no --profile: not posted)")
                else:
                    n = schedule_posts(api, v["job_id"], clips, args, state, now)
                    v["status"], v["posted"] = "scheduled", n
                    print(f"OK {url}: {n} post(s) scheduled")
            elif s["status"] in TERMINAL_BAD:
                v["status"] = "failed"
                print(f"FAILED {url}: job {s['status']}")
            save_state(path, state)
        busy = any(v["status"] == "processing" for v in state["vods"].values())
        todo = [u for u in urls if u not in state["vods"]]
        if todo and not busy and args.i_have_rights:
            job = submit(api, todo[0], args, found[todo[0]])
            if job:
                state["vods"][todo[0]] = {"job_id": job, "status": "processing"}
                print(f"SUBMITTED {todo[0]}: job {job}")
        elif todo and not args.i_have_rights:
            print("Nothing submitted: pass --i-have-rights (you own or may use the content).")
    save_state(path, state)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--vods", default="vods.txt", help="file with Twitch VOD urls, one per line")
    p.add_argument("--channel", help="Twitch login: take its finished VODs of --game instead of --vods")
    p.add_argument("--game", default="Grand Theft Auto VI")
    p.add_argument("--tag", default="", help="appended to every title and description")
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--state", default="vod_autopilot_state.json")
    p.add_argument("--profile", help="Upload-Post profile name; without it clips are made but not posted")
    p.add_argument("--platforms", default="tiktok,youtube", type=lambda s: s.split(","))
    p.add_argument("--hours", default="12,19", type=lambda s: [int(h) for h in s.split(",")])
    p.add_argument("--tz", default="Asia/Amman")
    p.add_argument("--per-vod", type=int, default=4, help="clips posted per VOD")
    p.add_argument("--minutes", type=int, default=20, help="stretch of the VOD to clip")
    p.add_argument("--monthly-cap", type=int, default=10, help="Upload-Post free plan: 10")
    p.add_argument("--i-have-rights", action="store_true")
    tick(p.parse_args(argv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
