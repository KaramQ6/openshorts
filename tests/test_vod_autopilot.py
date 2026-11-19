from datetime import datetime
from zoneinfo import ZoneInfo

import vod_autopilot as va

TZ = ZoneInfo("Asia/Amman")


def test_parse_vods_keeps_vod_links_once():
    text = ("https://www.twitch.tv/videos/123 # best\n\nhttps://youtube.com/x\n"
            "https://www.twitch.tv/videos/123\n")
    assert va.parse_vods(text) == ["https://www.twitch.tv/videos/123"]


def test_next_slots_fill_today_then_tomorrow():
    now = datetime(2026, 11, 20, 10, 0, tzinfo=TZ)
    out = va.next_slots(now, [12, 19], 3)
    assert [(s.day, s.hour) for s in out] == [(20, 12), (20, 19), (21, 12)]


def test_next_slots_continue_after_the_last_used_one():
    now = datetime(2026, 11, 20, 10, 0, tzinfo=TZ)
    after = datetime(2026, 11, 22, 19, 0, tzinfo=TZ)
    assert va.next_slots(now, [12, 19], 1, after)[0].day == 23


def test_top_clips_by_score_unscored_last():
    clips = [{"predicted_score": 3}, {}, {"predicted_score": 9}, {"predicted_score": 5}]
    assert va.top_clips(clips, 3) == [2, 3, 0]


def test_posts_left_counts_only_this_month():
    state = {"posted_at": ["2026-10-30T10:00", "2026-11-02T10:00", "2026-11-03T10:00"]}
    assert va.posts_left(state, "2026-11", 10) == 8
    assert va.posts_left(state, "2026-11", 2) == 0


def test_state_roundtrip_and_missing_file(tmp_path):
    p = tmp_path / "s.json"
    assert va.load_state(p)["vods"] == {}
    va.save_state(p, {"vods": {"u": {"status": "processing"}}})
    assert va.load_state(p)["vods"]["u"]["status"] == "processing"


def test_tagged_appends_once():
    assert va.tagged("Wild chase", "#X @Y") == "Wild chase #X @Y"
    assert va.tagged("Wild chase #X @Y", "#X @Y") == "Wild chase #X @Y"
    assert va.tagged(None, "#X") == "#X"


def test_channel_vods_skip_the_live_broadcast_and_short_ones():
    nodes = [{"id": 3, "lengthSeconds": 7200}, {"id": 2, "lengthSeconds": 7200},
             {"id": 1, "lengthSeconds": 60}]
    assert va.pick_channel_vods(nodes, live=True) == ["2"]
    assert va.pick_channel_vods(nodes, live=False) == ["2", "3"]


def test_game_ranges_reads_chapters_and_in_ranges_filters_moments():
    chapters = [
        {"positionMilliseconds": 0, "durationMilliseconds": 174000,
         "details": {"game": {"displayName": "Just Chatting"}}},
        {"positionMilliseconds": 174000, "durationMilliseconds": 10361000,
         "details": {"game": {"displayName": "Grand Theft Auto VI"}}}]
    r = va.game_ranges(chapters, "Grand Theft Auto VI")
    assert r == [(174.0, 10535.0)]
    moments = [{"start": 100.0, "duration": 30.0}, {"start": 500.0, "duration": 30.0}]
    assert va.in_ranges(moments, r) == [moments[1]]
    assert va.in_ranges(moments, None) == moments
