"""Chat bursts from a Twitch VOD's replay as a hint for the moment picker."""
import chat_signals as cs
import twitch_signals as ts


def _chat(per_bucket, burst_at=None, burst=0, bucket=10):
    """One message per chatter, ``per_bucket`` chatters in every bucket."""
    comments = []
    for b, n in enumerate(per_bucket):
        n = burst if b == burst_at else n
        comments += [(b * bucket + k % bucket, f"u{b}_{k}") for k in range(n)]
    return comments


def test_bucket_rates_count_distinct_chatters_over_covered_seconds():
    comments = [(0, "a"), (1, "a"), (2, "b"), (3, "nightbot"), (15, "c")]
    rates = cs.bucket_rates(comments, [(0, 4), (10, 19)], 30)
    assert rates[0] == 2 / 5          # a spamming counts once, the bot not at all
    assert rates[1] == 1 / 10
    assert rates[2] is None           # no page looked there


def test_bucket_rates_ignore_messages_outside_covered_seconds():
    assert cs.bucket_rates([(5, "a")], [(0, 3)], 10) == [0.0]


def test_find_bursts_picks_the_spike_not_the_steady_chat():
    per_bucket = [4] * 60
    comments = _chat(per_bucket, burst_at=30, burst=40)
    rates = cs.bucket_rates(comments, [(0, 599)], 600)
    assert cs.find_bursts(rates) == [(30, 40.0)]


def test_find_bursts_nothing_on_flat_chat():
    rates = cs.bucket_rates(_chat([5] * 60), [(0, 599)], 600)
    assert cs.find_bursts(rates) == []


def test_find_bursts_keeps_one_per_minute_loudest_first():
    rates = [0.4] * 60
    rates[20], rates[22], rates[45] = 3.0, 5.0, 4.0
    assert cs.find_bursts(rates) == [(22, 50.0), (45, 40.0)]


def test_bursts_to_moments_open_before_the_burst_and_stay_in_the_vod():
    out = cs.bursts_to_moments([(30, 40.0), (1, 12.4), (59, 9.0)], 600)
    assert out == [{"start": 280.0, "duration": 30.0, "views": 40},
                   {"start": 0.0, "duration": 30.0, "views": 12},
                   {"start": 570.0, "duration": 30.0, "views": 9}]


def test_bursts_feed_shift_clips_like_viewer_clips():
    moments = cs.bursts_to_moments([(400, 40.0)], 7200)
    assert ts.shift_clips(moments, 3600, 1800) == [{"start": 380.0, "end": 410.0, "views": 40}]


def test_chunk_bounds_cover_the_vod_bucket_aligned():
    chunks = cs.chunk_bounds(1005, 4)
    assert chunks[0][0] == 0 and chunks[-1][1] == 1005
    assert all(a[1] == b[0] for a, b in zip(chunks, chunks[1:]))
    assert all(s % 10 == 0 for s, _ in chunks)
    assert cs.chunk_bounds(25, 8) == [(0, 10), (10, 20), (20, 25)]


def test_walk_samples_once_per_bucket_in_dense_chat_and_jumps_in_quiet(monkeypatch):
    asked = []

    def fake_page(client, vod_id, offset):
        asked.append(offset)
        if offset < 100:                      # dense: a page covers 2 s
            return [(offset, "a"), (offset + 1, "b")]
        return [(offset, "c"), (offset + 45, "d")]   # quiet: a page covers 45 s

    monkeypatch.setattr(cs, "_page", fake_page)
    _, spans = cs._walk(None, "1", (0, 200), budget=100)
    assert asked == [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 146, 192]
    assert spans[-2:] == [(146, 191), (192, 237)]


def test_walk_skips_a_failed_page_and_gives_up_after_a_run(monkeypatch):
    def flaky(client, vod_id, offset):
        if offset == 10 or offset >= 50:
            raise TimeoutError
        return [(offset, "a")]

    monkeypatch.setattr(cs, "_page", flaky)
    comments, spans = cs._walk(None, "1", (0, 200), budget=100)
    assert comments == [(0, "a"), (20, "a"), (30, "a"), (40, "a")]
    assert (10, 10) not in spans          # the failed bucket stays unknown


def test_fetch_chat_moments_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("TWITCH_CHAT_SIGNAL", "0")
    assert cs.fetch_chat_moments("123") == []
