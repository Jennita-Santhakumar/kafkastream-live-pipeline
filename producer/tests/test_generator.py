import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from generator import EVENT_TYPES, EventGenerator


def test_next_event_has_required_fields():
    gen = EventGenerator(seed=42)
    event = gen.next_event()

    assert set(event.keys()) == {
        "event_id",
        "user_id",
        "event_type",
        "event_time",
        "value",
        "properties",
    }
    assert event["event_type"] in EVENT_TYPES
    assert event["user_id"].startswith("user_")


def test_events_are_unique():
    gen = EventGenerator(seed=1)
    ids = {gen.next_event()["event_id"] for _ in range(200)}
    assert len(ids) == 200


def test_purchase_events_have_higher_average_value_than_page_views():
    gen = EventGenerator(seed=7)
    purchases = []
    page_views = []
    for _ in range(3000):
        event = gen.next_event()
        if event["event_type"] == "purchase":
            purchases.append(event["value"])
        elif event["event_type"] == "page_view":
            page_views.append(event["value"])

    assert purchases and page_views
    assert (sum(purchases) / len(purchases)) > (sum(page_views) / len(page_views))


def test_event_time_is_millisecond_epoch():
    gen = EventGenerator(seed=3)
    event = gen.next_event()
    # sanity bound: between 2020-01-01 and 2100-01-01 in epoch ms
    assert 1_577_836_800_000 < event["event_time"] < 4_102_444_800_000
