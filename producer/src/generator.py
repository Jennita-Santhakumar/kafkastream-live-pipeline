"""Synthetic e-commerce event generation."""
import random
import time
import uuid
from dataclasses import dataclass, field

EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "remove_from_cart"]
EVENT_TYPE_WEIGHTS = [0.55, 0.20, 0.08, 0.12, 0.05]

NUM_USERS = 500


@dataclass
class EventGenerator:
    num_users: int = NUM_USERS
    seed: int | None = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def next_event(self) -> dict:
        rng = self._rng
        event_type = rng.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0]

        if event_type == "purchase":
            value = round(rng.uniform(15.0, 400.0), 2)
        elif event_type in ("add_to_cart", "remove_from_cart"):
            value = round(rng.uniform(5.0, 150.0), 2)
        else:
            value = round(rng.uniform(0.0, 5.0), 2)

        # occasionally emit an out-of-range value to exercise the streaming
        # job's validation stage with real bad data instead of only clean input
        if rng.random() < 0.01:
            value = -1.0

        now_ms = time.time_ns() // 1_000_000
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{rng.randint(1, self.num_users)}",
            "event_type": event_type,
            "event_time": now_ms,
            "value": value,
            "properties": {
                "session_id": str(rng.randint(1, self.num_users * 3)),
                "device": rng.choice(["mobile", "desktop", "tablet"]),
            },
        }
