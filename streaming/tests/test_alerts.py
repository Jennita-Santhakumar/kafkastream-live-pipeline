from alerts import MIN_HISTORY, RollingStats, check_alerts


def test_rolling_stats_no_alert_with_insufficient_history():
    rolling = RollingStats()
    for _ in range(MIN_HISTORY - 1):
        z, anomalous = rolling.evaluate("purchase", 10.0)
        assert anomalous is False


def test_rolling_stats_flags_extreme_deviation():
    rolling = RollingStats()
    for _ in range(10):
        rolling.evaluate("purchase", 10.0)
    z, anomalous = rolling.evaluate("purchase", 1000.0)
    # z is None here: stddev is exactly 0 for a constant 10-value history, so
    # a real z-score is undefined even though the deviation is still flagged.
    assert anomalous is True
    assert z is None


def test_rolling_stats_no_alert_for_stable_values():
    rolling = RollingStats()
    z = None
    anomalous = False
    for value in [10.0, 11.0, 9.0, 10.5, 9.5, 10.0, 10.2, 9.8, 10.1, 9.9]:
        z, anomalous = rolling.evaluate("purchase", value)
    assert anomalous is False


def test_check_alerts_flags_high_error_rate(spark):
    from enrichment import validate

    raw = spark.createDataFrame(
        [
            {
                "event_id": f"e{i}",
                "user_id": "u1",
                "event_type": "purchase",
                "value": -1.0,
                "event_time": "2024-01-01 00:00:00",
            }
            for i in range(20)
        ]
        + [
            {
                "event_id": "good",
                "user_id": "u1",
                "event_type": "purchase",
                "value": 5.0,
                "event_time": "2024-01-01 00:00:00",
            }
        ]
    )
    validated = validate(raw)
    alerts = check_alerts(validated, raw, RollingStats(), batch_id=1)
    error_alerts = [a for a in alerts if a["alert_type"] == "high_error_rate"]
    assert len(error_alerts) == 1


def test_check_alerts_no_alerts_on_clean_stable_batch(spark):
    from enrichment import validate

    raw = spark.createDataFrame(
        [
            {
                "event_id": f"e{i}",
                "user_id": "u1",
                "event_type": "purchase",
                "value": 5.0,
                "event_time": "2024-01-01 00:00:00",
            }
            for i in range(10)
        ]
    )
    validated = validate(raw)
    alerts = check_alerts(validated, raw, RollingStats(), batch_id=1)
    assert alerts == []
