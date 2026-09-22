from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from data.plugins.astrbot_plugin_codex_reset import forecast as forecast_module
from data.plugins.astrbot_plugin_codex_reset.forecast import Forecast
from data.plugins.astrbot_plugin_codex_reset.main import CodexResetPlugin

NOW = datetime(2026, 9, 22, 7, tzinfo=timezone.utc)
GROUP = "bot:GroupMessage:123"
OTHER = "bot:GroupMessage:456"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    """Freeze time at the observed live Watch snapshot."""

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz)

    monkeypatch.setattr(forecast_module, "datetime", FrozenDatetime)


@pytest.fixture
def payload():
    """Provide the API fields involved in the September 22 regression."""
    window = {
        "target_at": "2026-09-23T06:59:59.999Z",
        "end_at": "2026-09-23T06:59:59.999Z",
        "target_kind": "deadline",
    }
    return {
        "updated_at": NOW.isoformat(),
        "last_reset_at": "2026-09-12T08:09:17Z",
        "probabilities": {"rounded_24h": 20, "rounded_48h": 35},
        "confidence": "low",
        "alert_event_id": "signal:2102254445082116335:likely",
        "official_signal": {"window": deepcopy(window)},
        "latest_alert": {
            "kind": "watch",
            "state": "active",
            "source_at": "2026-09-22T04:31:32Z",
            "summary": "A reset is planned for Tuesday.",
            "url": "https://x.com/thsottiaux/status/2102254445082116335",
            "score": 93,
            "window": window,
        },
    }


@pytest.fixture
def plugin():
    """Create a plugin with isolated storage and mocked message delivery."""
    context = SimpleNamespace(
        send_message=AsyncMock(return_value=True),
        platform_manager=SimpleNamespace(platform_insts=[]),
    )
    instance = CodexResetPlugin(context, {"group_ids": [GROUP]})
    instance.known_groups = {GROUP}
    instance.delivery_cursors = {GROUP: "2026-09-12T08:09:17+00:00"}
    instance.put_kv_data = AsyncMock()
    return instance


def test_forecast_shows_promise_score_window_and_source(payload):
    forecast = Forecast.from_payload(payload)
    rendered = forecast.render(timezone(timedelta(hours=8)), "forecast")
    assert "Tibo 已预告重置" in rendered
    assert "93%（不是 24 小时概率）" in rendered
    assert "24 小时内重置概率：20%" in rendered
    assert "2026-09-23 14:59:59" in rendered
    assert "2026-09-22 12:31:32" in rendered
    assert payload["latest_alert"]["url"] in rendered
    assert "来源：https://codex-reset.com/" in rendered
    last = forecast.render(timezone.utc, "last")
    assert "2026-09-12 08:09:17" in last
    assert "预告" not in last


@pytest.mark.parametrize("score", [83, 93])
@pytest.mark.asyncio
async def test_existing_group_gets_promise_without_new_reset(plugin, payload, score):
    payload["latest_alert"]["score"] = score
    forecast = Forecast.from_payload(payload)
    before = dict(plugin.delivery_cursors)
    await plugin.notify_groups(forecast)
    await plugin.notify_groups(forecast)
    plugin.context.send_message.assert_awaited_once()
    message = plugin.context.send_message.await_args.args[1].chain[0].text
    assert "【Codex 重置预告】" in message
    assert "尚未确认重置完成" in message
    assert "历史模型" not in message
    assert plugin.delivery_cursors == before
    assert plugin.alert_cursors[GROUP] == payload["alert_event_id"]
    saved = plugin.put_kv_data.await_args.args[1]
    assert saved["alerts"][GROUP] == payload["alert_event_id"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", "reset"),
        ("kind", "banked"),
        ("state", "expired"),
        ("state", "corrected"),
        ("score", 50),
        ("score", True),
        ("score", {}),
        ("score", []),
        ("score", "93"),
        ("source_at", "invalid"),
        ("source_at", "2026-09-12T08:09:17Z"),
        ("source_at", "2026-09-23T00:00:00Z"),
        ("window", {"end_at": "2026-09-22T06:59:59Z"}),
        ("window", {"end_at": "invalid"}),
        ("window", [1]),
    ],
)
@pytest.mark.asyncio
async def test_inactive_or_malformed_signal_does_not_notify(
    plugin, payload, field, value
):
    payload["latest_alert"][field] = value
    await plugin.notify_groups(Forecast.from_payload(payload))
    plugin.context.send_message.assert_not_awaited()
    assert plugin.alert_cursors == {}


@pytest.mark.parametrize(
    "field,value",
    [
        ("latest_alert", None),
        ("alert_event_id", None),
        ("alert_event_id", ""),
        ("alert_event_id", []),
        ("stale", True),
        ("updated_at", "2026-09-22T06:00:00Z"),
    ],
)
@pytest.mark.asyncio
async def test_missing_or_stale_source_does_not_notify(plugin, payload, field, value):
    payload[field] = value
    await plugin.notify_groups(Forecast.from_payload(payload))
    plugin.context.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_undated_promise_can_notify_without_reset_history(plugin, payload):
    payload["last_reset_at"] = None
    payload["latest_alert"]["window"] = None
    payload["official_signal"] = None
    payload["latest_alert"]["score"] = 83
    plugin.delivery_cursors[GROUP] = None
    await plugin.notify_groups(Forecast.from_payload(payload))
    plugin.context.send_message.assert_awaited_once()
    assert plugin.delivery_cursors[GROUP] is None


@pytest.mark.asyncio
async def test_new_group_skips_past_reset_but_receives_current_promise(plugin, payload):
    plugin.delivery_cursors[GROUP] = None
    await plugin.notify_groups(Forecast.from_payload(payload))
    plugin.context.send_message.assert_awaited_once()
    assert plugin.delivery_cursors[GROUP] == "2026-09-12T08:09:17+00:00"


@pytest.mark.parametrize("failure", [False, RuntimeError("offline")])
@pytest.mark.asyncio
async def test_failed_group_retries_independently(plugin, payload, failure):
    plugin.config["group_ids"].append(OTHER)
    plugin.delivery_cursors[OTHER] = plugin.delivery_cursors[GROUP]
    plugin.context.send_message.side_effect = [failure, True, True]
    forecast = Forecast.from_payload(payload)
    await plugin.notify_groups(forecast)
    assert GROUP not in plugin.alert_cursors
    assert plugin.alert_cursors[OTHER] == payload["alert_event_id"]
    await plugin.notify_groups(forecast)
    assert plugin.context.send_message.await_count == 3
    assert plugin.context.send_message.await_args.args[0] == GROUP


@pytest.mark.asyncio
async def test_confirmation_after_promise_still_notifies(plugin, payload):
    await plugin.notify_groups(Forecast.from_payload(payload))
    payload["last_reset_at"] = "2026-09-22T06:30:00Z"
    # Even a lagging active Watch must not be sent after the reset fulfills it.
    await plugin.notify_groups(Forecast.from_payload(payload))
    await plugin.notify_groups(Forecast.from_payload(payload))
    assert plugin.context.send_message.await_count == 2
    text = plugin.context.send_message.await_args.args[1].chain[0].text
    assert "【Codex 额度重置通知】" in text
    assert plugin.delivery_cursors[GROUP] == "2026-09-22T06:30:00+00:00"


@pytest.mark.asyncio
async def test_failed_persistence_does_not_consume_promise(plugin, payload):
    plugin.put_kv_data.side_effect = RuntimeError("storage unavailable")
    with pytest.raises(RuntimeError, match="storage unavailable"):
        await plugin.notify_groups(Forecast.from_payload(payload))
    assert plugin.alert_cursors.get(GROUP) is None
    plugin.put_kv_data.side_effect = None
    await plugin.notify_groups(Forecast.from_payload(payload))
    assert plugin.context.send_message.await_count == 2


@pytest.mark.parametrize("already_sent", [False, True])
@pytest.mark.asyncio
async def test_upgrade_and_reload_preserve_receipts(plugin, payload, already_sent):
    stored = {
        "known_groups": [GROUP],
        "cursors": dict(plugin.delivery_cursors),
    }
    if already_sent:
        stored["alerts"] = {GROUP: payload["alert_event_id"]}
    plugin.get_kv_data = AsyncMock(return_value=stored)
    plugin.config["enabled"] = False
    await plugin.initialize()
    try:
        plugin.config["enabled"] = True
        await plugin.notify_groups(Forecast.from_payload(payload))
        assert plugin.context.send_message.await_count == (0 if already_sent else 1)
    finally:
        await plugin.terminate()


@pytest.mark.parametrize(
    "mode,groups,enabled",
    [
        ("whitelist", [], True),
        ("blacklist", [GROUP], True),
        ("whitelist", [GROUP], False),
    ],
)
@pytest.mark.asyncio
async def test_policy_applies_to_promise_notifications(
    plugin, payload, mode, groups, enabled
):
    plugin.config.update(group_mode=mode, group_ids=groups, enabled=enabled)
    await plugin.notify_groups(Forecast.from_payload(payload))
    plugin.context.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_reenabling_group_does_not_replay_same_promise(plugin, payload):
    forecast = Forecast.from_payload(payload)
    await plugin.notify_groups(forecast)
    plugin.config["group_ids"] = []
    await plugin.refresh_groups()
    plugin.config["group_ids"] = [GROUP]
    await plugin.refresh_groups()
    await plugin.notify_groups(forecast)
    plugin.context.send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_later_promise_is_delivered(plugin, payload):
    await plugin.notify_groups(Forecast.from_payload(payload))
    payload["alert_event_id"] = "signal:new:likely"
    payload["latest_alert"]["source_at"] = "2026-09-22T06:00:00Z"
    await plugin.notify_groups(Forecast.from_payload(payload))
    assert plugin.context.send_message.await_count == 2
