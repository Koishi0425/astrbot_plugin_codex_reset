import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def parse_time(value: str) -> datetime:
    """Parse an API timestamp without assuming the machine timezone.

    Args:
        value: ISO 8601 timestamp with an explicit UTC offset.

    Returns:
        The equivalent UTC datetime.

    Raises:
        ValueError: The timestamp is missing, malformed, or lacks a timezone.
    """
    if not isinstance(value, str):
        raise ValueError("Expected an ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


@dataclass
class Forecast:
    """A public forecast snapshot, separate from any personal quota."""

    updated_at: datetime
    last_reset: datetime | None
    payload: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: dict) -> "Forecast":
        """Validate timestamps and probabilities before changing state.

        Args:
            payload: JSON returned by the public forecast API.

        Returns:
            A snapshot for queries and reset comparison.

        Raises:
            ValueError: Required fields are absent or have invalid values.
        """
        if not isinstance(payload, dict) or "last_reset_at" not in payload:
            raise ValueError("Missing forecast fields")
        updated = parse_time(payload.get("updated_at"))
        last = payload["last_reset_at"]
        last = parse_time(last) if last is not None else None
        if last and last > updated:
            raise ValueError("Last reset is later than the snapshot")
        probabilities = payload.get("probabilities")
        if not isinstance(probabilities, dict):
            raise ValueError("Missing probabilities")
        for key in ("rounded_24h", "rounded_48h"):
            value = probabilities.get(key)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 <= value <= 100
            ):
                raise ValueError("Invalid probability")
        for key in ("latest_alert", "official_signal", "time_window", "cadence"):
            if payload.get(key) is not None and not isinstance(payload[key], dict):
                raise ValueError(f"Invalid {key}")
        return cls(updated, last, payload)

    def is_fresh(self) -> bool:
        """Check whether the source can be used for live notifications.

        Returns:
            True for snapshots no more than 30 minutes old or 5 minutes ahead.
        """
        age = (datetime.now(timezone.utc) - self.updated_at).total_seconds()
        return -300 <= age <= 1800 and not self.payload.get("stale", False)

    def active_watch(self) -> tuple[str, dict[str, Any]] | None:
        """Select a live reset promise without treating it as a completed reset.

        Returns:
            The upstream event ID and Watch details, or None for inactive,
            malformed, expired, or already fulfilled signals.
        """
        alert_id = self.payload.get("alert_event_id")
        alert = self.payload.get("latest_alert")
        if (
            not isinstance(alert_id, str)
            or not alert_id.strip()
            or not isinstance(alert, dict)
            or alert.get("kind") != "watch"
            or alert.get("state") != "active"
        ):
            return None
        score = alert.get("score")
        if (
            isinstance(score, bool)
            or not isinstance(score, (int, float))
            or score not in (83, 93)
        ):
            return None
        try:
            source_at = parse_time(alert.get("source_at"))
            if source_at > self.updated_at or (
                self.last_reset and source_at <= self.last_reset
            ):
                return None
            window = alert.get("window") or {}
            if not isinstance(window, dict):
                return None
            deadline = window.get("end_at") or window.get("target_at")
            if deadline and parse_time(deadline) <= datetime.now(timezone.utc):
                return None
        except ValueError:
            return None
        return alert_id, alert

    def render(self, tz: timezone, action: str = "status") -> str:
        """Render source facts and explicitly labeled timing estimates.

        Args:
            tz: Display timezone selected in plugin configuration.
            action: One of status, last, forecast, or watch (notification only).

        Returns:
            A Chinese response including source and calculation time.
        """
        data = self.payload
        lines = ["Codex 公共重置信息"]
        if action not in {"forecast", "watch"}:
            if self.last_reset:
                local = self.last_reset.astimezone(tz)
                elapsed = max(
                    0,
                    int((datetime.now(timezone.utc) - self.last_reset).total_seconds()),
                )
                days, hours = divmod(elapsed // 3600, 24)
                lines.extend(
                    [
                        f"上次重置：{local:%Y-%m-%d %H:%M:%S %Z}",
                        f"距今：{days} 天 {hours} 小时（网站记录的公告/确认时间）",
                    ]
                )
            else:
                lines.append("上次重置：网站暂无记录。")
            alert = data.get("latest_alert") or {}
            if alert.get("kind") == "reset" and alert.get("state") == "confirmed":
                try:
                    matches = parse_time(alert.get("source_at")) == self.last_reset
                except ValueError:
                    matches = False
                if matches and isinstance(alert.get("url"), str):
                    lines.append(f"公告：{alert['url']}")
        if action != "last":
            signal = data.get("official_signal") or {}
            watch = self.active_watch()
            if watch:
                _, alert = watch
                lines.append("当前状态：Tibo 已预告重置，等待确认。")
                lines.append(
                    f"网站预告信号评分：{alert['score']:g}%（不是 24 小时概率）"
                )
                summary = alert.get("summary")
                if isinstance(summary, str) and summary.strip():
                    lines.append(f"预告原文：{summary.strip()}")
                source_at = parse_time(alert["source_at"]).astimezone(tz)
                lines.append(f"预告发布：{source_at:%Y-%m-%d %H:%M:%S %Z}")
                url = alert.get("url")
                if isinstance(url, str) and url.startswith("https://"):
                    lines.append(f"推文：{url}")
                window = alert.get("window") or signal.get("window") or {}
            else:
                window = signal.get("window") or signal
            if action != "watch":
                probabilities = data["probabilities"]
                for hours in (24, 48):
                    value = probabilities.get(f"rounded_{hours}h")
                    display = f"{value:g}%" if value is not None else "暂无数据"
                    prefix = "历史模型：" if watch else ""
                    lines.append(
                        f"{prefix}网站计算时点起 {hours} 小时内重置概率：{display}"
                    )
                confidence = {"low": "低", "medium": "中", "high": "高"}.get(
                    data.get("confidence"), "未知"
                )
                lines.append(f"历史模型预测置信度：{confidence}")
            target = (
                (window.get("target_at") or window.get("end_at"))
                if isinstance(window, dict)
                else None
            )
            try:
                target_at = parse_time(target) if target else None
            except ValueError:
                target_at = None
            if target_at:
                lines.append(
                    f"公告窗口截止：{target_at.astimezone(tz):%Y-%m-%d %H:%M:%S %Z}"
                )
                lines.append("这是公告窗口边界，具体到账时间可能不同。")
                if target_at < datetime.now(timezone.utc):
                    lines.append("该窗口已经过去，尚不能据此认定已重置。")
            else:
                lines.append("下次确切时间：暂无可用的公告时间窗口。")
            if action != "watch":
                cadence = data.get("cadence") or {}
                median = cadence.get("recent_median_days")
                if (
                    self.last_reset
                    and isinstance(median, (int, float))
                    and not isinstance(median, bool)
                    and math.isfinite(median)
                    and 0 < median <= 365
                ):
                    estimate = self.last_reset + timedelta(days=median)
                    lines.append(
                        f"按近期中位间隔 {median:g} 天推算：{estimate.astimezone(tz):%Y-%m-%d %H:%M %Z}"
                    )
                    lines.append("此时间由插件按历史间隔推算，仅作参考。")
                    if estimate < datetime.now(timezone.utc):
                        lines.append("历史推算时间已过，不代表即将重置。")
                peak = data.get("time_window") or {}
                start, end = peak.get("start_hour"), peak.get("end_hour")
                if peak.get("timezone") == "UTC" and all(
                    isinstance(h, int) and not isinstance(h, bool) and 0 <= h <= 23
                    for h in (start, end)
                ):
                    anchor = datetime(2000, 1, 1, tzinfo=timezone.utc)
                    start_at = (anchor + timedelta(hours=start)).astimezone(tz)
                    end_at = (
                        anchor + timedelta(hours=end if end > start else end + 24)
                    ).astimezone(tz)
                    across = "次日 " if end_at.date() > start_at.date() else ""
                    lines.append(
                        f"历史常见时段：{start_at:%H:%M}–{across}{end_at:%H:%M %Z}"
                    )
            lines.append(
                "网站预测与信号评分不保证重置时间；个人额度请以 Codex 中显示为准。"
            )
        lines.append(f"网站更新：{self.updated_at.astimezone(tz):%Y-%m-%d %H:%M:%S %Z}")
        lines.append("来源：https://codex-reset.com/")
        return "\n".join(lines)
