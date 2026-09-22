import asyncio
import json
from contextlib import suppress
from datetime import timedelta, timezone
from time import monotonic

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star
from astrbot.core.platform.message_session import MessageSession

from .forecast import Forecast, parse_time


class CodexResetPlugin(Star):
    """Track public Codex resets and notify groups selected by one shared policy."""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.interval = max(60, int(config.get("poll_interval", 120)))
        offset = float(config.get("utc_offset", 8))
        if not -12 <= offset <= 14:
            raise ValueError("utc_offset must be between -12 and 14")
        self.tz = timezone(timedelta(hours=offset))
        if config.get("group_mode", "whitelist") not in {"whitelist", "blacklist"}:
            raise ValueError("group_mode must be whitelist or blacklist")
        groups = config.get("group_ids", [])
        if not isinstance(groups, list):
            raise ValueError("group_ids must be a list")
        normalized = []
        for group in groups:
            if not isinstance(group, (str, int)) or isinstance(group, bool):
                raise ValueError("Each group ID must be a string or integer")
            group = str(group).strip()
            if not group:
                continue
            if ":" in group:
                session = MessageSession.from_str(group)
                if (
                    session.message_type.value != "GroupMessage"
                    or not session.platform_id
                    or not session.session_id
                ):
                    raise ValueError(
                        "Only group sessions can receive reset notifications"
                    )
            if group not in normalized:
                normalized.append(group)
        config["group_ids"] = normalized
        self.known_groups: set[str] = set()
        self.delivery_cursors: dict[str, str | None] = {}
        self.alert_cursors: dict[str, str | None] = {}
        self.next_discovery = 0.0
        self.cached: Forecast | None = None
        self.cache_time = 0.0
        self.retry_after = 0.0
        self.last_error = ""
        self.fetch_lock = asyncio.Lock()
        self.state_lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None
        self.task: asyncio.Task | None = None

    async def initialize(self):
        """Restore delivery state and migrate the former subscription list once."""
        stored = await self.get_kv_data("delivery_state", None)
        migrating = stored is None
        if migrating:
            legacy = await self.get_kv_data("subscriptions", {})
            if not isinstance(legacy, dict):
                raise ValueError("Invalid stored Codex reset subscriptions")
            stored = {"known_groups": list(legacy), "cursors": legacy, "alerts": {}}
        if not isinstance(stored, dict):
            raise ValueError("Invalid stored Codex reset delivery state")
        stored.setdefault("alerts", {})
        if (
            not isinstance(stored.get("known_groups"), list)
            or not isinstance(stored.get("cursors"), dict)
            or not isinstance(stored.get("alerts"), dict)
        ):
            raise ValueError("Invalid stored Codex reset delivery state")
        for origin in [*stored["known_groups"], *stored["cursors"], *stored["alerts"]]:
            if not isinstance(origin, str):
                raise ValueError("Invalid stored group session")
            session = MessageSession.from_str(origin)
            if (
                session.message_type.value != "GroupMessage"
                or not session.platform_id
                or not session.session_id
            ):
                raise ValueError("Invalid stored group session")
        for cursor in stored["cursors"].values():
            if cursor is not None:
                parse_time(cursor)
        for alert_id in stored["alerts"].values():
            if alert_id is not None and (not isinstance(alert_id, str) or not alert_id):
                raise ValueError("Invalid stored Codex reset alert cursor")
        self.known_groups = set(stored["known_groups"]) | set(stored["cursors"])
        self.delivery_cursors = dict(stored["cursors"])
        self.alert_cursors = dict(stored["alerts"])
        if migrating:
            # An explicit new policy takes precedence over legacy subscriptions.
            if (
                self.config.get("group_mode", "whitelist") == "whitelist"
                and not self.config["group_ids"]
                and self.delivery_cursors
            ):
                self.config["group_ids"] = sorted(self.delivery_cursors)
                self.config.save_config()
            # This record also marks migration complete, even for an empty list.
            await self.save_delivery_state()
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={
                "User-Agent": "astrbot_plugin_codex_reset/1.2.1 (+https://github.com/Koishi0425/astrbot_plugin_codex_reset)",
                "Accept": "application/json",
            },
            trust_env=True,
        )
        if self.config.get("enabled", True):
            self.task = asyncio.create_task(self.poll(), name="codex-reset-poll")

    def is_group_enabled(self, origin: str) -> bool:
        """Apply the same notification policy in polling, commands, and status.

        Args:
            origin: Canonical group session, including the adapter instance ID.

        Returns:
            Whether the group is selected by the current list mode.
        """
        groups = self.config.get("group_ids", [])
        matched = origin in groups or origin.split(":", 2)[2] in groups
        mode = self.config.get("group_mode", "whitelist")
        return matched if mode == "whitelist" else mode == "blacklist" and not matched

    async def save_delivery_state(self):
        """Persist discovered addresses and per-event cursors without a second policy."""
        await self.put_kv_data(
            "delivery_state",
            {
                "known_groups": sorted(self.known_groups),
                "cursors": dict(self.delivery_cursors),
                "alerts": dict(self.alert_cursors),
            },
        )

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def remember_group(self, event: AstrMessageEvent):
        """Remember a group address without subscribing it or replying to messages.

        Args:
            event: A group message already admitted by AstrBot's event pipeline.
        """
        if not event.get_group_id():
            return
        origin = f"{event.get_platform_id()}:GroupMessage:{event.get_group_id()}"
        async with self.state_lock:
            if origin not in self.known_groups:
                self.known_groups.add(origin)
                try:
                    await self.save_delivery_state()
                except Exception:
                    self.known_groups.remove(origin)
                    raise

    async def refresh_groups(self):
        """Discover OneBot groups and derive delivery targets from configuration.

        OneBot supplies a membership list; other adapters learn addresses from
        incoming messages. Explicit whitelist sessions work without discovery.
        Discovery failure preserves known addresses for retry.
        """
        previous_known = set(self.known_groups)
        discovered = set(previous_known)
        groups = self.config.get("group_ids", [])
        mode = self.config.get("group_mode", "whitelist")
        needs_discovery = mode == "blacklist" or any(
            ":" not in group for group in groups
        )
        if needs_discovery and monotonic() >= self.next_discovery:
            self.next_discovery = monotonic() + 600
            found_onebot = False
            for platform in self.context.platform_manager.platform_insts:
                meta = platform.meta()
                if meta.name != "aiocqhttp":
                    continue
                found_onebot = True
                try:
                    entries = await asyncio.wait_for(
                        platform.get_client().call_action("get_group_list"), timeout=10
                    )
                    if not isinstance(entries, list) or any(
                        not isinstance(entry, dict)
                        or not isinstance(entry.get("group_id"), (str, int))
                        or isinstance(entry["group_id"], bool)
                        or not str(entry["group_id"]).strip()
                        for entry in entries
                    ):
                        raise ValueError("Invalid OneBot group list")
                    prefix = f"{meta.id}:GroupMessage:"
                    discovered = {
                        origin for origin in discovered if not origin.startswith(prefix)
                    }
                    discovered.update(
                        f"{prefix}{entry['group_id']}" for entry in entries
                    )
                except Exception as exc:
                    self.next_discovery = min(
                        self.next_discovery, monotonic() + self.interval
                    )
                    logger.warning(
                        "Codex reset group discovery failed for %s: %s", meta.id, exc
                    )
            if not found_onebot:
                self.next_discovery = monotonic() + self.interval
        async with self.state_lock:
            # Include addresses learned while the platform request was in flight.
            discovered.update(self.known_groups - previous_known)
            groups = self.config.get("group_ids", [])
            if self.config.get("group_mode", "whitelist") == "whitelist":
                discovered.update(
                    group for group in groups if ":GroupMessage:" in group
                )
            targets = {origin for origin in discovered if self.is_group_enabled(origin)}
            cursors = {
                origin: self.delivery_cursors.get(origin) for origin in sorted(targets)
            }
            if discovered != self.known_groups or cursors != self.delivery_cursors:
                previous_groups, previous_cursors = (
                    self.known_groups,
                    self.delivery_cursors,
                )
                self.known_groups, self.delivery_cursors = discovered, cursors
                try:
                    await self.save_delivery_state()
                except Exception:
                    self.known_groups, self.delivery_cursors = (
                        previous_groups,
                        previous_cursors,
                    )
                    raise

    async def fetch_forecast(self) -> Forecast:
        """Fetch and validate data, sharing requests across commands and polls.

        Returns:
            A recent upstream forecast.

        Raises:
            RuntimeError: The source is unavailable, stale, or malformed.
        """
        async with self.fetch_lock:
            now = monotonic()
            if self.cached and now - self.cache_time < 60 and self.cached.is_fresh():
                return self.cached
            if now < self.retry_after:
                raise RuntimeError(self.last_error)
            if self.session is None:
                raise RuntimeError("插件尚未初始化，请稍后重试。")
            try:
                async with self.session.get(
                    "https://codex-reset.com/api/forecast",
                    proxy=self.config.get("proxy") or None,
                ) as response:
                    if response.status == 403:
                        logger.info(
                            "Direct Codex reset request returned 403; using fallback."
                        )
                        async with self.session.get(
                            "https://r.jina.ai/http://https://codex-reset.com/api/forecast",
                            proxy=self.config.get("proxy") or None,
                            headers={"Accept": "text/plain"},
                        ) as fallback:
                            fallback.raise_for_status()
                            body = await fallback.text()
                        marker = "Markdown Content:\n"
                        _, separator, encoded = body.partition(marker)
                        if not separator:
                            raise ValueError(
                                "Fallback response is missing JSON content"
                            )
                        data = json.loads(encoded.strip())
                    else:
                        if response.status == 429:
                            delay = response.headers.get("Retry-After", "120")
                            try:
                                seconds = float(delay)
                            except ValueError:
                                seconds = 120
                            self.retry_after = monotonic() + max(
                                120, min(seconds, 3600)
                            )
                            raise RuntimeError("数据源请求过于频繁，正在等待后重试。")
                        response.raise_for_status()
                        data = await response.json()
                forecast = Forecast.from_payload(data)
                if not forecast.is_fresh():
                    raise RuntimeError("网站数据超过 30 分钟未更新，暂不用于重置通知。")
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                ValueError,
                RuntimeError,
            ) as exc:
                if isinstance(exc, RuntimeError):
                    self.last_error = str(exc)
                elif isinstance(exc, ValueError):
                    self.last_error = "网站数据格式异常，暂时无法获取重置信息。"
                else:
                    self.last_error = "无法连接数据源，请检查网络或代理后重试。"
                self.retry_after = max(self.retry_after, monotonic() + 60)
                logger.warning("Codex reset request failed: %s", exc)
                raise RuntimeError(self.last_error) from exc
            self.cached = forecast
            self.cache_time = monotonic()
            self.last_error = ""
            return forecast

    async def notify_groups(self, forecast: Forecast):
        """Deliver reset confirmations and live promises with separate receipts.

        Args:
            forecast: Validated source data. Stale snapshots are never delivered.
        """
        if not self.config.get("enabled", True) or not forecast.is_fresh():
            return
        reset_at = forecast.last_reset.isoformat() if forecast.last_reset else None
        watch = forecast.active_watch()
        async with self.state_lock:
            for origin, cursor in list(self.delivery_cursors.items()):
                if not self.is_group_enabled(origin):
                    continue
                pending = []
                if reset_at and cursor is None:
                    # Past resets establish a baseline; live promises remain useful.
                    self.delivery_cursors[origin] = reset_at
                    try:
                        await self.save_delivery_state()
                    except Exception:
                        self.delivery_cursors[origin] = cursor
                        raise
                elif reset_at and parse_time(cursor) < forecast.last_reset:
                    pending.append(
                        (
                            self.delivery_cursors,
                            reset_at,
                            "【Codex 额度重置通知】\n"
                            + forecast.render(self.tz, "last")
                            + "\n请在自己的 Codex 中确认额度到账。",
                        )
                    )
                if watch:
                    watch_id, alert = watch
                    if self.alert_cursors.get(origin) != watch_id and (
                        cursor is None
                        or parse_time(cursor) < parse_time(alert["source_at"])
                    ):
                        pending.append(
                            (
                                self.alert_cursors,
                                watch_id,
                                "【Codex 重置预告】\n"
                                + forecast.render(self.tz, "watch")
                                + "\n这是重置预告，网站尚未确认重置完成。",
                            )
                        )
                for receipts, event_id, message in pending:
                    try:
                        delivered = await asyncio.wait_for(
                            self.context.send_message(
                                origin, MessageChain().message(message)
                            ),
                            timeout=30,
                        )
                        if not delivered:
                            logger.warning(
                                "Codex reset platform unavailable: %s", origin
                            )
                            continue
                    except Exception as exc:
                        logger.warning(
                            "Codex reset delivery failed for %s: %s", origin, exc
                        )
                        continue
                    previous = receipts.get(origin)
                    receipts[origin] = event_id
                    # Failed groups and failed state writes retain their retry cursor.
                    try:
                        await self.save_delivery_state()
                    except Exception:
                        receipts[origin] = previous
                        raise

    async def poll(self):
        """Refresh configured targets and retry source and delivery errors."""
        while True:
            try:
                await self.refresh_groups()
                if self.delivery_cursors:
                    forecast = await self.fetch_forecast()
                    await self.notify_groups(forecast)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Codex reset polling failed: %s", exc)
            await asyncio.sleep(self.interval)

    @filter.command("codex", alias={"codex重置"})
    async def codex(self, event: AstrMessageEvent, action: str = "状态"):
        """Query public resets or edit the shared group notification policy.

        Args:
            event: Incoming AstrBot message event.
            action: Query or subscription action, in Chinese or English.

        Yields:
            A plain-text command response.
        """
        action = {
            "状态": "status",
            "上次": "last",
            "预测": "forecast",
            "订阅": "subscribe",
            "取消": "unsubscribe",
            "帮助": "help",
        }.get(action.lower(), action.lower())
        if action in {"subscribe", "unsubscribe"}:
            if not event.get_group_id():
                yield event.plain_result("请在需要接收通知的群聊中使用此指令。")
                return
            if not event.is_admin():
                yield event.plain_result("仅 AstrBot 管理员可以订阅或取消群通知。")
                return
            origin = f"{event.get_platform_id()}:GroupMessage:{event.get_group_id()}"
            group_id = str(event.get_group_id())
            async with self.state_lock:
                was_enabled = self.is_group_enabled(origin)
                previous = list(self.config.get("group_ids", []))
                groups = list(previous)
                mode = self.config.get("group_mode", "whitelist")
                add = (mode == "whitelist") == (action == "subscribe")
                if add:
                    if origin not in groups and group_id not in groups:
                        groups.append(origin)
                else:
                    groups = [
                        group for group in groups if group not in {origin, group_id}
                    ]
                self.config["group_ids"] = groups
                try:
                    self.config.save_config()
                except Exception as exc:
                    self.config["group_ids"] = previous
                    logger.error("Codex reset group policy save failed: %s", exc)
                    reply = "名单保存失败，请检查日志后重试；本群通知设置未更改。"
                else:
                    self.known_groups.add(origin)
                    # Only delivery cursors are separate from the WebUI policy.
                    if not self.is_group_enabled(origin):
                        self.delivery_cursors.pop(origin, None)
                    elif not was_enabled:
                        self.delivery_cursors[origin] = None
                    await self.save_delivery_state()
                    reply = (
                        "已启用本群 Codex 重置通知。"
                        if action == "subscribe"
                        else "已关闭本群 Codex 重置通知。"
                    )
                    label = "白名单" if mode == "whitelist" else "黑名单"
                    reply += f"\n{label}已同步到 WebUI；当前名单模式保持不变。"
                    if action == "subscribe" and not was_enabled:
                        reply += (
                            "\n历史重置仅建立基线；当前仍有效的重置预告会通知一次。"
                        )
                    if action == "subscribe" and not self.config.get("enabled", True):
                        reply += "\n当前自动通知已关闭，请在插件配置中启用并重载。"
            yield event.plain_result(reply)
            return
        if action not in {"status", "last", "forecast"}:
            yield event.plain_result(
                "Codex 公共重置追踪\n"
                "/codex 状态 — 上次重置与下次预测\n"
                "/codex 上次 — 上次重置时间\n"
                "/codex 预测 — 当前预告、时间窗口与 24/48 小时概率\n"
                "/codex 订阅 — 在当前名单模式下启用本群通知（管理员）\n"
                "/codex 取消 — 在当前名单模式下关闭本群通知（管理员）\n"
                "时间默认 UTC+08:00；跟踪公共全局重置。"
            )
            return
        warning = ""
        try:
            forecast = await self.fetch_forecast()
        except RuntimeError as exc:
            if self.cached is None:
                yield event.plain_result(str(exc))
                return
            forecast = self.cached
            warning = f"⚠ {exc}\n以下为旧缓存，预测可能已失效。\n"
        reply = warning + forecast.render(self.tz, action)
        if action == "status" and event.get_group_id():
            origin = f"{event.get_platform_id()}:GroupMessage:{event.get_group_id()}"
            allowed = self.is_group_enabled(origin)
            label = (
                "白名单"
                if self.config.get("group_mode", "whitelist") == "whitelist"
                else "黑名单"
            )
            reply += (
                "\n本群通知："
                + ("已启用" if allowed else "已关闭")
                + f"（{label}模式）"
            )
            if not self.config.get("enabled", True):
                reply += "\n自动通知总开关已关闭。"
        yield event.plain_result(reply)

    async def terminate(self):
        """Cancel polling before closing the shared HTTP session."""
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None
        if self.session:
            await self.session.close()
            self.session = None
