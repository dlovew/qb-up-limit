"""Emby 流量与状态采集调度"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import config_manager
import emby_browse_upload_stats
import emby_traffic_db
import playback_record_store
import secrets_store
import traffic_db
import emby_playback_traffic
import emby_traffic_tick_audit
import emby_user_sync
from emby_client import EmbyClient
from emby_docker import DockerStatsClient
from emby_lucky import (
    LuckyClient,
    calc_conn_traffic_deltas,
    calc_ip_traffic_deltas,
    extract_wan_ip_cumulative_traffic,
    iter_wan_conn_statistics,
    sum_positive,
)
from emby_traffic_filter import (
    apply_wan_traffic_filter,
    is_lan_ip,
    is_wan_playback_session,
    m3_allocate_wan_pool,
    m3_session_docker_share_bps,
    scale_m3_wan_pool_bytes,
    session_stream_bps,
)
from scheduler import clamp_interval, ticks_per_full_collect

logger = logging.getLogger(__name__)


def _active_playback_sessions(sessions: list) -> list:
    return [
        s for s in (sessions or [])
        if isinstance(s, dict) and bool(s.get('is_playing')) and not bool(s.get('is_paused'))
    ]


def _has_confirmed_wan_playback(sessions: list) -> bool:
    return any(
        is_wan_playback_session(s)
        for s in _active_playback_sessions(sessions)
    )


def _has_ambiguous_playback_endpoint(sessions: list) -> bool:
    for session in _active_playback_sessions(sessions):
        if not (session.get('remote_endpoint') or '').strip():
            return True
    return False


def _has_confirmed_lan_only_playback(sessions: list) -> bool:
    active = _active_playback_sessions(sessions)
    if not active:
        return False
    for session in active:
        if not (session.get('remote_endpoint') or '').strip():
            return False
        if bool(session.get('is_remote')):
            return False
        if is_wan_playback_session(session):
            return False
    return True


def _estimate_lan_tick_upload_bytes(sessions: list, tick_seconds: float) -> int:
    elapsed = max(0.5, min(120.0, float(tick_seconds or 1.0)))
    lan_bps = sum(
        max(0, int(session_stream_bps(session) or 0))
        for session in _active_playback_sessions(sessions)
        if not is_wan_playback_session(session)
    )
    if lan_bps <= 0:
        return 0
    return max(0, int(lan_bps * elapsed / 8))


def _estimate_m3_lan_tick_upload_bytes(sessions: list, tick_seconds: float) -> int:
    elapsed = max(0.5, min(120.0, float(tick_seconds or 1.0)))
    lan_bps = sum(
        m3_session_docker_share_bps(session)
        for session in _active_playback_sessions(sessions)
        if not is_wan_playback_session(session)
    )
    if lan_bps <= 0:
        return 0
    return max(0, int(lan_bps * elapsed / 8))


def _wan_filter_surplus_bytes(live_raw_up: int, live_delta_up: int) -> int:
    return max(0, int(live_raw_up or 0) - max(0, int(live_delta_up or 0)))


_M1_WAN_EMA_WARMUP_TICKS = 3
_M1_WAN_ELEVATED_RATIO = 1.12
_BACKLOG_DRAIN_MIN_BYTES = 512 * 1024


def _backlog_drain_cap_bytes(live_raw_up: int, live_wan_pool_up: int) -> int:
    """单 tick 从 backlog 补录时，WAN 池口径的上限（避免一次性灌入造成中段突刺）。"""
    wan = max(0, int(live_wan_pool_up or 0))
    raw = max(0, int(live_raw_up or 0))
    base = wan if wan > 0 else raw
    return max(base, _BACKLOG_DRAIN_MIN_BYTES)


def _filter_wan_pool_bytes(
    client: EmbyClient,
    raw_bytes: int,
    sessions: list,
    *,
    mode_code: str,
    m3_scale: float,
    wan_only_enabled: bool,
    burst_window_seconds: float = 8.0,
    now_epoch: float = None,
    tick_seconds: float = 1.0,
    ratio_only: bool = False,
) -> int:
    """将 Docker 原始增量按当前会话切出 WAN 池（M3 含 scale 与新外网突发）。"""
    raw = max(0, int(raw_bytes or 0))
    if raw <= 0:
        return 0
    if not wan_only_enabled:
        return raw
    if mode_code == 'M3':
        return m3_allocate_wan_pool(
            raw,
            sessions or [],
            scale=m3_scale,
            burst_window_seconds=burst_window_seconds,
            now_epoch=now_epoch,
            tick_seconds=tick_seconds,
            ratio_only=ratio_only,
        )
    filtered, _ = apply_wan_traffic_filter(
        raw, 0, sessions or [],
        enabled=getattr(client, 'wan_traffic_only', True),
    )
    return max(0, int(filtered or 0))


def _debug_mode_from_sessions(sessions: list) -> tuple:
    active = _active_playback_sessions(sessions)
    if not active:
        return 'M0', '无播放 M0', 0, 0
    wan = [s for s in active if is_wan_playback_session(s)]
    lan = [s for s in active if not is_wan_playback_session(s)]
    wan_count = len(wan)
    lan_count = len(lan)
    if wan_count <= 0 and lan_count > 0:
        return 'M1', '仅局域网 M1', lan_count, wan_count
    if wan_count > 0 and lan_count <= 0:
        return 'M2', '仅外网 M2', lan_count, wan_count
    return 'M3', '局域网+外网 M3', lan_count, wan_count


def _build_debug_traffic_metrics(total_upload_bytes: int, sessions: list,
                                 wan_upload_bytes: int,
                                 lan_upload_bytes: int = 0,
                                 program_remainder_bytes: int = 0,
                                 mode_switch_pending_bytes: int = 0,
                                 mode_switch_replay_bytes: int = 0,
                                 mode_switch_replay_alloc_bytes: int = 0,
                                 mode_switch_replay_total_bytes: int = 0,
                                 mode_switch_replay_alloc_total_bytes: int = 0,
                                 wan_alloc_backlog_bytes: int = 0,
                                 wan_alloc_backlog_applied_bytes: int = 0,
                                 m1_wan_capture_bytes: int = 0,
                                 wan_only_enabled: bool = True) -> dict:
    total_up = max(0, int(total_upload_bytes or 0))
    wan_up = max(0, min(total_up, int(wan_upload_bytes or 0)))
    lan_up = max(0, min(total_up, int(lan_upload_bytes or 0)))
    mode_code, mode_label, lan_count, wan_count = _debug_mode_from_sessions(sessions)
    if wan_up + lan_up > total_up:
        overflow = wan_up + lan_up - total_up
        lan_up = max(0, lan_up - overflow)
        if wan_up + lan_up > total_up:
            wan_up = max(0, total_up - lan_up)

    remainder_max = max(0, total_up - wan_up - lan_up)
    remainder_in = max(0, int(program_remainder_bytes or 0))
    remainder = min(remainder_max, remainder_in) if remainder_in > 0 else remainder_max

    if mode_code == 'M0':
        wan_up = 0
        lan_up = 0
        pending = max(0, int(mode_switch_pending_bytes or 0))
        if pending > 0 and pending >= total_up:
            remainder = 0
        else:
            remainder = max(0, total_up - pending)
    elif mode_code == 'M1':
        wan_up = 0
        if wan_only_enabled:
            lan_up = total_up
            remainder = 0
        else:
            if lan_up <= 0:
                lan_up = total_up
            lan_up = min(total_up, lan_up)
            remainder = max(0, total_up - lan_up)
    elif mode_code == 'M2':
        lan_up = 0
        if wan_up <= 0:
            wan_up = total_up
        wan_up = min(total_up, wan_up)
        remainder = max(0, total_up - wan_up)
    else:
        assigned = wan_up + lan_up + remainder
        if assigned < total_up:
            lan_up += (total_up - assigned)

    return {
        'mode_code': mode_code,
        'mode_label': mode_label,
        'lan_session_count': lan_count,
        'wan_session_count': wan_count,
        'total_upload_bytes': total_up,
        'wan_upload_bytes': max(0, int(wan_up)),
        'lan_upload_bytes': max(0, int(lan_up)),
        'program_remainder_bytes': max(0, int(remainder)),
        'mode_switch_pending_bytes': max(0, int(mode_switch_pending_bytes or 0)),
        'mode_switch_replay_bytes': max(0, int(mode_switch_replay_bytes or 0)),
        'mode_switch_replay_alloc_bytes': max(0, int(mode_switch_replay_alloc_bytes or 0)),
        'mode_switch_replay_total_bytes': max(0, int(mode_switch_replay_total_bytes or 0)),
        'mode_switch_replay_alloc_total_bytes': max(
            0, int(mode_switch_replay_alloc_total_bytes or 0),
        ),
        'wan_alloc_backlog_bytes': max(0, int(wan_alloc_backlog_bytes or 0)),
        'wan_alloc_backlog_applied_bytes': max(
            0, int(wan_alloc_backlog_applied_bytes or 0),
        ),
        'm1_wan_capture_bytes': max(0, int(m1_wan_capture_bytes or 0)),
    }


def _online_since_from_prev(prev: dict, was_online: bool = None) -> str:
    if was_online is None:
        was_online = prev.get('is_online')
    if was_online:
        cached = prev.get('online_since')
        if cached:
            return cached
    return emby_traffic_db._now().strftime('%Y-%m-%d %H:%M:%S')


class EmbyInstanceWorker:
    def __init__(self, monitor: 'EmbyMonitor', name: str):
        self.monitor = monitor
        self.name = name
        self._thread: threading.Thread = None
        self._running = False
        self._wake = threading.Event()
        self._was_online = False
        self._baseline_tx = None
        self._baseline_rx = None
        self._light_ticks = 0
        self._last_sessions = []
        self._mode_switch_pending_upload_bytes = 0
        self._mode_switch_pending_since_mono = None
        self._mode_switch_replay_total_bytes = 0
        self._mode_switch_replay_alloc_total_bytes = 0
        self._wan_alloc_backlog_bytes = 0
        self._wan_assign_budget_ledger = 0
        self._wan_assign_assigned_ledger = 0
        self._wan_ledger_session_id = ''
        self._prev_mode_code = 'M0'
        self._m1_wan_surplus_ema = 0.0
        self._m1_wan_capture_bytes = 0
        self._m1_wan_surplus_peak = 0
        self._m1_wan_surplus_warmup_ticks = 0
        self._verify_cumulative = {}
        self._last_tick_audit = {}
        self._lucky_ip_baselines: Dict[str, Dict[str, int]] = {}
        self._lucky_conn_baselines: Dict[str, Dict[str, int]] = {}
        self._lucky_conn_rows_last: list = []
        self._lucky_conn_deltas_last: Dict[str, int] = {}
        self._lucky_ip_traffic_last: Dict[str, Dict[str, int]] = {}
        self._lucky_ip_deltas_last: Dict[str, int] = {}
        self._lucky_total_out = 0
        self._lucky_total_in = 0
        self._lucky_analysis_last: Optional[dict] = None
        self._wan_client_sessions_last: list = []
        self._live_upload_hydrated = False

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, name=f'emby-collector-{self.name}', daemon=True,
        )
        self._thread.start()

    def stop(self, wait: bool = True):
        self._running = False
        self._wake.set()
        if self._thread:
            if wait:
                self._thread.join(timeout=15)
            self._thread = None

    def wake(self):
        self._wake.set()

    def _sleep_interval(self, tick_elapsed_seconds: float = 0.0):
        interval = clamp_interval(self.monitor.refresh_interval)
        wait_seconds = max(0.0, float(interval) - max(0.0, float(tick_elapsed_seconds or 0.0)))
        self._wake.wait(timeout=wait_seconds)
        self._wake.clear()

    def _ticks_per_full(self) -> int:
        return ticks_per_full_collect(
            self.monitor.collect_interval, self.monitor.refresh_interval)

    def _should_run_full_tick(self) -> bool:
        return self._light_ticks <= 0 or self._light_ticks >= self._ticks_per_full()

    def _get_client(self) -> Optional[EmbyClient]:
        with self.monitor._config_lock:
            return self.monitor.clients.get(self.name)

    def _container_ref(self, client: EmbyClient) -> str:
        return DockerStatsClient.resolve_container_ref(
            client.container_name, client.container_id)

    def _fetch_docker_stats(self, client: EmbyClient) -> Optional[dict]:
        ref = self._container_ref(client)
        if not ref:
            return None
        return self.monitor.docker.get_container_stats(ref)

    def _lucky_client(self, client: EmbyClient) -> Optional[LuckyClient]:
        token = secrets_store.get_lucky_open_token(self.name)
        if not token or not client.lucky_base_url:
            return None
        return LuckyClient(
            client.lucky_base_url,
            open_token=token,
            verify_ssl=client.lucky_verify_ssl,
        )

    def _lucky_wan_totals(self, res_list: list) -> tuple:
        total_out = total_in = 0
        for item in res_list or []:
            if not isinstance(item, dict):
                continue
            ip = str(item.get('IP') or '').strip()
            if not ip or is_lan_ip(ip):
                continue
            total_out += max(0, int(item.get('TrafficOut') or 0))
            total_in += max(0, int(item.get('TrafficIn') or 0))
        return total_out, total_in

    def _fetch_lucky_access_detail(self, client: EmbyClient) -> tuple:
        lc = self._lucky_client(client)
        if not lc or not client.lucky_rule_key or not client.lucky_sub_key:
            return None, False
        data, err = lc.fetch_access_detail(
            client.lucky_rule_key,
            client.lucky_sub_key,
        )
        if err or not data:
            return None, False
        return data, True

    def _fetch_sessions(self, client: EmbyClient) -> tuple:
        """返回 (sessions, ok)；拉取失败时 ok=False，调用方应保留分摊状态。"""
        try:
            return client.get_all_client_sessions() or [], True
        except Exception as e:
            logger.debug(f'[Emby:{self.name}] 获取会话失败: {e}')
            return None, False

    def _probe_api_online(self, client: EmbyClient) -> bool:
        return client.is_reachable()

    def _should_hold_mode_switch_pending(self, mode_code: str, current_upload_bytes: int,
                                         filtered_upload_bytes: int, *,
                                         api_online: bool,
                                         wan_only_enabled: bool,
                                         sessions: list) -> bool:
        if not api_online:
            return False
        current = max(0, int(current_upload_bytes or 0))
        if current <= 0 and int(self._mode_switch_pending_upload_bytes or 0) <= 0:
            return False
        grace_seconds = max(0, int(getattr(
            self.monitor, 'mode_switch_grace_seconds', 0,
        ) or 0))
        if grace_seconds <= 0:
            return False
        if mode_code == 'M0':
            return True
        if not wan_only_enabled:
            return False
        filtered = max(0, int(filtered_upload_bytes or 0))
        if filtered > 0:
            return False
        if mode_code == 'M1' and _has_confirmed_lan_only_playback(sessions):
            return False
        active = _active_playback_sessions(sessions)
        if not active:
            return False
        if _has_confirmed_wan_playback(sessions):
            return False
        if int(self._mode_switch_pending_upload_bytes or 0) > 0:
            return True
        return _has_ambiguous_playback_endpoint(sessions)

    def _reset_mode_switch_observation(self):
        self._mode_switch_replay_total_bytes = 0
        self._mode_switch_replay_alloc_total_bytes = 0

    def _reset_wan_assign_ledger(self):
        self._wan_assign_budget_ledger = 0
        self._wan_assign_assigned_ledger = 0
        self._wan_ledger_session_id = ''

    def _reset_m1_wan_capture(self):
        self._m1_wan_surplus_ema = 0.0
        self._m1_wan_capture_bytes = 0
        self._m1_wan_surplus_peak = 0
        self._m1_wan_surplus_warmup_ticks = 0

    def _m1_wan_elevated_bytes(self, surplus: int) -> int:
        ema = max(1.0, float(self._m1_wan_surplus_ema or 0.0))
        margin = max(256 * 1024, int(ema * 0.12))
        if int(surplus) < int(ema * _M1_WAN_ELEVATED_RATIO):
            return 0
        return max(0, int(surplus) - int(ema) - margin)

    def _flush_m1_wan_capture_to_backlog(self) -> int:
        captured = max(0, int(self._m1_wan_capture_bytes or 0))
        if captured > 0:
            self._wan_alloc_backlog_bytes += captured
        self._m1_wan_capture_bytes = 0
        self._m1_wan_surplus_peak = 0
        self._m1_wan_surplus_warmup_ticks = 0
        return captured

    def _flush_m1_wan_peak_fallback_to_backlog(self) -> int:
        peak = max(0, int(self._m1_wan_surplus_peak or 0))
        ema_baseline = max(1, int(self._m1_wan_surplus_ema or 0))
        margin = max(256 * 1024, int(ema_baseline * 0.12))
        fallback = max(0, peak - ema_baseline - margin)
        if fallback > 0:
            self._wan_alloc_backlog_bytes += fallback
        return fallback

    def _sync_wan_ledger_session(self, sessions: list) -> None:
        sid = ''
        for session in _active_playback_sessions(sessions or []):
            if not is_wan_playback_session(session):
                continue
            sid = str(
                session.get('emby_session_id')
                or session.get('session_id')
                or session.get('id')
                or '',
            ).strip()
            if sid:
                break
        if sid and sid != self._wan_ledger_session_id:
            self._reset_wan_assign_ledger()
            self._wan_ledger_session_id = sid

    def _plan_m3_wan_alloc_budget(
        self,
        client: EmbyClient,
        *,
        live_raw_up: int,
        mode_code: str,
        sessions: list,
        wan_only_enabled: bool,
        burst_window_seconds: float,
        now_epoch: float,
        tick_seconds: float,
    ) -> tuple:
        """单 tick WAN 分摊预算：live 可含突发；backlog 仅按比例切分避免补录放大。"""
        live_raw = max(0, int(live_raw_up or 0))
        raw_before = max(0, int(self._wan_alloc_backlog_bytes or 0))
        m3_scale = getattr(self.monitor, 'm3_wan_pool_scale', 1.0)
        filt_kw = dict(
            mode_code=mode_code,
            m3_scale=m3_scale,
            wan_only_enabled=wan_only_enabled,
            burst_window_seconds=burst_window_seconds,
            now_epoch=now_epoch,
            tick_seconds=tick_seconds,
        )

        raw_drain = 0
        if raw_before > 0:
            if live_raw > 0:
                soft_cap = min(_BACKLOG_DRAIN_MIN_BYTES, raw_before)
                raw_drain = min(raw_before, max(live_raw, soft_cap))
            else:
                raw_drain = min(raw_before, _BACKLOG_DRAIN_MIN_BYTES)

        wan_live = _filter_wan_pool_bytes(
            client,
            live_raw,
            sessions,
            **filt_kw,
        ) if live_raw > 0 else 0
        wan_drain = 0
        if raw_drain > 0:
            wan_drain = _filter_wan_pool_bytes(
                client,
                raw_drain,
                sessions,
                ratio_only=True,
                **filt_kw,
            )
        wan_from_docker = min(
            live_raw + raw_drain,
            max(0, int(wan_live or 0)) + max(0, int(wan_drain or 0)),
        )
        wan_backlog_part = max(0, int(wan_drain or 0))
        raw_after = raw_before - raw_drain
        return wan_from_docker, raw_drain, wan_live, wan_backlog_part, raw_after

    def _cap_wan_assign_by_ledger(
        self,
        wan_from_docker: int,
        *,
        wan_live: int,
        wan_backlog_part: int,
    ) -> int:
        """分摊账本：预算 += 本 tick filter 结果；实际灌入 ≤ 预算 − 已灌入。"""
        wan_from_docker = max(0, int(wan_from_docker or 0))
        wan_live = max(0, int(wan_live or 0))
        wan_backlog_part = max(0, int(wan_backlog_part or 0))
        if wan_from_docker <= 0:
            return 0
        self._wan_assign_budget_ledger += wan_live + wan_backlog_part
        allowed = max(
            0,
            int(self._wan_assign_budget_ledger or 0) - int(self._wan_assign_assigned_ledger or 0),
        )
        return min(wan_from_docker, allowed)

    def _accumulate_wan_filter_surplus_backlog(self, *, live_raw_up: int, live_delta_up: int,
                                               mode_code: str, prev_mode_code: str,
                                               sessions: list, tick_seconds: float,
                                               is_backfill: bool, wan_only_enabled: bool,
                                               grace_seconds: int, now_mono: float) -> None:
        """WAN 过滤未覆盖增量：M1 捕获窗口暂存，M3 入列后一次性补录。"""
        if not wan_only_enabled or is_backfill:
            return
        surplus = _wan_filter_surplus_bytes(live_raw_up, live_delta_up)

        if mode_code == 'M0' and not _active_playback_sessions(sessions):
            self._reset_m1_wan_capture()
            if surplus > 0 and grace_seconds <= 0:
                self._wan_alloc_backlog_bytes += surplus
            return

        if (
            mode_code == 'M1'
            and _has_confirmed_lan_only_playback(sessions)
            and not _has_confirmed_wan_playback(sessions)
        ):
            if surplus <= 0:
                return

            if self._m1_wan_surplus_ema <= 0:
                self._m1_wan_surplus_ema = float(surplus)
                self._m1_wan_surplus_peak = max(0, int(surplus))
                self._m1_wan_surplus_warmup_ticks = 1
                return

            self._m1_wan_surplus_ema = (
                0.12 * float(surplus) + 0.88 * self._m1_wan_surplus_ema
            )
            self._m1_wan_surplus_peak = max(
                int(self._m1_wan_surplus_peak or 0), int(surplus),
            )
            self._m1_wan_surplus_warmup_ticks = min(
                _M1_WAN_EMA_WARMUP_TICKS,
                int(self._m1_wan_surplus_warmup_ticks or 0) + 1,
            )

            if self._m1_wan_surplus_warmup_ticks >= _M1_WAN_EMA_WARMUP_TICKS:
                elevated = self._m1_wan_elevated_bytes(surplus)
                if elevated > 0:
                    self._m1_wan_capture_bytes += elevated
            return

        if mode_code in ('M2', 'M3') and _has_confirmed_wan_playback(sessions):
            if int(self._m1_wan_capture_bytes or 0) > 0:
                self._flush_m1_wan_capture_to_backlog()
            elif prev_mode_code == 'M1':
                self._flush_m1_wan_peak_fallback_to_backlog()
            self._m1_wan_surplus_peak = 0
            if mode_code == 'M3' and surplus > 0:
                lan_expected = _estimate_m3_lan_tick_upload_bytes(
                    sessions, tick_seconds,
                )
                spill = max(0, int(surplus) - lan_expected)
                if spill > 0:
                    self._wan_alloc_backlog_bytes += spill
            return

        if mode_code == 'M0' and surplus > 0 and grace_seconds <= 0:
            self._wan_alloc_backlog_bytes += surplus

    def _resolve_mode_switch_debug_total(self, current_upload_bytes: int,
                                         filtered_upload_bytes: int,
                                         mode_code: str, *,
                                         api_online: bool,
                                         wan_only_enabled: bool,
                                         sessions: list,
                                         now_mono: float) -> tuple:
        current = max(0, int(current_upload_bytes or 0))
        filtered = max(0, int(filtered_upload_bytes or 0))
        pending = max(0, int(self._mode_switch_pending_upload_bytes or 0))
        since = self._mode_switch_pending_since_mono
        grace_seconds = max(0, int(getattr(
            self.monitor, 'mode_switch_grace_seconds', 0,
        ) or 0))
        pending_timeout_seconds = max(10.0, float(grace_seconds) + 2.0)

        if grace_seconds <= 0:
            replay = pending if mode_code != 'M0' else 0
            merged = current + pending
            self._mode_switch_pending_upload_bytes = 0
            self._mode_switch_pending_since_mono = None
            if replay > 0:
                self._mode_switch_replay_total_bytes += replay
            return merged, replay

        if not api_online:
            merged = current + pending
            self._mode_switch_pending_upload_bytes = 0
            self._mode_switch_pending_since_mono = None
            return merged, 0

        hold = self._should_hold_mode_switch_pending(
            mode_code,
            current,
            filtered,
            api_online=api_online,
            wan_only_enabled=wan_only_enabled,
            sessions=sessions,
        )

        if hold and current > 0:
            if since is None:
                since = now_mono
                self._mode_switch_pending_since_mono = since
            pending += current
            self._mode_switch_pending_upload_bytes = pending

        if pending > 0:
            if since is None:
                since = now_mono
                self._mode_switch_pending_since_mono = since
            elapsed = now_mono - float(since)
            if not hold and mode_code != 'M0':
                replay = pending
                merged = current + pending
                self._mode_switch_pending_upload_bytes = 0
                self._mode_switch_pending_since_mono = None
                self._mode_switch_replay_total_bytes += replay
                if (
                    replay > 0
                    and wan_only_enabled
                    and mode_code in ('M2', 'M3')
                ):
                    self._wan_alloc_backlog_bytes += replay
                return merged, replay
            if elapsed <= float(grace_seconds):
                return 0, 0
            # 超过缓冲窗口后继续保留待判定流量，避免 API 慢 1-2 秒导致首段流量丢失。
            if elapsed <= pending_timeout_seconds:
                return max(0, current), 0
            # 长时间仍无法判定模式，回退为程序余量；挂起部分转入分摊 backlog 等待会话就绪。
            fallback_pending = pending
            self._mode_switch_pending_upload_bytes = 0
            self._mode_switch_pending_since_mono = None
            if fallback_pending > 0:
                self._wan_alloc_backlog_bytes += fallback_pending
            return fallback_pending + max(0, current), 0

        if mode_code == 'M0' and current <= 0:
            if not _active_playback_sessions(sessions):
                self._reset_mode_switch_observation()
                if int(self._mode_switch_pending_upload_bytes or 0) <= 0:
                    self._wan_alloc_backlog_bytes = 0
                    self._reset_wan_assign_ledger()
                    self._reset_m1_wan_capture()
        self._mode_switch_pending_upload_bytes = 0
        self._mode_switch_pending_since_mono = None
        return max(0, current), 0

    def _loop(self):
        while self._running and self.monitor._running:
            tick_started = time.monotonic()
            try:
                if self._should_run_full_tick():
                    self._tick(full=True)
                    self._light_ticks = 0
                else:
                    self._tick(full=False)
                self._light_ticks += 1
            except Exception as e:
                logger.error(f'[Emby:{self.name}] 采集循环异常: {e}', exc_info=True)
            tick_elapsed = time.monotonic() - tick_started
            self._sleep_interval(tick_elapsed)

    def _apply_traffic_filter(self, client: EmbyClient, delta_up: int, delta_dl: int,
                              sessions: list) -> tuple:
        return apply_wan_traffic_filter(
            delta_up, delta_dl, sessions or [],
            enabled=getattr(client, 'wan_traffic_only', True),
        )

    def _sync_playback_sessions(self, client: EmbyClient, *, api_online: bool) -> list:
        """拉取并同步播放会话；API 返回空列表表示无播放（如 Web 返回键退出）。"""
        if not api_online:
            return []
        fetched_sessions, sessions_fetch_ok = self._fetch_sessions(client)
        if not sessions_fetch_ok:
            logger.debug(
                f'[Emby:{self.name}] 会话拉取失败，保留 WAN 分摊状态',
            )
            return list(self._last_sessions or [])
        fetched_sessions = fetched_sessions or []
        estimate_upload_enabled = bool(
            getattr(client, 'upload_tracking_enabled', False),
        )
        traffic_collect_mode = str(
            getattr(client, 'traffic_collect_mode', '') or '',
        ).strip().lower()
        sessions = [
            {
                **s,
                'estimate_upload_enabled': estimate_upload_enabled,
                'traffic_collect_mode': traffic_collect_mode,
            }
            if isinstance(s, dict) else s
            for s in fetched_sessions
        ]
        store_snapshot = None
        with self.monitor._config_lock:
            inst_cfg = config_manager.get_emby_instance(
                self.name, self.monitor.config,
            )
        if inst_cfg:
            credit_browse = bool(
                inst_cfg.get('lucky_credit_browse_traffic', False),
            ) and traffic_collect_mode == 'lucky'
            try:
                _, store_snapshot = playback_record_store.tick_from_sessions(
                    self.name, sessions, api_online=api_online,
                    return_store=True,
                )
            except Exception as e:
                logger.debug(
                    f'[Emby:{self.name}] 播放段记录更新失败: {e}',
                )
        sessions = playback_record_store.enrich_sessions_playback_started_at(
            self.name, sessions, store=store_snapshot,
        )
        try:
            emby_playback_traffic.purge_stopped_wan_live_upload_state(
                self.name, sessions,
            )
        except Exception as e:
            logger.debug(
                f'[Emby:{self.name}] 停止会话分摊状态清理失败: {e}',
            )
        if not fetched_sessions:
            self._wan_alloc_backlog_bytes = 0
            self._reset_wan_assign_ledger()
            self._reset_m1_wan_capture()
            self._verify_cumulative = {}
            self._last_tick_audit = {}
            self._mode_switch_pending_upload_bytes = 0
            self._mode_switch_pending_since_mono = None
        self._wan_client_sessions_last = [
            dict(s) for s in sessions
            if isinstance(s, dict) and s.get('is_remote')
        ]
        from emby_client import EmbyClient
        open_sessions = [
            s for s in sessions
            if isinstance(s, dict) and EmbyClient.is_current_playback_session(s)
        ]
        self._last_sessions = open_sessions
        return open_sessions

    def _tick(self, full: bool):
        client = self._get_client()
        if not client:
            return

        collect_mode = str(getattr(client, 'traffic_collect_mode', '') or '').strip().lower()
        if collect_mode not in ('docker', 'lucky'):
            collect_mode = ''

        was_online = self._was_online
        api_online = self._probe_api_online(client)
        has_container = bool(self._container_ref(client))
        docker_stats = (
            self._fetch_docker_stats(client)
            if collect_mode == 'docker' and has_container else None
        )
        sessions = self._sync_playback_sessions(client, api_online=api_online)

        credit_browse = (
            collect_mode == 'lucky'
            and bool(getattr(client, 'lucky_credit_browse_traffic', False))
        )
        if not self._live_upload_hydrated and api_online:
            try:
                emby_playback_traffic.hydrate_live_upload_state(
                    self.name,
                    list(self._wan_client_sessions_last or sessions or []),
                    credit_browse=credit_browse,
                )
            except Exception as e:
                logger.debug(
                    f'[Emby:{self.name}] 会话流量续传恢复失败: {e}',
                )
            self._live_upload_hydrated = True

        docker_available = self.monitor.docker.is_available()
        lucky_available = False
        lucky_ip_deltas: Dict[str, int] = {}
        lucky_ip_traffic: Dict[str, Dict[str, int]] = {}
        is_online = api_online
        if collect_mode == 'docker':
            is_online = api_online or (docker_stats is not None)
        recovering = not was_online and is_online
        is_backfill = False
        backfill_up = backfill_dl = 0
        raw_up = raw_dl = 0
        live_raw_up = live_raw_dl = 0
        delta_up = delta_dl = 0
        live_delta_up = live_delta_dl = 0
        allocation_debug = {
            'total_upload_bytes': 0,
            'wan_upload_bytes': 0,
            'lan_upload_bytes': 0,
            'wan_pool_bytes': 0,
            'assigned_bytes': 0,
            'remainder_bytes': 0,
            'program_remainder_bytes': 0,
            'target_session_count': 0,
        }

        if collect_mode == 'docker' and docker_stats and has_container:
            tx = docker_stats['tx_bytes']
            rx = docker_stats['rx_bytes']
            raw_up = raw_dl = 0
            if self._baseline_tx is not None and self._baseline_rx is not None:
                raw_up = max(0, tx - self._baseline_tx) if tx >= self._baseline_tx else 0
                raw_dl = max(0, rx - self._baseline_rx) if rx >= self._baseline_rx else 0
            filt_up, filt_dl = self._apply_traffic_filter(
                client, raw_up, raw_dl, sessions)
            live_raw_up = raw_up
            live_raw_dl = raw_dl
            live_delta_up = filt_up
            live_delta_dl = filt_dl
            is_backfill = bool(
                full
                and recovering
                and emby_traffic_db.has_docker_baseline(self.name)
                and (raw_up > 0 or raw_dl > 0)
            )
            if self._baseline_tx is not None:
                _, _, backfill_up, backfill_dl = emby_traffic_db.save_snapshot(
                    self.name,
                    tx,
                    rx,
                    record_up=filt_up,
                    record_down=filt_dl,
                    is_backfill=is_backfill,
                )
            else:
                emby_traffic_db.save_snapshot(
                    self.name,
                    tx,
                    rx,
                    record_up=0,
                    record_down=0,
                    is_backfill=False,
                )
                backfill_up = backfill_dl = 0
            if is_backfill and (backfill_up > 0 or backfill_dl > 0):
                logger.info(
                    f'[Emby:{self.name}] 离线恢复补录上行='
                    f'{backfill_up / 1024 / 1024:.2f}MB'
                    f'/下行={backfill_dl / 1024 / 1024:.2f}MB'
                )
            self._baseline_tx = tx
            self._baseline_rx = rx
        elif collect_mode == 'lucky':
            lucky_detail = None
            if full or self._was_online:
                lucky_detail, lucky_available = self._fetch_lucky_access_detail(client)
            if lucky_detail:
                is_online = api_online or lucky_available
                res_list = lucky_detail.get('resList') or []
                lucky_baselines = emby_traffic_db.load_lucky_ip_baselines(self.name)
                out_deltas, in_deltas, new_baselines = calc_ip_traffic_deltas(
                    res_list,
                    lucky_baselines,
                    wan_only=True,
                )
                emby_traffic_db.save_lucky_ip_baselines(self.name, new_baselines)
                self._lucky_ip_baselines = new_baselines
                conn_baselines = emby_traffic_db.load_lucky_conn_baselines(self.name)
                conn_out_deltas, conn_in_deltas, new_conn_baselines = (
                    calc_conn_traffic_deltas(
                        res_list,
                        conn_baselines,
                        wan_only=True,
                    )
                )
                emby_traffic_db.save_lucky_conn_baselines(
                    self.name, new_conn_baselines,
                )
                self._lucky_conn_baselines = new_conn_baselines
                self._lucky_conn_rows_last = iter_wan_conn_statistics(
                    res_list, wan_only=True,
                )
                self._lucky_conn_deltas_last = dict(conn_out_deltas)
                lucky_ip_traffic = extract_wan_ip_cumulative_traffic(res_list)
                self._lucky_ip_traffic_last = lucky_ip_traffic
                self._lucky_ip_deltas_last = dict(out_deltas)
                lucky_ip_deltas = out_deltas
                raw_up = sum_positive(out_deltas)
                raw_dl = sum_positive(in_deltas)
                live_raw_up = raw_up
                live_raw_dl = raw_dl
                live_delta_up = raw_up
                live_delta_dl = raw_dl
                last_tx, last_rx = emby_traffic_db.get_instance_last_totals(self.name)
                is_backfill = (
                    full
                    and recovering
                    and emby_traffic_db.has_docker_baseline(self.name)
                )
                _, _, backfill_up, backfill_dl = emby_traffic_db.save_snapshot(
                    self.name,
                    last_tx + raw_up,
                    last_rx + raw_dl,
                    record_up=raw_up,
                    record_down=raw_dl,
                    is_backfill=is_backfill,
                )
                if is_backfill and (backfill_up > 0 or backfill_dl > 0):
                    logger.info(
                        f'[Emby:{self.name}] Lucky 离线恢复补录上行='
                        f'{backfill_up / 1024 / 1024:.2f}MB'
                        f'/下行={backfill_dl / 1024 / 1024:.2f}MB'
                    )
                total_out, total_in = self._lucky_wan_totals(res_list)
                self._lucky_total_out = total_out
                self._lucky_total_in = total_in
            else:
                if api_online:
                    is_online = api_online
                lucky_ip_traffic = dict(self._lucky_ip_traffic_last or {})
                lucky_ip_deltas = dict(self._lucky_ip_deltas_last or {})

        mode_sessions = sessions
        wan_alloc_sessions = (
            list(getattr(self, '_wan_client_sessions_last', None) or [])
            if credit_browse else mode_sessions
        )

        if not is_online and self._was_online:
            logger.warning(f'[Emby:{self.name}] 连接中断，进入离线探测模式')

        emby_traffic_db.update_instance_status(
            self.name,
            is_online=is_online,
            api_online=api_online,
            docker_available=docker_available and has_container,
        )

        now_mono = time.monotonic()
        now_epoch = time.time()
        mode_code, _, _, _ = _debug_mode_from_sessions(mode_sessions)
        wan_only_enabled = bool(getattr(client, 'wan_traffic_only', True))
        burst_window_seconds = float(
            getattr(self.monitor, 'burst_new_session_window_seconds', 8) or 8,
        )
        allocation_tick_seconds = max(1, int(self.monitor.refresh_interval or 1))
        if (
            collect_mode != 'lucky'
            and full
            and raw_up > 0
            and live_raw_up == raw_up
        ):
            allocation_tick_seconds = max(1, int(self.monitor.collect_interval or 1))
        if collect_mode == 'lucky':
            conn_deltas = dict(getattr(self, '_lucky_conn_deltas_last', None) or {})
            conn_rows = list(getattr(self, '_lucky_conn_rows_last', None) or [])
            alloc_deltas = conn_deltas if conn_deltas else lucky_ip_deltas
            if credit_browse:
                try:
                    import emby_continuous_playback
                    emby_continuous_playback.tick(
                        self.name,
                        list(self._wan_client_sessions_last or []),
                    )
                except Exception as e:
                    logger.debug(
                        f'[Emby:{self.name}] 连播上下文更新失败: {e}',
                    )
            # 边界顺序保障：playback_record_store.tick_from_sessions（上文，
            # 会在换集/结案时先把累加器结转进旧分段）必须在下方 Lucky 分摊
            # 之前执行，这样换集 tick 的新增量只会计入新分段，旧分段已完整
            # 结转，边界误差被限制在至多一个 tick 内。
            if sessions is not None and live_delta_up > 0 and alloc_deltas:
                try:
                    if conn_deltas and conn_rows:
                        part = emby_playback_traffic.accumulate_wan_upload_by_conn(
                            self.name,
                            wan_alloc_sessions,
                            conn_deltas,
                            conn_rows,
                            tick_seconds=allocation_tick_seconds,
                            credit_browse=credit_browse,
                            ip_deltas=lucky_ip_deltas,
                        )
                    else:
                        part = emby_playback_traffic.accumulate_wan_upload_by_ip(
                            self.name,
                            mode_sessions,
                            lucky_ip_deltas,
                            tick_seconds=allocation_tick_seconds,
                        )
                    if isinstance(part, dict):
                        allocation_debug.update(part)
                except Exception as e:
                    logger.debug(
                        f'[Emby:{self.name}] Lucky 上行分摊失败: {e}',
                    )
            if credit_browse:
                lucky_analysis = None
                try:
                    lucky_analysis = emby_playback_traffic.get_lucky_conn_debug_snapshot(
                        self.name,
                        wan_alloc_sessions,
                        conn_rows,
                        alloc_deltas or {},
                        credit_browse=True,
                    )
                except Exception as e:
                    logger.debug(
                        f'[Emby:{self.name}] Lucky 选片结算快照失败: {e}',
                    )
                if isinstance(lucky_analysis, dict):
                    self._lucky_analysis_last = lucky_analysis
                elif self._lucky_analysis_last is not None:
                    lucky_analysis = self._lucky_analysis_last
                try:
                    import browse_upload_settler
                    browse_upload_settler.tick(
                        self.name,
                        list(self._wan_client_sessions_last or []),
                        api_online=api_online,
                        credit_enabled=True,
                        analysis=lucky_analysis,
                        min_upload_bytes=getattr(
                            self.monitor, 'browse_upload_min_bytes', None,
                        ),
                    )
                except Exception as e:
                    logger.warning(
                        f'[Emby:{self.name}] 选片流量结算失败: {e}',
                        exc_info=True,
                    )
        elif collect_mode == 'docker' and mode_code == 'M3' and wan_only_enabled:
            live_delta_up = m3_allocate_wan_pool(
                live_raw_up,
                mode_sessions,
                scale=getattr(self.monitor, 'm3_wan_pool_scale', 1.0),
                burst_window_seconds=burst_window_seconds,
                now_epoch=now_epoch,
                tick_seconds=allocation_tick_seconds,
            )
        grace_seconds = max(0, int(getattr(
            self.monitor, 'mode_switch_grace_seconds', 0,
        ) or 0))
        if collect_mode == 'docker':
            self._accumulate_wan_filter_surplus_backlog(
                live_raw_up=live_raw_up,
                live_delta_up=live_delta_up,
                mode_code=mode_code,
                prev_mode_code=self._prev_mode_code,
                sessions=mode_sessions,
                tick_seconds=allocation_tick_seconds,
                is_backfill=is_backfill,
                wan_only_enabled=wan_only_enabled,
                grace_seconds=grace_seconds,
                now_mono=now_mono,
            )
            debug_total_up, replay_upload_up = self._resolve_mode_switch_debug_total(
                live_raw_up,
                live_delta_up,
                mode_code,
                api_online=api_online,
                wan_only_enabled=wan_only_enabled,
                sessions=mode_sessions,
                now_mono=now_mono,
            )
        else:
            debug_total_up = live_raw_up
            replay_upload_up = 0

        replay_alloc_up = 0
        if collect_mode == 'docker':
            if replay_upload_up > 0 and mode_code != 'M0':
                if wan_only_enabled and mode_code in ('M2', 'M3'):
                    # 待判定回放已写入 backlog，由限幅补录路径统一分摊，避免与 backlog 双通道叠加。
                    pass
                elif wan_only_enabled:
                    replay_alloc_up, _ = self._apply_traffic_filter(
                        client, replay_upload_up, 0, mode_sessions,
                    )
                else:
                    replay_alloc_up = max(0, int(replay_upload_up or 0))
                if replay_alloc_up > 0:
                    self._mode_switch_replay_alloc_total_bytes += replay_alloc_up
        # 离线补录 tick 不把间隙流量分摊到当前会话（无法准确归属），但会清空本轮新增展示。
        alloc_sessions = mode_sessions if mode_sessions else (sessions or [])
        wan_backlog_applied_bytes = 0
        wan_backlog_before = max(0, int(self._wan_alloc_backlog_bytes or 0))
        alloc_input_up = 0
        effective_alloc_up = 0
        filter_wan_pool_up = max(0, int(live_delta_up or 0))
        if collect_mode == 'docker' and sessions is not None:
            if is_backfill:
                alloc_input_up = 0
            elif wan_only_enabled:
                base_wan_up = 0 if mode_code == 'M0' else max(0, int(live_delta_up or 0))
                alloc_input_up = max(0, base_wan_up + int(replay_alloc_up or 0))
            else:
                base_total_up = max(0, int(live_raw_up or 0))
                alloc_input_up = max(0, base_total_up + int(replay_alloc_up or 0))

            can_alloc = bool(_active_playback_sessions(alloc_sessions))
            wan_ready = (
                not wan_only_enabled
                or _has_confirmed_wan_playback(alloc_sessions)
            )
            backlog_contrib = 0
            backlog_raw_before = max(0, int(self._wan_alloc_backlog_bytes or 0))
            backlog_raw_after = backlog_raw_before
            if mode_code == 'M0' and int(replay_alloc_up or 0) <= 0:
                effective_alloc_up = 0
            elif (
                wan_only_enabled
                and mode_code in ('M2', 'M3')
                and wan_ready
            ):
                self._sync_wan_ledger_session(alloc_sessions)
                (
                    wan_from_docker,
                    _raw_drain,
                    wan_live,
                    wan_backlog_part,
                    backlog_raw_after,
                ) = self._plan_m3_wan_alloc_budget(
                    client,
                    live_raw_up=live_raw_up,
                    mode_code=mode_code,
                    sessions=alloc_sessions,
                    wan_only_enabled=wan_only_enabled,
                    burst_window_seconds=burst_window_seconds,
                    now_epoch=now_epoch,
                    tick_seconds=allocation_tick_seconds,
                )
                effective_alloc_up = self._cap_wan_assign_by_ledger(
                    wan_from_docker,
                    wan_live=wan_live,
                    wan_backlog_part=wan_backlog_part,
                )
                backlog_contrib = max(
                    0, int(effective_alloc_up or 0) - int(filter_wan_pool_up or 0),
                )
            else:
                effective_alloc_up = max(0, int(alloc_input_up or 0))

            if effective_alloc_up > 0 and not can_alloc:
                self._wan_alloc_backlog_bytes = backlog_raw_before
            elif effective_alloc_up > 0:
                wan_backlog_applied_bytes = backlog_contrib if backlog_contrib > 0 else 0
                try:
                    alloc_debug = emby_playback_traffic.accumulate_wan_upload(
                        self.name,
                        alloc_sessions,
                        effective_alloc_up,
                        wan_pool_only=wan_only_enabled,
                        new_session_window_seconds=self.monitor.burst_new_session_window_seconds,
                        seek_window_seconds=self.monitor.burst_seek_window_seconds,
                        priority_mode=self.monitor.burst_priority_mode,
                        tick_seconds=allocation_tick_seconds,
                    )
                    if isinstance(alloc_debug, dict):
                        allocation_debug.update(alloc_debug)
                        assigned_up = int(
                            alloc_debug.get('wan_upload_bytes')
                            or alloc_debug.get('assigned_bytes')
                            or effective_alloc_up
                            or 0,
                        )
                        if (
                            wan_only_enabled
                            and mode_code in ('M2', 'M3')
                            and wan_ready
                        ):
                            self._wan_assign_assigned_ledger += max(0, assigned_up)
                            self._wan_alloc_backlog_bytes = max(
                                0, int(backlog_raw_after or 0),
                            )
                        else:
                            remainder = alloc_debug.get('program_remainder_bytes')
                            if remainder is None:
                                remainder = alloc_debug.get('remainder_bytes')
                            self._wan_alloc_backlog_bytes = max(
                                0, int(remainder or 0),
                            )
                except Exception as e:
                    logger.debug(
                        f'[Emby:{self.name}] 外网播放上行累计失败: {e}',
                    )
                    self._wan_alloc_backlog_bytes = backlog_raw_before
            elif (
                not is_backfill
                and mode_code != 'M0'
                and wan_only_enabled
                and int(alloc_input_up or 0) > 0
                and not can_alloc
            ):
                self._wan_alloc_backlog_bytes += max(0, int(alloc_input_up or 0))
        elif collect_mode == 'lucky':
            alloc_wan_up = allocation_debug.get('wan_upload_bytes')
            if alloc_wan_up is None:
                alloc_wan_up = allocation_debug.get('wan_pool_bytes')
            live_delta_up = max(0, int(alloc_wan_up or live_delta_up or 0))
            filter_wan_pool_up = live_delta_up
            effective_alloc_up = live_delta_up
            alloc_input_up = live_delta_up
        elif collect_mode == 'docker':
            alloc_wan_up = allocation_debug.get('wan_upload_bytes')
            if alloc_wan_up is None:
                alloc_wan_up = allocation_debug.get('wan_pool_bytes')
            alloc_total_up = allocation_debug.get('total_upload_bytes')
            if alloc_total_up is None:
                alloc_total_up = live_raw_up
            if wan_only_enabled:
                live_delta_up = max(0, int(alloc_wan_up or 0))
            else:
                live_delta_up = max(0, int(alloc_total_up or 0))
        else:
            live_delta_up = 0
            live_delta_dl = 0
            alloc_wan_up = 0
        debug_wan_up = (
            allocation_debug.get('wan_upload_bytes')
            if allocation_debug.get('wan_upload_bytes') is not None
            else allocation_debug.get('wan_pool_bytes') or 0
        )
        debug_lan_up = (
            allocation_debug.get('lan_upload_bytes')
            if allocation_debug.get('lan_upload_bytes') is not None
            else allocation_debug.get('lan_pool_bytes') or 0
        )
        if wan_only_enabled and debug_total_up > 0 and int(debug_lan_up or 0) <= 0:
            if mode_code == 'M1':
                debug_lan_up = int(debug_total_up)
            else:
                # wan_pool_only 路径仅分摊 WAN 池；M3 调试补齐 LAN 残余便于观测。
                debug_lan_up = max(
                    0, int(debug_total_up) - max(0, int(debug_wan_up or 0)),
                )
        debug_traffic_metrics = _build_debug_traffic_metrics(
            debug_total_up,
            mode_sessions,
            debug_wan_up,
            debug_lan_up,
            allocation_debug.get('program_remainder_bytes')
            if allocation_debug.get('program_remainder_bytes') is not None
            else allocation_debug.get('remainder_bytes') or 0,
            mode_switch_pending_bytes=max(
                0, int(self._mode_switch_pending_upload_bytes or 0),
            ),
            mode_switch_replay_bytes=max(0, int(replay_upload_up or 0)),
            mode_switch_replay_alloc_bytes=max(0, int(replay_alloc_up or 0)),
            mode_switch_replay_total_bytes=max(
                0, int(self._mode_switch_replay_total_bytes or 0),
            ),
            mode_switch_replay_alloc_total_bytes=max(
                0, int(self._mode_switch_replay_alloc_total_bytes or 0),
            ),
            wan_alloc_backlog_bytes=max(0, int(self._wan_alloc_backlog_bytes or 0)),
            wan_alloc_backlog_applied_bytes=max(0, int(wan_backlog_applied_bytes or 0)),
            m1_wan_capture_bytes=max(0, int(self._m1_wan_capture_bytes or 0)),
            wan_only_enabled=wan_only_enabled,
        )
        lucky_conn_debug: dict = {}
        try:
            annotate_kwargs = {}
            sessions = emby_playback_traffic.annotate_live_sessions_upload(
                self.name, sessions, **annotate_kwargs,
            )
        except Exception as e:
            logger.debug(
                f'[Emby:{self.name}] 实时会话上行调试字段附加失败: {e}',
            )
        if collect_mode == 'lucky':
            try:
                verdict_sessions = list(
                    getattr(self, '_wan_client_sessions_last', None) or [],
                )
                if not verdict_sessions:
                    verdict_sessions = list(mode_sessions or [])
                lucky_conn_debug = emby_playback_traffic.get_lucky_conn_debug_snapshot(
                    self.name,
                    verdict_sessions,
                    list(getattr(self, '_lucky_conn_rows_last', None) or []),
                    dict(getattr(self, '_lucky_conn_deltas_last', None) or {}),
                    credit_browse=credit_browse,
                )
            except Exception as e:
                logger.debug(
                    f'[Emby:{self.name}] Lucky 连接调试快照失败: {e}',
                )
        tick_wan_assigned = max(
            0,
            int(allocation_debug.get('wan_upload_bytes')
                or allocation_debug.get('wan_pool_bytes') or 0),
        )
        self._last_tick_audit = emby_traffic_tick_audit.build_tick_audit(
            mode_code=mode_code,
            live_raw_up=live_raw_up,
            live_delta_up=filter_wan_pool_up,
            alloc_input_up=alloc_input_up,
            effective_alloc_up=effective_alloc_up,
            allocation_debug=allocation_debug,
            wan_backlog_before=wan_backlog_before,
            wan_backlog_after=max(0, int(self._wan_alloc_backlog_bytes or 0)),
            wan_backlog_applied=wan_backlog_applied_bytes,
            replay_alloc_up=replay_alloc_up,
            m1_capture_bytes=max(0, int(self._m1_wan_capture_bytes or 0)),
            mode_switch_pending_bytes=max(
                0, int(self._mode_switch_pending_upload_bytes or 0),
            ),
            debug_total_up=debug_total_up,
            debug_wan_up=int(debug_traffic_metrics.get('wan_upload_bytes') or 0),
            debug_lan_up=int(debug_traffic_metrics.get('lan_upload_bytes') or 0),
            debug_remainder_up=int(debug_traffic_metrics.get('program_remainder_bytes') or 0),
            sessions=sessions,
            wan_only_enabled=wan_only_enabled,
            m3_wan_pool_scale=getattr(self.monitor, 'm3_wan_pool_scale', 1.0),
            tick_seconds=allocation_tick_seconds,
        )
        if mode_code in ('M2', 'M3') and _has_confirmed_wan_playback(mode_sessions):
            self._verify_cumulative = emby_traffic_tick_audit.merge_cumulative(
                self._verify_cumulative,
                self._last_tick_audit,
                mode_code=mode_code,
                tick_wan_assigned=tick_wan_assigned,
            )
        elif mode_code == 'M0' and not _active_playback_sessions(mode_sessions):
            self._verify_cumulative = {}
            self._reset_wan_assign_ledger()
        self.monitor.update_live_cache(
            self.name,
            is_online=is_online,
            api_online=api_online,
            docker_available=docker_available and has_container,
            lucky_available=lucky_available if collect_mode == 'lucky' else False,
            traffic_collect_mode=collect_mode,
            delta_up=live_delta_up,
            delta_dl=live_delta_dl,
            sessions=sessions,
            debug_metrics=debug_traffic_metrics,
            traffic_tick_audit={
                'tick': self._last_tick_audit,
                'cumulative': dict(self._verify_cumulative or {}),
            },
            full=full,
            lucky_ip_traffic=lucky_ip_traffic if collect_mode == 'lucky' else None,
            lucky_ip_tick_deltas=lucky_ip_deltas if collect_mode == 'lucky' else None,
            lucky_conn_debug=lucky_conn_debug if collect_mode == 'lucky' else None,
        )
        if full:
            try:
                emby_playback_traffic.sync_live_upload_persistence(
                    self.name,
                    list(self._wan_client_sessions_last or sessions or []),
                    credit_browse=credit_browse,
                )
            except Exception as e:
                logger.debug(
                    f'[Emby:{self.name}] 会话流量持久化失败: {e}',
                )
            if api_online:
                try:
                    emby_user_sync.sync_deleted_users(self.name, client)
                except Exception as e:
                    logger.debug(
                        f'[Emby:{self.name}] 用户同步失败: {e}',
                    )
        if not sessions:
            try:
                emby_playback_traffic.clear_instance_live_upload_state(self.name)
            except Exception as e:
                logger.debug(
                    f'[Emby:{self.name}] 无会话时分摊状态清理失败: {e}',
                )
        self._prev_mode_code = mode_code
        self._was_online = is_online


class EmbyMonitor:
    def __init__(self, config: dict, config_path: str = None):
        self.config_path = config_path or config_manager.CONFIG_PATH
        self.config = config
        self.docker = DockerStatsClient()
        self.clients: Dict[str, EmbyClient] = {}
        self._workers: Dict[str, EmbyInstanceWorker] = {}
        self._running = False
        self._config_lock = threading.Lock()
        self._live_cache: Dict[str, dict] = {}
        self._live_cache_lock = threading.Lock()
        self._collect_generation: Dict[str, int] = {}
        self._state_generation: Dict[str, int] = {}
        self._live_status_traffic_cache: Dict[str, dict] = {}
        self._live_status_traffic_cache_at = 0.0
        self._live_status_traffic_cache_lock = threading.Lock()
        self._live_status_traffic_cache_ttl = 1.0
        self._apply_global_config()
        self._init_clients()

    def _apply_global_config(self):
        self.global_cfg = config_manager.get_global_config(self.config)
        try:
            refresh = int(self.global_cfg.get('refresh_interval', 1))
        except (TypeError, ValueError):
            refresh = 1
        refresh = max(
            config_manager.REFRESH_INTERVAL_MIN,
            min(config_manager.REFRESH_INTERVAL_MAX, refresh),
        )
        self.refresh_interval = refresh
        self.collect_interval = config_manager.collect_interval_for_refresh(refresh)
        try:
            burst_new_window = int(
                self.global_cfg.get('emby_burst_new_session_window_seconds', 8),
            )
        except (TypeError, ValueError):
            burst_new_window = 8
        try:
            burst_seek_window = int(
                self.global_cfg.get('emby_burst_seek_window_seconds', 6),
            )
        except (TypeError, ValueError):
            burst_seek_window = 6
        self.burst_new_session_window_seconds = max(1, min(30, burst_new_window))
        self.burst_seek_window_seconds = max(1, min(30, burst_seek_window))
        mode = str(self.global_cfg.get('emby_burst_priority_mode') or '').strip().lower()
        self.burst_priority_mode = mode if mode in ('seek_first', 'new_first') else 'seek_first'
        try:
            mode_switch_grace = int(
                self.global_cfg.get('emby_mode_switch_grace_seconds', 2),
            )
        except (TypeError, ValueError):
            mode_switch_grace = 2
        self.mode_switch_grace_seconds = max(0, min(10, mode_switch_grace))
        self.preplay_burst_mbps = config_manager.clamp_emby_preplay_burst_mbps(
            self.global_cfg.get('emby_preplay_burst_mbps', 1.5),
        )
        self.preplay_burst_window_seconds = (
            config_manager.clamp_emby_preplay_burst_window_seconds(
                self.global_cfg.get('emby_preplay_burst_window_seconds', 3),
            )
        )
        try:
            import emby_playback_traffic
            emby_playback_traffic.set_browse_stream_burst_bps(
                config_manager.emby_preplay_burst_bps(self.global_cfg),
            )
            emby_playback_traffic.set_browse_stream_burst_window_seconds(
                self.preplay_burst_window_seconds,
            )
        except Exception as e:
            logger.debug(f'推流突发识别阈值配置同步失败: {e}')
        self.m3_wan_pool_scale = config_manager.clamp_emby_m3_wan_pool_scale(
            self.global_cfg.get('emby_m3_wan_pool_scale', 1.0),
        )
        self.browse_upload_min_bytes = config_manager.emby_browse_upload_min_bytes(
            self.global_cfg,
        )
        tz_name = self.global_cfg.get('timezone', 'Asia/Shanghai')
        try:
            self.timezone = ZoneInfo(tz_name)
        except Exception:
            self.timezone = ZoneInfo('Asia/Shanghai')
        traffic_db.set_timezone(self.timezone)

    def _now(self) -> datetime:
        return datetime.now(self.timezone)

    def _init_clients(self):
        if not self.global_cfg.get('emby_enabled', False):
            return
        for inst_cfg in self.config.get('emby_instances', []):
            name = inst_cfg['name']
            self.clients[name] = EmbyClient(inst_cfg)
            logger.info(
                f'初始化 Emby 实例: {name} ({inst_cfg.get("host")}:'
                f'{inst_cfg.get("port", 8096)})'
            )

    def apply_config(self, new_config: dict) -> bool:
        try:
            with self._config_lock:
                new_config = config_manager.enrich_config(new_config or {})
                self.config = new_config
                self._apply_global_config()
                enabled = bool(self.global_cfg.get('emby_enabled', False))
                new_instances = {
                    i['name']: i for i in (new_config.get('emby_instances') or [])
                } if enabled else {}
                for name in list(self.clients.keys()):
                    if name not in new_instances:
                        del self.clients[name]
                for name, inst_cfg in new_instances.items():
                    if name in self.clients:
                        self.clients[name].update_config(inst_cfg)
                    else:
                        self.clients[name] = EmbyClient(inst_cfg)
            self._sync_workers()
            return True
        except Exception as e:
            logger.error(f'Emby 配置应用失败: {e}', exc_info=True)
            return False

    def reload_config(self):
        try:
            new_config = config_manager.load_runtime_config(self.config_path)
            return self.apply_config(new_config)
        except Exception as e:
            logger.error(f'Emby 配置热重载失败: {e}', exc_info=True)
            return False

    def _sync_workers(self):
        enabled = bool(self.global_cfg.get('emby_enabled', False))
        with self._config_lock:
            names = set(self.clients.keys()) if enabled else set()
        for name in list(self._workers.keys()):
            if name not in names:
                self._workers[name].stop()
                del self._workers[name]
        if not enabled:
            return
        for name in names:
            if name not in self._workers:
                worker = EmbyInstanceWorker(self, name)
                self._workers[name] = worker
                if self._running:
                    worker.start()
            elif self._running:
                self._workers[name].wake()

    def start(self):
        self._running = True
        self._sync_workers()
        logger.info(
            f'Emby 监控已启动（数据采集 {clamp_interval(self.collect_interval)}s，'
            f'轻量探测 {clamp_interval(self.refresh_interval)}s）'
        )

    def stop(self):
        self._running = False
        for worker in self._workers.values():
            worker.stop()
        self._workers.clear()

    def _bump_collect_generation(self, name: str) -> int:
        with self._live_cache_lock:
            val = self._collect_generation.get(name, 0) + 1
            self._collect_generation[name] = val
            return val

    def _bump_state_generation(self, name: str) -> int:
        with self._live_cache_lock:
            val = self._state_generation.get(name, 0) + 1
            self._state_generation[name] = val
            return val

    def update_live_cache(self, name: str, is_online: bool, api_online: bool,
                          docker_available: bool, delta_up: int, delta_dl: int,
                          sessions: list, debug_metrics: dict, full: bool,
                          traffic_tick_audit: dict = None,
                          lucky_available: bool = False,
                          traffic_collect_mode: str = '',
                          lucky_ip_traffic: dict = None,
                          lucky_ip_tick_deltas: dict = None,
                          lucky_conn_debug: dict = None):
        with self._live_cache_lock:
            prev = self._live_cache.get(name, {})
            prev_api_online = prev.get('api_online', False)
            offline_since = None
            online_since = None
            if api_online:
                online_since = _online_since_from_prev(prev, prev_api_online)
            else:
                if prev_api_online:
                    offline_since = emby_traffic_db._now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    offline_since = prev.get('offline_since')
                    if not offline_since:
                        offline_since = emby_traffic_db._now().strftime('%Y-%m-%d %H:%M:%S')
            entry = {
                'name': name,
                'is_online': is_online,
                'api_online': api_online,
                'docker_available': docker_available,
                'lucky_available': lucky_available,
                'traffic_collect_mode': traffic_collect_mode or '',
                'online_since': online_since,
                'offline_since': offline_since,
                'recent_delta_bytes': delta_up,
                'recent_delta_download_bytes': delta_dl,
                'session_count': len(sessions),
                'sessions': sessions,
                'debug_traffic_metrics': dict(debug_metrics or {}),
                'traffic_tick_audit': dict(traffic_tick_audit or {}),
                'collect_generation': self._collect_generation.get(name, 0),
                'state_generation': self._state_generation.get(name, 0),
            }
            if lucky_ip_traffic is not None:
                entry['lucky_ip_traffic'] = dict(lucky_ip_traffic or {})
            elif prev.get('lucky_ip_traffic'):
                entry['lucky_ip_traffic'] = dict(prev.get('lucky_ip_traffic') or {})
            if lucky_ip_tick_deltas is not None:
                entry['lucky_ip_tick_deltas'] = {
                    str(k): max(0, int(v or 0))
                    for k, v in (lucky_ip_tick_deltas or {}).items()
                    if str(k).strip()
                }
            elif prev.get('lucky_ip_tick_deltas'):
                entry['lucky_ip_tick_deltas'] = dict(prev.get('lucky_ip_tick_deltas') or {})
            if lucky_conn_debug is not None:
                entry['lucky_conn_debug'] = dict(lucky_conn_debug or {})
            elif prev.get('lucky_conn_debug'):
                prev_debug = prev.get('lucky_conn_debug')
                if isinstance(prev_debug, dict):
                    entry['lucky_conn_debug'] = dict(prev_debug)
                elif isinstance(prev_debug, list):
                    entry['lucky_conn_debug'] = {
                        'version': 2,
                        'groups': [],
                        'rows': list(prev_debug),
                        'emby_without_lucky': [],
                        'total_connections': len(prev_debug or []),
                    }
            self._live_cache[name] = entry
        if full:
            self._bump_collect_generation(name)
        self._bump_state_generation(name)

    def _get_live_status_traffic_batch(self, clients: dict) -> tuple:
        now_mono = time.monotonic()
        names = list(clients.keys())
        now = self._now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        period_key = (
            today_start.isoformat(),
            yesterday_start.isoformat(),
            month_start.isoformat(),
        )
        with self._live_status_traffic_cache_lock:
            if (
                names
                and self._live_status_traffic_cache
                and (now_mono - self._live_status_traffic_cache_at)
                < self._live_status_traffic_cache_ttl
                and self._live_status_traffic_cache.get('_period_key') == period_key
                and set(
                    n for n in self._live_status_traffic_cache.keys()
                    if not str(n).startswith('_')
                ) >= set(names)
            ):
                return (
                    {
                        name: dict(self._live_status_traffic_cache.get(name) or {})
                        for name in names
                    },
                    dict(self._live_status_traffic_cache.get('_status_map') or {}),
                    dict(self._live_status_traffic_cache.get('_data_starts') or {}),
                )

        credit_browse_map = {
            name: (
                client.traffic_collect_mode == 'lucky'
                and bool(client.lucky_credit_browse_traffic)
            )
            for name, client in clients.items()
        }
        upload_batch = emby_browse_upload_stats.get_live_status_upload_batch(
            names, credit_browse_map, today_start, yesterday_start, month_start,
        )
        download_today = emby_traffic_db.get_period_bytes_batch(
            names, today_start, 'download',
        )
        download_yesterday_base = emby_traffic_db.get_period_bytes_batch(
            names, yesterday_start, 'download',
        )
        download_month = emby_traffic_db.get_period_bytes_batch(
            names, month_start, 'download',
        )
        download_total = emby_traffic_db.get_total_bytes_batch(names, 'download')
        data_starts = emby_traffic_db.get_data_start_times_batch(names)
        status_map = {
            row['instance_name']: row
            for row in emby_traffic_db.get_all_instance_status()
        }
        traffic_batch = {}
        for name in names:
            upload = upload_batch.get(name, {})
            today_dl = download_today.get(name, 0)
            yesterday_base_dl = download_yesterday_base.get(name, 0)
            traffic_batch[name] = {
                'today_upload': int(upload.get('today_upload') or 0),
                'today_download': today_dl,
                'yesterday_upload': int(upload.get('yesterday_upload') or 0),
                'yesterday_download': max(0, yesterday_base_dl - today_dl),
                'month_upload': int(upload.get('month_upload') or 0),
                'month_download': download_month.get(name, 0),
                'device_upload': int(upload.get('device_upload') or 0),
                'device_download': download_total.get(name, 0),
            }
        cached = dict(traffic_batch)
        cached['_period_key'] = period_key
        cached['_status_map'] = status_map
        cached['_data_starts'] = data_starts
        with self._live_status_traffic_cache_lock:
            self._live_status_traffic_cache = cached
            self._live_status_traffic_cache_at = now_mono
        return traffic_batch, status_map, data_starts

    def get_live_status_summary(self) -> list:
        with self._live_cache_lock:
            cache = {k: dict(v) for k, v in self._live_cache.items()}
        result = []
        with self._config_lock:
            clients = dict(self.clients)
        traffic_batch, status_map, data_starts = self._get_live_status_traffic_batch(
            clients,
        )
        for name, client in clients.items():
            live = cache.get(name, {})
            status = status_map.get(name, {})
            traffic = traffic_batch.get(name, {})
            credit_browse = (
                client.traffic_collect_mode == 'lucky'
                and bool(client.lucky_credit_browse_traffic)
            )
            today_up = int(traffic.get('today_upload') or 0)
            today_dl = int(traffic.get('today_download') or 0)
            yesterday_up = int(traffic.get('yesterday_upload') or 0)
            yesterday_dl = int(traffic.get('yesterday_download') or 0)
            month_up = int(traffic.get('month_upload') or 0)
            month_dl = int(traffic.get('month_download') or 0)
            device_up = int(traffic.get('device_upload') or 0)
            device_dl = int(traffic.get('device_download') or 0)

            api_online = live.get('api_online', status.get('api_online', 0) == 1)
            raw_data_start = data_starts.get(name)
            data_start_time = (
                traffic_db._format_datetime_seconds(raw_data_start)
                if raw_data_start else None
            )
            offline_since = None
            online_since = None
            if api_online:
                raw_online = live.get('online_since')
                if raw_online:
                    online_since = traffic_db._format_datetime_seconds(raw_online)
            else:
                raw_offline = live.get('offline_since') or status.get('last_update')
                if raw_offline:
                    offline_since = traffic_db._format_datetime_seconds(raw_offline)

            result.append({
                **live,
                'name': name,
                'host': client.host,
                'port': client.port,
                'use_https': client.use_https,
                'container_name': client.container_name,
                'container_id': client.container_id,
                'display_priority': client.display_priority,
                'wan_traffic_only': client.wan_traffic_only,
                'traffic_collect_mode': client.traffic_collect_mode,
                'estimate_upload_enabled': client.traffic_collect_mode == 'docker',
                'lucky_rule_label': client.lucky_rule_label,
                'lucky_credit_browse_traffic': client.lucky_credit_browse_traffic,
                'is_online': live.get('is_online', status.get('is_online', 0) == 1),
                'api_online': api_online,
                'offline_since': offline_since,
                'online_since': online_since,
                'data_start_time': data_start_time,
                'docker_available': live.get(
                    'docker_available', status.get('docker_available', 0) == 1),
                'lucky_available': live.get('lucky_available', False),
                'traffic_collect_mode': live.get(
                    'traffic_collect_mode', client.traffic_collect_mode),
                'docker_socket_available': self.docker.is_available(),
                'monthly_uploaded_bytes': month_up,
                'monthly_downloaded_bytes': month_dl,
                'today_uploaded_bytes': today_up,
                'today_downloaded_bytes': today_dl,
                'yesterday_uploaded_bytes': yesterday_up,
                'yesterday_downloaded_bytes': yesterday_dl,
                'device_uploaded_bytes': device_up,
                'device_downloaded_bytes': device_dl,
                'recent_delta_bytes': live.get('recent_delta_bytes', 0),
                'recent_delta_download_bytes': live.get(
                    'recent_delta_download_bytes', 0),
                'session_count': live.get('session_count', 0),
                'sessions': live.get('sessions') or [],
                'lucky_ip_traffic': live.get('lucky_ip_traffic') or {},
                'lucky_ip_tick_deltas': live.get('lucky_ip_tick_deltas') or {},
                'traffic_tick_audit': live.get('traffic_tick_audit') or {},
                'collect_interval': self.collect_interval,
                'refresh_interval': self.refresh_interval,
                'last_update': status.get('last_update'),
                'collect_generation': live.get(
                    'collect_generation', self._collect_generation.get(name, 0)),
                'state_generation': live.get(
                    'state_generation', self._state_generation.get(name, 0)),
            })
        result.sort(key=lambda x: (x.get('display_priority', 500), x.get('name', '')))
        return result

    def get_status_summary(self) -> list:
        return self.get_live_status_summary()

    def get_traffic_verify_summary(self, instance_name: str = None) -> list:
        with self._live_cache_lock:
            cache = {k: dict(v) for k, v in self._live_cache.items()}
        rows = []
        for name, live in cache.items():
            if instance_name and name != instance_name:
                continue
            audit = live.get('traffic_tick_audit') or {}
            tick = audit.get('tick') or {}
            cumulative = audit.get('cumulative') or {}
            metrics = live.get('debug_traffic_metrics') or {}
            rows.append({
                'instance_name': name,
                'mode_code': metrics.get('mode_code') or '',
                'mode_label': metrics.get('mode_label') or '',
                'tick_passed': bool(tick.get('passed')),
                'tick_failed_count': int(tick.get('failed_count') or 0),
                'tick_checks': tick.get('checks') or [],
                'tick_failed_checks': tick.get('failed_checks') or [],
                'tick_inputs': tick.get('inputs') or {},
                'tick_outputs': tick.get('outputs') or {},
                'cumulative': cumulative,
                'debug_traffic_metrics': metrics,
                'recent_delta_bytes': int(live.get('recent_delta_bytes') or 0),
            })
        rows.sort(key=lambda r: r.get('instance_name') or '')
        return rows

    def reset_traffic_verify(self, instance_name: str):
        worker = self._workers.get(instance_name)
        if worker:
            worker._verify_cumulative = {}
            worker._last_tick_audit = {}
            worker.wake()

    def test_docker_container(self, container_name: str = '',
                              container_id: str = '') -> dict:
        return self.docker.test_container(container_name, container_id)

    def reset_traffic_stats(self, instance_name: str):
        if not instance_name:
            raise ValueError('参数缺失')
        with self._config_lock:
            if not config_manager.get_emby_instance(instance_name, self.config):
                raise ValueError('设备不存在')
        emby_traffic_db.reset_instance_traffic(instance_name)
        emby_traffic_db.clear_lucky_ip_baselines(instance_name)
        try:
            emby_playback_traffic.clear_persisted_live_upload_state(instance_name)
        except Exception:
            pass
        try:
            import browse_upload_settler
            browse_upload_settler.clear_instance(instance_name)
        except Exception:
            pass
        worker = self._workers.get(instance_name)
        if worker:
            worker._live_upload_hydrated = False
            worker._lucky_ip_baselines = {}
            worker._lucky_conn_baselines = {}
            worker._lucky_conn_rows_last = []
            worker._lucky_conn_deltas_last = {}
            worker._lucky_ip_traffic_last = {}
            worker._lucky_ip_deltas_last = {}
            worker._wan_client_sessions_last = []
            worker._lucky_total_out = 0
            worker._lucky_total_in = 0
            client = None
            with self._config_lock:
                client = self.clients.get(instance_name)
            if client:
                stats = worker._fetch_docker_stats(client)
                if stats:
                    worker._baseline_tx = stats['tx_bytes']
                    worker._baseline_rx = stats['rx_bytes']
            worker.wake()
        self._bump_collect_generation(instance_name)
        self._bump_state_generation(instance_name)
        logger.info(f'Emby 流量统计重置完成: {instance_name}')

    def wake_all(self):
        for worker in self._workers.values():
            worker.wake()
