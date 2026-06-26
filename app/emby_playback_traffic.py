"""Emby 单次外网播放会话的上行流量估算累计。"""

import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from emby_traffic_filter import (
    is_wan_playback_session,
    legacy_playback_accumulator_key,
    playback_accumulator_key,
    session_docker_share_bps,
    session_stream_bps,
)

_lock = threading.RLock()
_upload_accumulators: Dict[str, Dict[str, int]] = {}
_allocator_runtime: Dict[str, dict] = {}
_live_tick_uploads: Dict[str, Dict[str, int]] = {}
_accumulator_touch_mono: Dict[str, Dict[str, float]] = {}

DEFAULT_NEW_SESSION_WINDOW_SECONDS = 8
DEFAULT_SEEK_WINDOW_SECONDS = 6
DEFAULT_PRIORITY_MODE = 'seek_first'
_VALID_PRIORITY_MODES = frozenset({'seek_first', 'new_first'})
_MAX_WINDOW_SECONDS = 30
_SESSION_STATE_GRACE_SECONDS = 120
_ACCUMULATOR_STALE_SECONDS = 30 * 60


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_window_seconds(value, default_value: int) -> int:
    parsed = _safe_int(value, default_value)
    if parsed <= 0:
        parsed = default_value
    return max(1, min(_MAX_WINDOW_SECONDS, parsed))


def _normalize_priority_mode(value) -> str:
    mode = str(value or '').strip().lower()
    if mode not in _VALID_PRIORITY_MODES:
        return DEFAULT_PRIORITY_MODE
    return mode


def _session_runtime_lookup_key(session: dict, *, fallback: str = '') -> str:
    sid = str(
        session.get('emby_session_id')
        or session.get('session_id')
        or session.get('id')
        or ''
    ).strip()
    if sid:
        return f'sid:{sid}'
    persist_key = playback_accumulator_key(session)
    if persist_key:
        return persist_key
    return str(fallback or '').strip()


def _migrate_accumulator_key(name: str, old_key: str, new_key: str,
                             now_mono: float) -> None:
    old_key = str(old_key or '').strip()
    new_key = str(new_key or '').strip()
    if not name or not old_key or not new_key or old_key == new_key:
        return
    bucket = _upload_accumulators.get(name)
    if not bucket or old_key not in bucket:
        return
    moved = max(0, int(bucket.pop(old_key, 0) or 0))
    if moved <= 0:
        return
    bucket[new_key] = bucket.get(new_key, 0) + moved
    touched = _accumulator_touch_mono.setdefault(name, {})
    touched.pop(old_key, None)
    _touch_accumulator_key(name, new_key, now_mono)


def _active_upload_sessions(sessions: list) -> List[dict]:
    return [
        s for s in (sessions or [])
        if isinstance(s, dict)
        and bool(s.get('is_playing'))
        and not bool(s.get('is_paused'))
    ]


def _resolve_wan_pool(delta_up: int, active: List[dict], wan: List[dict],
                      *, wan_pool_only: bool) -> int:
    if delta_up <= 0 or not active or not wan:
        return 0
    if wan_pool_only:
        return delta_up
    lan = [s for s in active if not is_wan_playback_session(s)]
    if not lan:
        return delta_up
    wan_bps = sum(max(0, session_stream_bps(s)) for s in wan)
    total_bps = sum(max(0, session_stream_bps(s)) for s in active)
    if total_bps <= 0:
        ratio = len(wan) / len(active)
    else:
        ratio = wan_bps / total_bps
    ratio = max(0.0, min(1.0, ratio))
    return int(delta_up * ratio)


def _distribute_weighted(pool: int, infos: List[dict]) -> Dict[str, int]:
    result: Dict[str, int] = {}
    pool = max(0, int(pool or 0))
    if pool <= 0 or not infos:
        return result
    weights = [max(0, int(i.get('bps') or 0)) for i in infos]
    total_weight = sum(weights)
    if total_weight <= 0:
        share = pool // len(infos)
        remainder = pool % len(infos)
        for idx, info in enumerate(infos):
            key = info.get('key') or ''
            if not key:
                continue
            result[key] = result.get(key, 0) + share + (1 if idx < remainder else 0)
        return result
    assigned = 0
    for idx, info in enumerate(infos):
        key = info.get('key') or ''
        if not key:
            continue
        if idx == len(infos) - 1:
            part = pool - assigned
        else:
            part = int(pool * weights[idx] / total_weight)
            assigned += part
        result[key] = result.get(key, 0) + max(0, int(part))
    return result


def _parse_iso_epoch_seconds(value: str) -> Optional[float]:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        if raw.endswith('Z'):
            raw = raw[:-1] + '+00:00'
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


def _instance_runtime_state(name: str) -> dict:
    state = _allocator_runtime.get(name)
    if state is None:
        state = {
            'last_tick_mono': None,
            'sessions': {},
        }
        _allocator_runtime[name] = state
    return state


def _session_runtime_state(state: dict, key: str, now_mono: float) -> dict:
    sessions = state.setdefault('sessions', {})
    entry = sessions.get(key)
    if entry is None:
        entry = {
            'first_seen_mono': now_mono,
            'last_seen_mono': now_mono,
            'last_seek_count': 0,
            'seek_active_until_mono': 0.0,
            'live_last_upload_bytes': None,
            'live_last_sample_mono': None,
        }
        sessions[key] = entry
    return entry


def _touch_session_flags(state: dict, key: str, session: dict,
                         now_mono: float, now_epoch: float,
                         new_window_seconds: int, seek_window_seconds: int) -> tuple:
    entry = _session_runtime_state(state, key, now_mono)
    entry['last_seen_mono'] = now_mono

    seek_count = max(0, _safe_int(session.get('seek_count'), 0))
    last_seek_count = max(0, _safe_int(entry.get('last_seek_count'), 0))
    if seek_count > last_seek_count:
        entry['seek_active_until_mono'] = now_mono + seek_window_seconds
    entry['last_seek_count'] = seek_count

    if seek_count > 0 and float(entry.get('seek_active_until_mono') or 0.0) <= now_mono:
        last_seek_at = _parse_iso_epoch_seconds(session.get('last_seek_at') or '')
        if last_seek_at is not None:
            elapsed = now_epoch - last_seek_at
            if elapsed < seek_window_seconds:
                entry['seek_active_until_mono'] = now_mono + max(
                    0.5, seek_window_seconds - max(0.0, elapsed),
                )

    is_new = (now_mono - float(entry.get('first_seen_mono') or now_mono)) <= new_window_seconds
    is_seek = float(entry.get('seek_active_until_mono') or 0.0) > now_mono
    return bool(is_new), bool(is_seek)


def _cleanup_runtime_state(name: str, state: dict, now_mono: float,
                           keep_seconds: int) -> None:
    sessions = state.get('sessions') or {}
    stale_after = max(30, int(keep_seconds))
    for key, entry in list(sessions.items()):
        last_seen = float(entry.get('last_seen_mono') or 0.0)
        if now_mono - last_seen > stale_after:
            sessions.pop(key, None)
    _cleanup_stale_accumulators(name, now_mono)
    if not sessions and not (_upload_accumulators.get(name) or {}):
        _allocator_runtime.pop(name, None)
    if not sessions and not (_live_tick_uploads.get(name) or {}):
        _live_tick_uploads.pop(name, None)


def _pick_burst_targets(infos: List[dict], priority_mode: str) -> List[dict]:
    seek_infos = [i for i in infos if i.get('is_seek')]
    new_infos = [i for i in infos if i.get('is_new')]
    if priority_mode == 'new_first':
        if new_infos:
            return new_infos
        if seek_infos:
            return seek_infos
        return []
    if seek_infos:
        return seek_infos
    if new_infos:
        return new_infos
    return []


def _estimate_expected_pool_bytes(infos: List[dict], elapsed_seconds: float) -> int:
    elapsed = max(0.5, min(120.0, float(elapsed_seconds or 1.0)))
    bps_total = sum(max(0, int(i.get('bps') or 0)) for i in infos)
    if bps_total <= 0:
        return 0
    return max(0, int(bps_total * elapsed / 8))


def _allocation_debug_payload(*, total_upload_bytes: int = 0, wan_upload_bytes: int = 0,
                              lan_upload_bytes: int = 0, assigned_bytes: int = 0,
                              target_session_count: int = 0,
                              wan_session_count: int = 0,
                              lan_session_count: int = 0,
                              remainder_bytes: int = 0) -> dict:
    total_upload = max(0, int(total_upload_bytes or 0))
    wan_upload = max(0, int(wan_upload_bytes or 0))
    lan_upload = max(0, int(lan_upload_bytes or 0))
    assigned = max(0, int(assigned_bytes or 0))
    if wan_upload + lan_upload > total_upload:
        overflow = wan_upload + lan_upload - total_upload
        lan_upload = max(0, lan_upload - overflow)
    if assigned <= 0:
        assigned = wan_upload + lan_upload
    assigned = max(0, min(total_upload, assigned))
    remainder = max(0, int(remainder_bytes or 0))
    remainder = min(max(0, total_upload - assigned), remainder if remainder > 0 else total_upload - assigned)
    return {
        'total_upload_bytes': total_upload,
        'wan_upload_bytes': wan_upload,
        'lan_upload_bytes': lan_upload,
        # 兼容旧字段命名
        'wan_pool_bytes': wan_upload,
        'lan_pool_bytes': lan_upload,
        'assigned_bytes': assigned,
        'remainder_bytes': remainder,
        'program_remainder_bytes': remainder,
        'target_session_count': max(0, int(target_session_count or 0)),
        'wan_session_count': max(0, int(wan_session_count or 0)),
        'lan_session_count': max(0, int(lan_session_count or 0)),
    }


def _set_live_tick_uploads(name: str, uploads: Dict[str, int]) -> None:
    cleaned = {
        str(k): max(0, int(v))
        for k, v in (uploads or {}).items()
        if k and int(v or 0) > 0
    }
    _live_tick_uploads[name] = cleaned


def _touch_accumulator_key(name: str, key: str, now_mono: float) -> None:
    if not name or not key:
        return
    touched = _accumulator_touch_mono.setdefault(name, {})
    touched[key] = float(now_mono)


def _cleanup_stale_accumulators(name: str, now_mono: float,
                                stale_seconds: int = _ACCUMULATOR_STALE_SECONDS) -> None:
    bucket = _upload_accumulators.get(name)
    if not bucket:
        _upload_accumulators.pop(name, None)
        _accumulator_touch_mono.pop(name, None)
        return

    touched = _accumulator_touch_mono.get(name)
    if touched is None:
        touched = {}
        _accumulator_touch_mono[name] = touched

    stale_after = max(60, int(stale_seconds or _ACCUMULATOR_STALE_SECONDS))
    runtime_sessions = (_allocator_runtime.get(name) or {}).get('sessions') or {}
    tick_bucket = _live_tick_uploads.get(name)
    for key in list(bucket.keys()):
        seen_at = float(touched.get(key) or 0.0)
        if seen_at <= 0.0:
            touched[key] = now_mono
            continue
        if now_mono - seen_at <= stale_after:
            continue
        bucket.pop(key, None)
        touched.pop(key, None)
        runtime_sessions.pop(key, None)
        if isinstance(tick_bucket, dict):
            tick_bucket.pop(key, None)

    if not bucket:
        _upload_accumulators.pop(name, None)
    if not touched:
        _accumulator_touch_mono.pop(name, None)
    if isinstance(tick_bucket, dict) and not tick_bucket:
        _live_tick_uploads.pop(name, None)


def _accumulator_key_candidates(event: dict) -> List[str]:
    keys: List[str] = []
    stored = str(event.get('upload_accumulator_key') or '').strip()
    if stored:
        keys.append(stored)
    for factory in (playback_accumulator_key, legacy_playback_accumulator_key):
        key = factory(event)
        if key and key not in keys:
            keys.append(key)
    return keys


def _match_accumulator_key(bucket: dict, event: dict) -> Optional[str]:
    if not bucket or not event:
        return None
    for key in _accumulator_key_candidates(event):
        if key in bucket:
            return key

    user = (event.get('user_name') or '').strip().casefold()
    client = (event.get('client') or '').strip().casefold()
    title = (event.get('item_title') or event.get('episode_title') or '').casefold()
    series = (event.get('series_name') or '').casefold()
    episode_label = (event.get('episode_label') or '').strip().casefold()
    item_id = str(event.get('item_id') or '').strip()
    if not user:
        return None

    best_key = None
    best_score = 0
    for key in list(bucket.keys()):
        if not key.startswith(f'{user}|'):
            continue
        score = 1
        parts = key.split('|')
        if item_id and item_id in parts:
            score += 20
        if client and client in parts:
            score += 8
        if episode_label and episode_label in parts:
            score += 12
        if title and (key.endswith(f'|{title}') or f'|{title}' in key):
            score += 6
        if series and series in parts:
            score += 3
        if score > best_score:
            best_score = score
            best_key = key
    if best_score >= 10:
        return best_key
    return None


def _resolve_bucket_upload(bucket: dict, event: dict, preferred_key: str = '') -> tuple:
    if not bucket or not event:
        return None, None
    key = str(preferred_key or '').strip()
    if key and key in bucket:
        return key, max(0, int(bucket.get(key) or 0))
    key = _match_accumulator_key(bucket, event)
    if not key:
        return None, None
    value = bucket.get(key)
    if value is None:
        return None, None
    return key, max(0, int(value))


def _snapshot_upload_buckets(name: str) -> tuple:
    with _lock:
        return (
            dict(_upload_accumulators.get(name) or {}),
            dict(_live_tick_uploads.get(name) or {}),
        )


def accumulate_wan_upload(instance_name: str, sessions: list, delta_up: int,
                          wan_pool_only: bool = False,
                          new_session_window_seconds: int = DEFAULT_NEW_SESSION_WINDOW_SECONDS,
                          seek_window_seconds: int = DEFAULT_SEEK_WINDOW_SECONDS,
                          priority_mode: str = DEFAULT_PRIORITY_MODE,
                          tick_seconds: float = None) -> dict:
    """将本采集周期上传按会话分摊，并累计外网会话分配结果。"""
    name = (instance_name or '').strip()
    delta_up = max(0, int(delta_up or 0))
    if not name:
        return _allocation_debug_payload()
    now_mono = time.monotonic()
    if delta_up <= 0 or not sessions:
        with _lock:
            _set_live_tick_uploads(name, {})
            runtime = _allocator_runtime.get(name)
            if runtime is not None:
                _cleanup_runtime_state(
                    name, runtime, now_mono, _SESSION_STATE_GRACE_SECONDS,
                )
            else:
                _cleanup_stale_accumulators(name, now_mono)
        return _allocation_debug_payload(
            total_upload_bytes=delta_up,
            remainder_bytes=delta_up,
        )

    new_window = _normalize_window_seconds(
        new_session_window_seconds, DEFAULT_NEW_SESSION_WINDOW_SECONDS,
    )
    seek_window = _normalize_window_seconds(
        seek_window_seconds, DEFAULT_SEEK_WINDOW_SECONDS,
    )
    mode = _normalize_priority_mode(priority_mode)
    active = _active_upload_sessions(sessions)
    if not active:
        with _lock:
            _set_live_tick_uploads(name, {})
            runtime = _allocator_runtime.get(name)
            keep_seconds = (
                max(new_window, seek_window) + _SESSION_STATE_GRACE_SECONDS
            )
            if runtime is not None:
                _cleanup_runtime_state(name, runtime, now_mono, keep_seconds)
            else:
                _cleanup_stale_accumulators(name, now_mono)
        return _allocation_debug_payload(
            total_upload_bytes=delta_up,
            remainder_bytes=delta_up,
        )

    now_epoch = time.time()
    with _lock:
        runtime = _instance_runtime_state(name)
        last_tick = runtime.get('last_tick_mono')
        runtime['last_tick_mono'] = now_mono
        if last_tick is None:
            elapsed_seconds = float(tick_seconds or 1.0)
        else:
            elapsed_seconds = now_mono - float(last_tick)
        elapsed_seconds = max(0.5, min(120.0, elapsed_seconds))

        infos_all: List[dict] = []
        wan_infos: List[dict] = []
        key_meta: Dict[str, dict] = {}
        lan_idx = 0
        wan_idx = 0

        for session in active:
            is_wan = bool(is_wan_playback_session(session))
            bps = max(0, int(session_docker_share_bps(session) or 0))
            if bps <= 0:
                bps = max(0, int(session_stream_bps(session) or 0))
            if is_wan:
                wan_idx += 1
                sid = str(
                    session.get('emby_session_id')
                    or session.get('session_id')
                    or session.get('id')
                    or ''
                ).strip()
                persist_key = playback_accumulator_key(session)
                if not persist_key and sid:
                    persist_key = f'sid:{sid}'
                runtime_key = _session_runtime_lookup_key(
                    session, fallback=f'wan-ephemeral:{wan_idx}',
                )
                is_new, is_seek = _touch_session_flags(
                    runtime,
                    runtime_key,
                    session,
                    now_mono,
                    now_epoch,
                    new_window,
                    seek_window,
                )
                sessions_state = runtime.setdefault('sessions', {})
                runtime_entry = sessions_state.get(runtime_key) or {}
                old_persist = str(runtime_entry.get('persist_key') or '').strip()
                new_persist = str(persist_key or '').strip()
                if old_persist and new_persist and old_persist != new_persist:
                    _migrate_accumulator_key(name, old_persist, new_persist, now_mono)
                if new_persist:
                    runtime_entry['persist_key'] = new_persist
                    sessions_state[runtime_key] = runtime_entry
                info_key = runtime_key
                info = {
                    'key': info_key,
                    'bps': bps,
                    'is_new': is_new,
                    'is_seek': is_seek,
                }
                infos_all.append(info)
                wan_infos.append(info)
                key_meta[info_key] = {
                    'is_wan': True,
                    'persist_key': persist_key if persist_key else None,
                }
                continue

            lan_idx += 1
            info_key = f'lan:{lan_idx}'
            info = {
                'key': info_key,
                'bps': bps,
                'is_new': False,
                'is_seek': False,
            }
            infos_all.append(info)
            key_meta[info_key] = {
                'is_wan': False,
                'persist_key': None,
            }

        if not infos_all:
            _set_live_tick_uploads(name, {})
            _cleanup_runtime_state(
                name, runtime, now_mono, max(new_window, seek_window) + _SESSION_STATE_GRACE_SECONDS,
            )
            return _allocation_debug_payload(
                total_upload_bytes=delta_up,
                remainder_bytes=delta_up,
            )

        allocation_infos = wan_infos if wan_pool_only else infos_all
        if not allocation_infos:
            _set_live_tick_uploads(name, {})
            _cleanup_runtime_state(
                name, runtime, now_mono, max(new_window, seek_window) + _SESSION_STATE_GRACE_SECONDS,
            )
            return _allocation_debug_payload(
                total_upload_bytes=delta_up,
                remainder_bytes=delta_up,
                target_session_count=len(infos_all),
                wan_session_count=len(wan_infos),
                lan_session_count=max(0, len(infos_all) - len(wan_infos)),
            )

        if wan_pool_only and wan_infos:
            # 输入已是 filter 切出的 WAN 池，全量分给外网会话（突发优先新/seek 会话）。
            burst_targets = _pick_burst_targets(wan_infos, mode)
            primary = burst_targets if burst_targets else wan_infos
            merged = _distribute_weighted(delta_up, primary)
            assigned = sum(merged.values())
            remainder = max(0, delta_up - assigned)
            if remainder > 0:
                extra = _distribute_weighted(remainder, wan_infos)
                for key, amount in extra.items():
                    merged[key] = merged.get(key, 0) + amount
        else:
            expected_pool = _estimate_expected_pool_bytes(allocation_infos, elapsed_seconds)
            base_pool = min(delta_up, expected_pool)
            base_shares = _distribute_weighted(base_pool, allocation_infos)
            assigned_base = sum(base_shares.values())
            burst_pool = max(0, delta_up - assigned_base)
            burst_targets = _pick_burst_targets(wan_infos, mode)
            burst_fallback = allocation_infos if wan_pool_only else infos_all
            burst_shares = _distribute_weighted(
                burst_pool,
                burst_targets if burst_targets else burst_fallback,
            )
            merged: Dict[str, int] = {}
            for mapping in (base_shares, burst_shares):
                for key, amount in mapping.items():
                    if amount <= 0:
                        continue
                    merged[key] = merged.get(key, 0) + amount

        wan_upload = 0
        lan_upload = 0
        wan_tick_uploads: Dict[str, int] = {}
        for key, amount in merged.items():
            meta = key_meta.get(key) or {}
            amount = max(0, int(amount or 0))
            if amount <= 0:
                continue
            if meta.get('is_wan'):
                wan_upload += amount
                persist_key = str(meta.get('persist_key') or '').strip()
                if persist_key:
                    wan_tick_uploads[persist_key] = wan_tick_uploads.get(persist_key, 0) + amount
            else:
                lan_upload += amount

        _set_live_tick_uploads(name, wan_tick_uploads)
        if wan_tick_uploads:
            bucket = _upload_accumulators.setdefault(name, {})
            for key, amount in wan_tick_uploads.items():
                bucket[key] = bucket.get(key, 0) + amount
                _touch_accumulator_key(name, key, now_mono)

        _cleanup_runtime_state(
            name, runtime, now_mono, max(new_window, seek_window) + _SESSION_STATE_GRACE_SECONDS,
        )
        assigned_total = max(0, wan_upload + lan_upload)
        return _allocation_debug_payload(
            total_upload_bytes=delta_up,
            wan_upload_bytes=wan_upload,
            lan_upload_bytes=lan_upload,
            assigned_bytes=assigned_total,
            remainder_bytes=max(0, delta_up - assigned_total),
            target_session_count=len(infos_all),
            wan_session_count=len(wan_infos),
            lan_session_count=max(0, len(infos_all) - len(wan_infos)),
        )


def peek_accumulated_upload(instance_name: str, event: dict) -> Optional[int]:
    name = (instance_name or '').strip()
    if not name or not event:
        return None
    bucket, _ = _snapshot_upload_buckets(name)
    _, value = _resolve_bucket_upload(bucket, event)
    if value is None:
        return None
    return value


def annotate_live_sessions_upload(instance_name: str, sessions: list) -> List[dict]:
    """给实时会话附加当前已累计分摊上行与本轮分摊新增（调试展示）。"""
    name = (instance_name or '').strip()
    result: List[dict] = []
    upload_bucket: Dict[str, int] = {}
    tick_bucket: Dict[str, int] = {}
    if name:
        upload_bucket, tick_bucket = _snapshot_upload_buckets(name)

    for raw in sessions or []:
        if isinstance(raw, dict):
            session = dict(raw)
        else:
            continue

        if name and session.get('is_remote') and bool(session.get('is_playing')) and not bool(
            session.get('is_paused'),
        ):
            live_key, upload_live = _resolve_bucket_upload(upload_bucket, session)
            if upload_live is None:
                upload_live = 0
            _, upload_1s = _resolve_bucket_upload(
                tick_bucket, session, preferred_key=live_key or '',
            )
            if upload_1s is None:
                upload_1s = 0

            session['estimated_upload_bytes_live'] = upload_live
            session['estimated_upload_bytes_1s_live'] = upload_1s
        result.append(session)
    return result


def _purge_session_allocation_runtime(name: str, event: dict, *,
                                      bucket_key: str = '') -> None:
    runtime = _allocator_runtime.get(name)
    sessions_state = (runtime or {}).get('sessions') or {}
    sid = str(
        event.get('emby_session_id')
        or event.get('session_id')
        or event.get('id')
        or ''
    ).strip()
    keys = set()
    if bucket_key:
        keys.add(bucket_key)
    if sid:
        keys.add(f'sid:{sid}')
    for lookup in _accumulator_key_candidates(event):
        if lookup:
            keys.add(lookup)
    for key in list(sessions_state.keys()):
        if key in keys:
            sessions_state.pop(key, None)
    tick_bucket = _live_tick_uploads.get(name)
    if isinstance(tick_bucket, dict):
        for lookup in keys:
            tick_bucket.pop(lookup, None)
    if runtime and not sessions_state and not (_upload_accumulators.get(name) or {}):
        _allocator_runtime.pop(name, None)
    if isinstance(tick_bucket, dict) and not tick_bucket:
        _live_tick_uploads.pop(name, None)


def clear_instance_live_upload_state(instance_name: str) -> None:
    """API 无播放会话时，清空实例级实时分摊状态。"""
    name = (instance_name or '').strip()
    if not name:
        return
    with _lock:
        _upload_accumulators.pop(name, None)
        _live_tick_uploads.pop(name, None)
        _accumulator_touch_mono.pop(name, None)
        _allocator_runtime.pop(name, None)


def purge_stopped_wan_live_upload_state(instance_name: str, sessions: list) -> None:
    """停止播放后清理实时分摊桶与运行时状态，避免同会话再次播放时叠加旧累计。"""
    name = (instance_name or '').strip()
    if not name:
        return
    for raw in sessions or []:
        if not isinstance(raw, dict):
            continue
        if not raw.get('is_remote'):
            continue
        if bool(raw.get('is_playing')) and not bool(raw.get('is_paused')):
            continue
        if not (raw.get('item_id') or raw.get('title')):
            continue
        with _lock:
            bucket = _upload_accumulators.get(name) or {}
            touched = _accumulator_touch_mono.get(name)
            key = _match_accumulator_key(bucket, raw)
            if key:
                bucket.pop(key, None)
                if isinstance(touched, dict):
                    touched.pop(key, None)
                if not bucket:
                    _upload_accumulators.pop(name, None)
                if isinstance(touched, dict) and not touched:
                    _accumulator_touch_mono.pop(name, None)
            _purge_session_allocation_runtime(name, raw, bucket_key=key or '')


def take_accumulated_upload(instance_name: str, event: dict) -> Optional[int]:
    """读取并清除与停止播放事件匹配的外网累计上行（累加器路径信任 Docker 实测）。"""
    name = (instance_name or '').strip()
    if not name or not event:
        return None
    with _lock:
        bucket = _upload_accumulators.get(name) or {}
        touched = _accumulator_touch_mono.get(name)
        key = _match_accumulator_key(bucket, event)
        if not key:
            return None
        value = bucket.pop(key, None)
        if key:
            if isinstance(touched, dict):
                touched.pop(key, None)
        if not bucket:
            _upload_accumulators.pop(name, None)
        if isinstance(touched, dict) and not touched:
            _accumulator_touch_mono.pop(name, None)
        _purge_session_allocation_runtime(name, event, bucket_key=key or '')
    if value is None:
        return None
    raw = max(0, int(value))
    if raw <= 0:
        return None
    return raw
