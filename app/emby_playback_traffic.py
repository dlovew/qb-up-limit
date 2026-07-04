"""Emby 单次外网播放会话的上行流量估算累计。"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from emby_client import EmbyClient
from emby_traffic_filter import (
    filter_superseded_wan_sessions,
    is_wan_playback_session,
    is_wan_remote_session,
    legacy_playback_accumulator_key,
    parse_endpoint_ip,
    playback_accumulator_key,
    session_docker_share_bps,
    session_stream_bps,
)

try:
    from emby_lucky_verdict import (
        analyze_lucky_connections,
        binding_targets_from_analysis,
        browse_persist_key_variants_for_session,
        is_wan_browse_session,
        legacy_browse_persist_key_for_session,
        match_hints_from_analysis,
        sid_from_browse_persist_key,
    )
except ImportError:
    def analyze_lucky_connections(*args, **kwargs):
        return {
            'version': 2, 'groups': [], 'rows': [],
            'emby_without_lucky': [],
        }

    def binding_targets_from_analysis(analysis):
        return {}

    def match_hints_from_analysis(analysis):
        return {}

    def is_wan_browse_session(session, *, credit_browse=True):
        return False

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_upload_accumulators: Dict[str, Dict[str, int]] = {}
_browse_upload_accumulators: Dict[str, Dict[str, int]] = {}
_browse_session_meta: Dict[str, Dict[str, dict]] = {}
_allocator_runtime: Dict[str, dict] = {}
_live_tick_uploads: Dict[str, Dict[str, int]] = {}
_accumulator_touch_mono: Dict[str, Dict[str, float]] = {}
_conn_bindings: Dict[str, Dict[str, str]] = {}
_conn_match_hints: Dict[str, Dict[str, str]] = {}
_segment_conn_baselines: Dict[str, Dict[str, Dict[str, int]]] = {}
_conn_info_cache: Dict[str, Dict[str, dict]] = {}
# 开播前突发追踪：Emby 会话在开播前会有一段（可长达数十秒）仍报 connected/viewing
# 而连接已在推流缓冲的窗口，这段突发会按选片入账进选片桶。此处并行记录"计入选片
# 桶的推流突发量"（带时间戳，不受实时累加器清理影响），待开播结算那一刻把「开播前
# 突发窗口」秒内的突发从选片桶移回播放键累加器，更早的突发保留为真实选片流量。
# name -> browse_key -> [(monotonic_ts, bytes), ...]
_browse_preplay_burst: Dict[str, Dict[str, List[Tuple[float, int]]]] = {}
# 推流突发识别阈值（字节/秒）：会话仍报 connected/viewing 但单 tick 上传速率达到
# 此值，即判定为开播缓冲突发。可由「推流突发识别阈值 (MB/s)」设置项覆盖。
BROWSE_STREAM_BURST_BPS = 1_500_000
# 开播前突发窗口（秒）：开播结算时只回溯该秒数内的突发归入播放。可由设置项覆盖。
BROWSE_STREAM_BURST_WINDOW_SECONDS = 3
# 突发条目最长保留时长（秒）：超过则视为与本次开播无关的历史，避免无限增长。
_BURST_ENTRY_RETENTION_SECONDS = 120


def set_browse_stream_burst_bps(bps) -> None:
    """由配置同步推流突发识别阈值（字节/秒）。"""
    global BROWSE_STREAM_BURST_BPS
    try:
        val = int(bps)
    except (TypeError, ValueError):
        return
    if val > 0:
        BROWSE_STREAM_BURST_BPS = val


def set_browse_stream_burst_window_seconds(seconds) -> None:
    """由配置同步开播前突发窗口（秒）。"""
    global BROWSE_STREAM_BURST_WINDOW_SECONDS
    try:
        val = int(seconds)
    except (TypeError, ValueError):
        return
    if val > 0:
        BROWSE_STREAM_BURST_WINDOW_SECONDS = val


DEFAULT_NEW_SESSION_WINDOW_SECONDS = 8
DEFAULT_SEEK_WINDOW_SECONDS = 6
DEFAULT_PRIORITY_MODE = 'seek_first'
_VALID_PRIORITY_MODES = frozenset({'seek_first', 'new_first'})
_MAX_WINDOW_SECONDS = 30
_SESSION_STATE_GRACE_SECONDS = 120
_ACCUMULATOR_STALE_SECONDS = 30 * 60

_hydrated_instances: Set[str] = set()


def _keys_for_session(session: dict, *, credit_browse: bool = False) -> Set[str]:
    keys: Set[str] = set()
    if not isinstance(session, dict):
        return keys
    for key in _accumulator_key_candidates(session):
        if key:
            keys.add(key)
    persist = _persist_key_for_session(session)
    if persist:
        keys.add(persist)
    if credit_browse:
        try:
            from emby_lucky_verdict import browse_persist_key_for_session
            browse_key = browse_persist_key_for_session(session)
            if browse_key:
                keys.add(browse_key)
        except Exception:
            pass
    return keys


def collect_online_persist_keys(
    instance_name: str,
    sessions: list,
    *,
    credit_browse: bool = False,
) -> Set[str]:
    """收集当前在线外网会话对应的累加器键（含选片与受保护播放段）。"""
    name = (instance_name or '').strip()
    keys: Set[str] = set()
    if not name:
        return keys
    for raw in sessions or []:
        if not isinstance(raw, dict):
            continue
        prepared = raw
        if raw.get('NowPlayingItem') or raw.get('PlayState'):
            prepared = EmbyClient.normalize_session(raw)
        if is_wan_playback_session(prepared):
            keys.update(_keys_for_session(prepared, credit_browse=credit_browse))
        elif is_wan_remote_session(prepared) and EmbyClient.is_current_playback_session(
            prepared,
        ):
            keys.update(_keys_for_session(prepared, credit_browse=credit_browse))
        elif credit_browse and is_wan_remote_session(prepared):
            keys.update(_keys_for_session(prepared, credit_browse=True))
    try:
        import playback_record_store
        for sid in playback_record_store.protected_playback_session_ids(name):
            if sid:
                keys.add(f'sid:{sid}')
        for key, _ in playback_record_store.open_playing_upload_checkpoints(
            name,
        ).items():
            if key:
                keys.add(key)
    except Exception:
        pass
    if credit_browse:
        with _lock:
            browse_bucket = dict(_browse_upload_accumulators.get(name) or {})
        for bkey, amount in browse_bucket.items():
            if str(bkey or '').startswith('browse:') and int(amount or 0) > 0:
                keys.add(str(bkey).strip())
    return keys


def _playback_checkpoint_bytes(instance_name: str, active_keys: Set[str]) -> Dict[str, int]:
    if not active_keys:
        return {}
    try:
        import playback_record_store
        checkpoints = playback_record_store.open_playing_upload_checkpoints(
            instance_name,
        )
    except Exception:
        return {}
    return {
        key: val
        for key, val in checkpoints.items()
        if key in active_keys and int(val or 0) > 0
    }


def hydrate_live_upload_state(
    instance_name: str,
    sessions: list,
    *,
    credit_browse: bool = False,
) -> None:
    """服务重启后：仅恢复当前在线会话的播放/选片累加器与 Lucky 连接归属。"""
    name = (instance_name or '').strip()
    if not name or name in _hydrated_instances:
        return
    _hydrated_instances.add(name)
    active_keys = collect_online_persist_keys(
        name, sessions, credit_browse=credit_browse,
    )
    if not active_keys:
        return
    try:
        import emby_traffic_db
        db_upload = emby_traffic_db.load_session_upload_accumulators(name)
        db_browse = emby_traffic_db.load_browse_upload_accumulators(name)
        db_bindings = emby_traffic_db.load_lucky_conn_bindings(name)
    except Exception as e:
        logger.warning(f'[Emby:{name}] 读取会话流量持久化失败: {e}')
        return
    checkpoints = _playback_checkpoint_bytes(name, active_keys)
    restored_upload = 0
    restored_browse = 0
    restored_bindings = 0
    now_mono = time.monotonic()
    with _lock:
        upload_bucket = _upload_accumulators.setdefault(name, {})
        for key in active_keys:
            if key.startswith('browse:'):
                continue
            mem = max(0, int(upload_bucket.get(key) or 0))
            if key in checkpoints:
                val = max(mem, int(checkpoints.get(key) or 0))
            else:
                val = max(mem, int(db_upload.get(key) or 0))
            if val > 0:
                upload_bucket[key] = val
                _touch_accumulator_key(name, key, now_mono)
                restored_upload += 1
        browse_bucket = _browse_upload_accumulators.setdefault(name, {})
        for key in active_keys:
            if not key.startswith('browse:'):
                continue
            val = max(
                int(browse_bucket.get(key) or 0),
                int(db_browse.get(key) or 0),
            )
            if val > 0:
                browse_bucket[key] = val
                restored_browse += 1
        bindings = _conn_bindings.setdefault(name, {})
        for addr, pkey in db_bindings.items():
            if pkey in active_keys and addr:
                bindings[addr] = pkey
                restored_bindings += 1
    if restored_upload or restored_browse or restored_bindings:
        logger.info(
            f'[Emby:{name}] 会话流量续传: 播放键={restored_upload} '
            f'选片键={restored_browse} 连接归属={restored_bindings}',
        )


def sync_live_upload_persistence(
    instance_name: str,
    sessions: list,
    *,
    credit_browse: bool = False,
) -> None:
    """将当前在线会话的累加器与连接归属写入数据库。"""
    name = (instance_name or '').strip()
    if not name:
        return
    active_keys = collect_online_persist_keys(
        name, sessions, credit_browse=credit_browse,
    )
    with _lock:
        upload_bucket = dict(_upload_accumulators.get(name) or {})
        browse_bucket = dict(_browse_upload_accumulators.get(name) or {})
        bindings = dict(_conn_bindings.get(name) or {})
    filtered_upload = {
        k: v for k, v in upload_bucket.items()
        if k in active_keys and int(v or 0) > 0 and not k.startswith('browse:')
    }
    filtered_browse = {
        k: v for k, v in browse_bucket.items()
        if k in active_keys and int(v or 0) > 0
    }
    filtered_bindings = {
        addr: pkey for addr, pkey in bindings.items()
        if pkey in active_keys and str(addr).strip()
    }
    try:
        import emby_traffic_db
        emby_traffic_db.replace_session_upload_accumulators(name, filtered_upload)
        emby_traffic_db.replace_browse_upload_accumulators(name, filtered_browse)
        emby_traffic_db.replace_lucky_conn_bindings(name, filtered_bindings)
    except Exception as e:
        logger.debug(f'[Emby:{name}] 会话流量持久化失败: {e}')


def clear_persisted_live_upload_state(instance_name: str) -> None:
    """清空实例级会话流量持久化并允许下次重新 hydrate。"""
    name = (instance_name or '').strip()
    if not name:
        return
    _hydrated_instances.discard(name)
    try:
        import emby_traffic_db
        emby_traffic_db.clear_live_upload_persistence(name)
    except Exception as e:
        logger.debug(f'[Emby:{name}] 清空会话流量持久化失败: {e}')


def _delete_persisted_upload_keys(instance_name: str, keys: list) -> None:
    uniq = [str(k).strip() for k in (keys or []) if str(k).strip()]
    if not uniq:
        return
    playback_keys = [k for k in uniq if not k.startswith('browse:')]
    browse_keys = [k for k in uniq if k.startswith('browse:')]
    try:
        import emby_traffic_db
        if playback_keys:
            emby_traffic_db.delete_session_upload_accumulator_keys(
                instance_name, playback_keys,
            )
        if browse_keys:
            emby_traffic_db.delete_browse_upload_accumulator_keys(
                instance_name, browse_keys,
            )
    except Exception as e:
        logger.debug(
            f'[Emby:{instance_name}] 删除持久化累加器键失败: {e}',
        )


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
    _delete_persisted_upload_keys(name, [old_key])


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


def _protected_accumulator_keys(name: str) -> Set[str]:
    """open/待确认播放段：累加器不得因暂停无增量而过期清理。"""
    keys: Set[str] = set()
    if not name:
        return keys
    try:
        import playback_record_store
        for sid in playback_record_store.protected_playback_session_ids(name):
            if sid:
                keys.add(f'sid:{sid}')
        for key in playback_record_store.open_playing_upload_checkpoints(name):
            if key:
                keys.add(key)
    except Exception:
        pass
    return keys


def touch_playback_upload_keys(
    instance_name: str,
    subject: dict,
    *,
    now_mono: float = None,
) -> None:
    """刷新 open 播放段累加器 touch，避免长暂停被 stale 清理。"""
    name = (instance_name or '').strip()
    if not name or not subject:
        return
    now = time.monotonic() if now_mono is None else float(now_mono)
    with _lock:
        for key in _accumulator_key_candidates(subject):
            if key:
                _touch_accumulator_key(name, key, now)


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
    protected = _protected_accumulator_keys(name)
    runtime_sessions = (_allocator_runtime.get(name) or {}).get('sessions') or {}
    tick_bucket = _live_tick_uploads.get(name)
    removed_keys: List[str] = []
    for key in list(bucket.keys()):
        if key in protected:
            touched[key] = now_mono
            continue
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
        removed_keys.append(key)

    if removed_keys:
        _delete_persisted_upload_keys(name, removed_keys)

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


def _segment_traffic_key(record: dict) -> str:
    persist = playback_accumulator_key(record) or str(
        record.get('upload_accumulator_key') or '',
    ).strip()
    item_id = str(record.get('item_id') or '').strip()
    if persist and item_id:
        return f'{persist}|{item_id}'
    return persist or item_id


def _persist_key_for_session(session: dict) -> str:
    key = playback_accumulator_key(session)
    if key:
        return key
    sid = str(
        session.get('emby_session_id')
        or session.get('session_id')
        or session.get('id')
        or ''
    ).strip()
    if sid:
        return f'sid:{sid}'
    return ''


def _prune_lucky_conn_bindings(
    bindings: Dict[str, str],
    match_hints: Dict[str, str],
    active_keys: set,
    active_remote: list,
) -> None:
    valid_pkeys = {
        _persist_key_for_session(s)
        for s in (active_remote or [])
        if _persist_key_for_session(s)
    }
    for addr in list((bindings or {}).keys()):
        pkey = str(bindings.get(addr) or '').strip()
        if not pkey or pkey not in active_keys or pkey not in valid_pkeys:
            bindings.pop(addr, None)
    for addr in list((match_hints or {}).keys()):
        pkey = str(match_hints.get(addr) or '').strip()
        if not pkey or pkey not in active_keys:
            match_hints.pop(addr, None)


def _lucky_active_keys(
    active_remote: list,
    *,
    credit_browse: bool,
    browse_bucket: Optional[Dict[str, int]] = None,
) -> set:
    active_keys = {
        _persist_key_for_session(s)
        for s in (active_remote or [])
        if _persist_key_for_session(s)
    }
    if credit_browse:
        for session in active_remote or []:
            if is_wan_browse_session(session, credit_browse=True):
                for bkey in browse_persist_key_variants_for_session(session):
                    if bkey:
                        active_keys.add(bkey)
    for bkey, amount in (browse_bucket or {}).items():
        if str(bkey or '').startswith('browse:') and int(amount or 0) > 0:
            active_keys.add(str(bkey).strip())
    return active_keys


def clear_lucky_bindings_for_persist_key(instance_name: str, persist_key: str) -> None:
    """账户切换等场景：清除指向指定 persist_key 的连接绑定。"""
    name = (instance_name or '').strip()
    target = str(persist_key or '').strip()
    if not name or not target:
        return
    with _lock:
        runtime = _lucky_runtime(name)
        bindings: Dict[str, str] = runtime['bindings']
        hints: Dict[str, str] = runtime['match_hints']
        for addr in list(bindings.keys()):
            if str(bindings.get(addr) or '').strip() == target:
                bindings.pop(addr, None)
        for addr in list(hints.keys()):
            if str(hints.get(addr) or '').strip() == target:
                hints.pop(addr, None)


def _lucky_runtime(instance_name: str) -> dict:
    state = _conn_bindings.setdefault(instance_name, {})
    hints = _conn_match_hints.setdefault(instance_name, {})
    _segment_conn_baselines.setdefault(instance_name, {})
    _conn_info_cache.setdefault(instance_name, {})
    return {
        'bindings': state,
        'match_hints': hints,
        'segment_baselines': _segment_conn_baselines[instance_name],
        'conn_info': _conn_info_cache[instance_name],
    }


def _sync_lucky_match_hints(
    instance_name: str,
    sessions: list,
    analysis: dict,
) -> None:
    """同步跨 tick 连接匹配记忆，并清理已下线会话。"""
    name = (instance_name or '').strip()
    if not name:
        return
    active_remote = [
        s for s in (sessions or [])
        if isinstance(s, dict) and is_wan_remote_session(s)
    ]
    active_remote, _ = filter_superseded_wan_sessions(active_remote)
    active_keys = _lucky_active_keys(active_remote, credit_browse=False)
    runtime = _lucky_runtime(name)
    hints: Dict[str, str] = runtime['match_hints']
    for addr, pkey in match_hints_from_analysis(analysis).items():
        if pkey in active_keys:
            hints[addr] = pkey
    for addr in list(hints.keys()):
        pkey = str(hints.get(addr) or '').strip()
        if not pkey or pkey not in active_keys:
            hints.pop(addr, None)


def on_playback_segment_started(instance_name: str, record: dict) -> None:
    """新播放段：记录连接累计基线，连播换集后本段从零展示。"""
    name = (instance_name or '').strip()
    if not name or not record:
        return
    seg_key = _segment_traffic_key(record)
    if not seg_key:
        return
    with _lock:
        runtime = _lucky_runtime(name)
        runtime['segment_baselines'].setdefault(seg_key, {})


def on_playback_segment_finalized(instance_name: str, record: dict) -> None:
    name = (instance_name or '').strip()
    if not name or not record:
        return
    seg_key = _segment_traffic_key(record)
    if not seg_key:
        return
    with _lock:
        baselines = _segment_conn_baselines.get(name) or {}
        baselines.pop(seg_key, None)


def _apply_conn_deltas_to_accumulator(
    instance_name: str,
    conn_shares: Dict[str, int],
) -> int:
    name = (instance_name or '').strip()
    if not name or not conn_shares:
        return 0
    now_mono = time.monotonic()
    assigned = 0
    with _lock:
        wan_tick_uploads: Dict[str, int] = {}
        for persist_key, amount in conn_shares.items():
            amount = max(0, int(amount or 0))
            if amount <= 0 or not persist_key:
                continue
            wan_tick_uploads[persist_key] = (
                wan_tick_uploads.get(persist_key, 0) + amount
            )
            assigned += amount
        if wan_tick_uploads:
            bucket = _upload_accumulators.setdefault(name, {})
            for key, amount in wan_tick_uploads.items():
                bucket[key] = bucket.get(key, 0) + amount
                _touch_accumulator_key(name, key, now_mono)
        _set_live_tick_uploads(name, wan_tick_uploads)
    return assigned


def _apply_conn_deltas_to_browse_accumulator(
    instance_name: str,
    conn_shares: Dict[str, int],
) -> int:
    name = (instance_name or '').strip()
    if not name or not conn_shares:
        return 0
    now_mono = time.monotonic()
    assigned = 0
    with _lock:
        for persist_key, amount in conn_shares.items():
            amount = max(0, int(amount or 0))
            if amount <= 0 or not persist_key:
                continue
            bucket = _browse_upload_accumulators.setdefault(name, {})
            bucket[persist_key] = bucket.get(persist_key, 0) + amount
            assigned += amount
    return assigned


def _snapshot_browse_upload_buckets(name: str) -> Dict[str, int]:
    with _lock:
        return dict(_browse_upload_accumulators.get(name) or {})


def peek_browse_upload_bucket(name: str) -> Dict[str, int]:
    """选片累计桶快照（只读）。"""
    return _snapshot_browse_upload_buckets((name or '').strip())


def peek_browse_upload_bytes_for_sid(
    instance_name: str,
    sid: str,
    meta: Optional[dict] = None,
) -> int:
    """读取指定会话 sid 在选片累计桶中的字节（不清除）。"""
    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    if not name or not sid:
        return 0
    subject = dict(meta or {})
    subject.setdefault('id', sid)
    subject.setdefault('session_id', sid)
    subject.setdefault('emby_session_id', sid)
    keys = browse_persist_key_variants_for_session(subject)
    if not keys:
        legacy = legacy_browse_persist_key_for_session(subject)
        if legacy:
            keys = [legacy]
    bucket = peek_browse_upload_bucket(name)
    total = 0
    seen: Set[str] = set()
    for key in keys:
        seen.add(key)
        total += max(0, int(bucket.get(key) or 0))
    for key, amount in bucket.items():
        if key in seen:
            continue
        if sid_from_browse_persist_key(key) == sid:
            total += max(0, int(amount or 0))
    return total


def transfer_browse_bytes_to_play_for_session(
    instance_name: str,
    session: dict,
) -> int:
    """连播切集误入选片桶的字节转回播放累加器（不生成选片记录）。"""
    name = (instance_name or '').strip()
    if not name or not isinstance(session, dict):
        return 0
    from emby_lucky_verdict import (
        browse_persist_key_variants_for_session,
        persist_key_for_session,
        sid_from_browse_persist_key,
    )
    play_key = persist_key_for_session(session)
    if not play_key:
        return 0
    sid = str(
        session.get('emby_session_id')
        or session.get('session_id')
        or session.get('id')
        or '',
    ).strip()
    bucket = peek_browse_upload_bucket(name)
    keys_to_take: List[str] = []
    seen: Set[str] = set()
    for key in browse_persist_key_variants_for_session(session):
        if key and key in bucket and int(bucket.get(key) or 0) > 0:
            keys_to_take.append(key)
            seen.add(key)
    if sid:
        for key, amount in bucket.items():
            if key in seen or int(amount or 0) <= 0:
                continue
            if sid_from_browse_persist_key(key) == sid:
                keys_to_take.append(key)
                seen.add(key)
    moved = 0
    for key in keys_to_take:
        part = take_accumulated_browse_upload_by_key(name, key)
        if part is not None and int(part) > 0:
            moved += int(part)
    if moved <= 0:
        return 0
    now_mono = time.monotonic()
    with _lock:
        play_bucket = _upload_accumulators.setdefault(name, {})
        play_bucket[play_key] = play_bucket.get(play_key, 0) + moved
        _touch_accumulator_key(name, play_key, now_mono)
    logger.info(
        f'[Emby:{name}] 连播切集选片桶转播放 sid={sid or "?"} '
        f'bytes={moved} play_key={play_key}',
    )
    return moved


def remember_browse_session_meta(instance_name: str, sid: str, meta: dict) -> None:
    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    if not name or not sid or not isinstance(meta, dict):
        return
    user_name = str(meta.get('user_name') or '').strip()
    if not user_name:
        return
    with _lock:
        bucket = _browse_session_meta.setdefault(name, {})
        prev = bucket.get(sid) or {}
        bucket[sid] = {
            'user_name': user_name,
            'user_id': str(meta.get('user_id') or prev.get('user_id') or ''),
            'viewing_title': str(
                meta.get('viewing_title') or prev.get('viewing_title') or '',
            ),
            'series_name': str(
                meta.get('series_name') or prev.get('series_name') or '',
            ),
            'episode_label': str(
                meta.get('episode_label') or prev.get('episode_label') or '',
            ),
            'episode_title': str(
                meta.get('episode_title') or prev.get('episode_title') or '',
            ),
            'device_name': str(
                meta.get('device_name') or prev.get('device_name') or '',
            ),
            'client': str(meta.get('client') or prev.get('client') or ''),
            'client_ip': str(meta.get('client_ip') or prev.get('client_ip') or ''),
            'production_year': (
                meta.get('production_year')
                if meta.get('production_year') is not None
                else prev.get('production_year')
            ),
            'emby_session_id': sid,
            'session_id': sid,
            'id': sid,
        }


def get_browse_session_meta(instance_name: str, sid: str) -> dict:
    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    if not name or not sid:
        return {}
    with _lock:
        return dict((_browse_session_meta.get(name) or {}).get(sid) or {})


def pop_browse_session_meta(instance_name: str, sid: str) -> None:
    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    if not name or not sid:
        return
    with _lock:
        bucket = _browse_session_meta.get(name)
        if bucket:
            bucket.pop(sid, None)
            if not bucket:
                _browse_session_meta.pop(name, None)


def _browse_sid_from_key(key: str) -> str:
    return sid_from_browse_persist_key(key)


def _session_for_sid(sessions: list, sid: str) -> Optional[dict]:
    target = str(sid or '').strip()
    if not target:
        return None
    for raw in sessions or []:
        if not isinstance(raw, dict):
            continue
        if raw.get('NowPlayingItem') or raw.get('PlayState'):
            prepared = EmbyClient.normalize_session(raw)
        else:
            prepared = raw
        psid = str(
            prepared.get('id')
            or prepared.get('emby_session_id')
            or prepared.get('session_id')
            or '',
        ).strip()
        if psid == target:
            return prepared
    return None


def _meta_from_lucky_row(row: dict) -> dict:
    emby_user = str(row.get('emby_user') or '').strip()
    user_name = emby_user.split('·', 1)[0].strip() if emby_user else ''
    user_id = ''
    key = str(row.get('browse_persist_key') or '').strip()
    if key.startswith('browse:') and not key.startswith('browse:sid:'):
        parts = key.split(':')
        if len(parts) >= 3:
            user_id = str(parts[1] or '').strip()
    return {
        'user_name': user_name,
        'user_id': user_id,
        'viewing_title': str(row.get('media_label') or '').strip(),
    }


def _remember_browse_meta_from_analysis(instance_name: str, analysis: dict) -> None:
    name = (instance_name or '').strip()
    if not name or not isinstance(analysis, dict):
        return
    for row in analysis.get('rows') or []:
        if str(row.get('billing_state') or '') != 'browse_credited':
            continue
        key = str(row.get('browse_persist_key') or '').strip()
        sid = _browse_sid_from_key(key)
        if not sid:
            continue
        meta = _meta_from_lucky_row(row)
        if meta.get('user_name'):
            remember_browse_session_meta(name, sid, meta)


def _wan_pool_sessions(sessions: list, *, credit_browse: bool) -> list:
    if credit_browse:
        return [
            s for s in (sessions or [])
            if isinstance(s, dict) and is_wan_remote_session(s)
        ]
    return [
        s for s in (sessions or [])
        if isinstance(s, dict) and is_wan_playback_session(s)
    ]


def take_accumulated_browse_upload_by_key(
    instance_name: str,
    persist_key: str,
) -> Optional[int]:
    """读取并清除指定选片累计键的上行字节。"""
    name = (instance_name or '').strip()
    key = str(persist_key or '').strip()
    if not name or not key:
        return None
    with _lock:
        bucket = _browse_upload_accumulators.get(name) or {}
        value = bucket.pop(key, None)
        if not bucket:
            _browse_upload_accumulators.pop(name, None)
        # 选片键被结算/取走：同步清除其开播前突发计数，避免键复用时误移。
        burst_bucket = _browse_preplay_burst.get(name) or {}
        if key in burst_bucket:
            burst_bucket.pop(key, None)
            if not burst_bucket:
                _browse_preplay_burst.pop(name, None)
    if key:
        _delete_persisted_upload_keys(name, [key])
    if value is None:
        return None
    raw = max(0, int(value))
    return raw if raw > 0 else None


def take_accumulated_browse_upload(instance_name: str, subject: dict) -> Optional[int]:
    """读取并清除与选片段匹配的累计上行。"""
    if not subject:
        return None
    key = str(subject.get('upload_accumulator_key') or '').strip()
    if not key:
        from emby_lucky_verdict import browse_persist_key_for_session
        key = browse_persist_key_for_session(subject)
    if not key:
        return None
    return take_accumulated_browse_upload_by_key(instance_name, key)


def _is_pre_play_stream_burst(
    session: Optional[dict],
    delta: int,
    tick_seconds: Optional[float],
) -> bool:
    """流量形态识别：会话尚未翻到 playing（仍 connected/viewing），但单 tick
    增量达到推流突发速率，判定为开播缓冲突发（Emby 会话状态滞后窗口）。"""
    if not session or not isinstance(session, dict):
        return False
    mode = str(session.get('session_mode') or '').strip()
    if mode not in ('connected', 'viewing'):
        return False
    tick_s = float(tick_seconds or 1.0)
    if tick_s <= 0:
        tick_s = 1.0
    rate = int(delta or 0) / tick_s
    return rate >= BROWSE_STREAM_BURST_BPS


def _tag_preplay_burst(name: str, browse_key: str, delta: int) -> None:
    """记录计入选片桶的推流突发量（带时间戳，须在持有 _lock 时调用）。"""
    if not name or not browse_key or delta <= 0:
        return
    now = time.monotonic()
    bucket = _browse_preplay_burst.setdefault(name, {})
    entries = bucket.setdefault(browse_key, [])
    entries.append((now, int(delta)))
    # 丢弃过旧条目，避免长时间未结算的键无限增长。
    cutoff = now - _BURST_ENTRY_RETENTION_SECONDS
    if entries and entries[0][0] < cutoff:
        bucket[browse_key] = [e for e in entries if e[0] >= cutoff]


def settle_preplay_burst_to_play(instance_name: str, session: dict) -> int:
    """开播结算时，把「开播前突发窗口」秒内误计入选片桶的推流突发移回播放键累加器。

    在会话已转 playing 的安全时刻调用：仅回溯窗口内的突发归到播放段，窗口之外
    的更早突发保留在选片桶，作为真实选片流量结算。返回移动的字节数。"""
    name = (instance_name or '').strip()
    if not name or not isinstance(session, dict):
        return 0
    play_key = _persist_key_for_session(session)
    moved = 0
    now = time.monotonic()
    window_start = now - float(BROWSE_STREAM_BURST_WINDOW_SECONDS)
    with _lock:
        burst_bucket = _browse_preplay_burst.get(name) or {}
        if not burst_bucket:
            return 0
        browse_bucket = _browse_upload_accumulators.get(name) or {}
        sid = str(
            session.get('id') or session.get('session_id')
            or session.get('emby_session_id') or '',
        ).strip()
        for bkey in list(burst_bucket.keys()):
            if sid and _browse_sid_from_key(bkey) != sid:
                continue
            entries = burst_bucket.get(bkey) or []
            # 仅回溯开播前窗口内的突发，窗口之外的更早突发保留为选片。
            burst = sum(
                max(0, int(b)) for (ts, b) in entries if ts >= window_start
            )
            avail = max(0, int(browse_bucket.get(bkey) or 0))
            move = min(burst, avail)
            if move > 0 and play_key:
                browse_bucket[bkey] = avail - move
                acc = _upload_accumulators.setdefault(name, {})
                acc[play_key] = int(acc.get(play_key, 0)) + move
                _touch_accumulator_key(name, play_key, now)
                moved += move
            burst_bucket.pop(bkey, None)
        if not burst_bucket:
            _browse_preplay_burst.pop(name, None)
    return moved


def accumulate_wan_upload_by_conn(
    instance_name: str,
    sessions: list,
    conn_deltas: Dict[str, int],
    conn_rows: List[dict],
    *,
    tick_seconds: float = None,
    credit_browse: bool = False,
    ip_deltas: Dict[str, int] = None,
) -> dict:
    """Lucky：按 ConnsStatistics 连接级增量归属到外网会话。

    ip_deltas 为同一 tick 的 IP 级 TrafficOut 增量（权威总量，含在两次轮询
    之间开启又关闭、未出现在 ConnsStatistics 中的短连接）。连接级增量之和
    通常 <= IP 级总量，二者之差即"丢失的短连接流量"，在此按 IP 补给该 IP
    下唯一入账（正在推流）的会话，避免长视频某几段被系统性少计。
    """
    name = (instance_name or '').strip()
    deltas = {
        str(addr).strip(): max(0, int(v or 0))
        for addr, v in (conn_deltas or {}).items()
        if str(addr).strip() and int(v or 0) > 0
    }
    if not name or not deltas:
        return _allocation_debug_payload()

    active_remote = _wan_pool_sessions(sessions, credit_browse=credit_browse)
    active_remote, _ = filter_superseded_wan_sessions(active_remote)
    if not active_remote:
        return _allocation_debug_payload(
            total_upload_bytes=sum(deltas.values()),
            remainder_bytes=sum(deltas.values()),
        )

    assigned_total = 0
    remainder_total = 0
    merged = _allocation_debug_payload()
    conn_shares: Dict[str, int] = {}
    browse_shares: Dict[str, int] = {}
    unassigned_by_ip: Dict[str, int] = {}
    conn_delta_by_ip: Dict[str, int] = {}
    credited_by_ip: Dict[str, Dict[str, int]] = {}

    with _lock:
        runtime = _lucky_runtime(name)
        bindings: Dict[str, str] = runtime['bindings']
        match_hints: Dict[str, str] = dict(runtime.get('match_hints') or {})
        upload_bucket = dict(_upload_accumulators.get(name) or {})
        browse_bucket = dict(_browse_upload_accumulators.get(name) or {})
        for conn in conn_rows or []:
            addr = str(conn.get('remote_addr') or '').strip()
            if addr:
                runtime['conn_info'][addr] = dict(conn)

        active_keys = _lucky_active_keys(
            active_remote,
            credit_browse=credit_browse,
            browse_bucket=browse_bucket,
        )
        _prune_lucky_conn_bindings(
            bindings, match_hints, active_keys, active_remote,
        )

        analysis = analyze_lucky_connections(
            sessions,
            conn_rows,
            deltas,
            bindings=bindings,
            match_hints=match_hints,
            upload_bucket=upload_bucket,
            browse_upload_bucket=browse_bucket,
            credit_browse=credit_browse,
            instance_name=name,
        )
        for addr, pkey in binding_targets_from_analysis(analysis).items():
            bindings[addr] = pkey
        _sync_lucky_match_hints(name, sessions, analysis)

        for row in analysis.get('rows') or []:
            addr = str(row.get('remote_addr') or '').strip()
            delta = max(0, int(deltas.get(addr) or 0))
            if delta <= 0:
                continue
            billing = str(row.get('billing_state') or '')
            pkey = str(row.get('billing_persist_key') or '').strip()
            browse_key = str(row.get('browse_persist_key') or '').strip()
            row_ip = str(row.get('ip') or parse_endpoint_ip(addr) or '').strip()
            if row_ip:
                conn_delta_by_ip[row_ip] = conn_delta_by_ip.get(row_ip, 0) + delta
            if billing == 'credited' and pkey:
                conn_shares[pkey] = conn_shares.get(pkey, 0) + delta
                if row_ip:
                    bucket = credited_by_ip.setdefault(row_ip, {})
                    bucket[pkey] = bucket.get(pkey, 0) + delta
            elif billing == 'browse_credited' and browse_key:
                browse_sid = _browse_sid_from_key(browse_key)
                stream_session = _session_for_sid(sessions, browse_sid)
                is_burst = _is_pre_play_stream_burst(
                    stream_session, delta, tick_seconds,
                )
                route_play = False
                if stream_session:
                    import emby_continuous_playback
                    route_play = emby_continuous_playback.should_route_browse_delta_to_play(
                        name, stream_session,
                    )
                if route_play:
                    play_key = _persist_key_for_session(stream_session)
                    if play_key:
                        conn_shares[play_key] = (
                            conn_shares.get(play_key, 0) + delta
                        )
                    else:
                        browse_shares[browse_key] = (
                            browse_shares.get(browse_key, 0) + delta
                        )
                else:
                    browse_shares[browse_key] = (
                        browse_shares.get(browse_key, 0) + delta
                    )
                    # 开播前 Emby 仍报 connected/viewing 但连接已推流缓冲：
                    # 照常计入选片桶，同时标记该突发量，待开播结算移回播放键。
                    if is_burst:
                        _tag_preplay_burst(name, browse_key, delta)
            else:
                ip = str(
                    row.get('ip') or parse_endpoint_ip(addr) or '',
                ).strip()
                if ip:
                    unassigned_by_ip[ip] = unassigned_by_ip.get(ip, 0) + delta
                else:
                    remainder_total += delta

    for ip, pool in unassigned_by_ip.items():
        pool = max(0, int(pool or 0))
        if pool <= 0:
            continue
        ip_sessions = [
            s for s in active_remote
            if parse_endpoint_ip(s.get('remote_endpoint') or '') == ip
        ]
        if not ip_sessions:
            remainder_total += pool
            continue
        infos = [
            {'key': _persist_key_for_session(s), 'bps': session_stream_bps(s)}
            for s in ip_sessions
        ]
        infos = [i for i in infos if i.get('key')]
        extra = _distribute_weighted(pool, infos)
        for key, amount in extra.items():
            conn_shares[key] = conn_shares.get(key, 0) + amount

    # IP 级总量对账：把"IP 权威增量 - 连接级增量之和"的短缺补给该 IP 下
    # 唯一入账会话（丢失的短连接流量归位）。多会话或无入账会话时不猜测，
    # 交由 remainder 处理，避免误计。补给量以 IP 总量为界，不会双计。
    for ip, ip_total in (ip_deltas or {}).items():
        ip = str(ip or '').strip()
        ip_total = max(0, int(ip_total or 0))
        if not ip or ip_total <= 0:
            continue
        conn_sum = max(0, int(conn_delta_by_ip.get(ip, 0)))
        shortfall = ip_total - conn_sum
        if shortfall <= 0:
            continue
        credited_pkeys = credited_by_ip.get(ip) or {}
        if len(credited_pkeys) != 1:
            remainder_total += shortfall
            continue
        primary_pkey = next(iter(credited_pkeys))
        conn_shares[primary_pkey] = conn_shares.get(primary_pkey, 0) + shortfall

    assigned_total = _apply_conn_deltas_to_accumulator(name, conn_shares)
    assigned_total += _apply_conn_deltas_to_browse_accumulator(name, browse_shares)
    _remember_browse_meta_from_analysis(name, analysis)
    remainder_total += max(0, sum(deltas.values()) - assigned_total)

    # IP 级对账后归属量可能超过连接级增量之和（补入了短连接流量），
    # 因此总量取二者与余量之和的较大者，避免 remainder 出现负值。
    total = max(sum(deltas.values()), assigned_total + remainder_total)
    merged['total_upload_bytes'] = total
    merged['wan_upload_bytes'] = assigned_total
    merged['assigned_bytes'] = assigned_total
    merged['remainder_bytes'] = max(0, total - assigned_total)
    merged['program_remainder_bytes'] = remainder_total
    merged['wan_session_count'] = len(active_remote)
    return merged


def get_lucky_conn_debug_snapshot(
    instance_name: str,
    sessions: list,
    conn_rows: List[dict],
    conn_deltas: Dict[str, int],
    *,
    credit_browse: bool = False,
) -> dict:
    """调试：Lucky 连接统一裁决快照（按 IP 分组）。"""
    name = (instance_name or '').strip()
    if not name:
        return {
            'version': 2,
            'groups': [],
            'rows': [],
            'emby_without_lucky': [],
            'total_connections': 0,
        }
    deltas = {
        str(k).strip(): max(0, int(v or 0))
        for k, v in (conn_deltas or {}).items()
        if str(k).strip()
    }
    with _lock:
        bindings = dict(_conn_bindings.get(name) or {})
        match_hints = dict(_conn_match_hints.get(name) or {})
        upload_bucket = dict(_upload_accumulators.get(name) or {})
        browse_bucket = dict(_browse_upload_accumulators.get(name) or {})
    analysis = analyze_lucky_connections(
        sessions,
        conn_rows,
        deltas,
        bindings=bindings,
        match_hints=match_hints,
        upload_bucket=upload_bucket,
        browse_upload_bucket=browse_bucket,
        credit_browse=credit_browse,
        instance_name=name,
    )
    return analysis


def peek_accumulated_upload(instance_name: str, event: dict) -> Optional[int]:
    name = (instance_name or '').strip()
    if not name or not event:
        return None
    bucket, _ = _snapshot_upload_buckets(name)
    _, value = _resolve_bucket_upload(bucket, event)
    if value is None:
        return None
    return value


def annotate_live_sessions_upload(
    instance_name: str,
    sessions: list,
    *,
    lucky_ip_traffic: Dict[str, Dict[str, int]] = None,
    lucky_ip_tick_deltas: Dict[str, int] = None,
) -> List[dict]:
    """给实时会话附加本段上行累计与本轮新增（统一读会话累加器）。"""
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

        if (
            name
            and session.get('is_remote')
            and EmbyClient.is_current_playback_session(session)
        ):
            live_key, upload_live = _resolve_bucket_upload(upload_bucket, session)
            if upload_live is None:
                upload_live = 0
            upload_1s = 0
            if not bool(session.get('is_paused')):
                _, upload_1s = _resolve_bucket_upload(
                    tick_bucket, session, preferred_key=live_key or '',
                )
                if upload_1s is None:
                    upload_1s = 0

            upload_floor = max(0, int(session.get('estimated_upload_bytes_floor') or 0))
            session['estimated_upload_bytes_live'] = max(upload_live, upload_floor)
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
        # 选片累计桶不在此清空：选片会话断开后仍需保留累计字节，
        # 等待结算器在宽限期后读取入库（由 take_accumulated_browse_upload_by_key 按键清除）。
        _live_tick_uploads.pop(name, None)
        _accumulator_touch_mono.pop(name, None)
        _allocator_runtime.pop(name, None)
        _conn_bindings.pop(name, None)
        _conn_match_hints.pop(name, None)
        _segment_conn_baselines.pop(name, None)
        _conn_info_cache.pop(name, None)
        # 选片会话 meta 不在此清空：断开宽限期内结算器仍需用户名。
    # 选片结算器跟踪状态不在此清空：无会话时正是断开/离开选片后的入库宽限期。


def purge_user_state(
    instance_name: str,
    user_name: str,
    *,
    user_ids: list = None,
) -> None:
    """Emby 用户删除后，清理内存中的分摊/选片累计与连接归属。"""
    from emby_lucky_verdict import persist_key_belongs_to_user

    name = (instance_name or '').strip()
    user_fold = str(user_name or '').strip().casefold()
    if not name or not user_fold:
        return
    uid_set = {
        str(uid or '').strip()
        for uid in (user_ids or [])
        if str(uid or '').strip()
    }
    with _lock:
        for bucket in (_upload_accumulators.get(name) or {}).copy().items():
            key = str(bucket[0] or '').strip()
            if persist_key_belongs_to_user(key, user_fold, uid_set):
                (_upload_accumulators.get(name) or {}).pop(key, None)
        browse_bucket = _upload_accumulators.get(name) or {}
        for key in list(browse_bucket.keys()):
            if persist_key_belongs_to_user(str(key or '').strip(), user_fold, uid_set):
                browse_bucket.pop(key, None)
        tick_bucket = _live_tick_uploads.get(name) or {}
        for key in list(tick_bucket.keys()):
            if persist_key_belongs_to_user(str(key or '').strip(), user_fold, uid_set):
                tick_bucket.pop(key, None)
        bindings = _conn_bindings.get(name) or {}
        for addr, pkey in list(bindings.items()):
            if persist_key_belongs_to_user(str(pkey or '').strip(), user_fold, uid_set):
                bindings.pop(addr, None)
        hints = _conn_match_hints.get(name) or {}
        for addr, pkey in list(hints.items()):
            if persist_key_belongs_to_user(str(pkey or '').strip(), user_fold, uid_set):
                hints.pop(addr, None)
        for store in (_browse_session_meta,):
            inst_map = store.get(name) or {}
            for sid, meta in list(inst_map.items()):
                meta_name = str((meta or {}).get('user_name') or '').strip().casefold()
                meta_uid = str((meta or {}).get('user_id') or '').strip()
                if meta_name == user_fold or (meta_uid and meta_uid in uid_set):
                    inst_map.pop(sid, None)


def purge_stopped_wan_live_upload_state(instance_name: str, sessions: list) -> None:
    """停止播放后清理实时分摊桶；open/待确认记录存活期间不得清桶。"""
    name = (instance_name or '').strip()
    if not name:
        return
    from emby_client import EmbyClient
    try:
        import playback_record_store
        protected_sids = playback_record_store.protected_playback_session_ids(name)
    except Exception:
        protected_sids = set()
    for raw in sessions or []:
        if not isinstance(raw, dict):
            continue
        prepared = raw
        if raw.get('NowPlayingItem') or raw.get('PlayState'):
            prepared = EmbyClient.normalize_session(raw)
        if EmbyClient.is_current_playback_session(prepared):
            continue
        if not EmbyClient.session_has_now_playing_media(prepared):
            continue
        sid = str(
            prepared.get('id')
            or raw.get('emby_session_id')
            or raw.get('id')
            or raw.get('Id')
            or raw.get('session_id')
            or ''
        ).strip()
        endpoint = (
            prepared.get('remote_endpoint')
            or raw.get('remote_endpoint')
            or raw.get('RemoteEndPoint')
            or ''
        ).strip()
        remote_ok = bool(prepared.get('is_remote') or raw.get('is_remote'))
        if not remote_ok and endpoint:
            from emby_traffic_filter import is_wan_endpoint
            remote_ok = is_wan_endpoint(endpoint)
        if not remote_ok:
            continue
        if sid and sid in protected_sids:
            try:
                import playback_record_store
                playback_record_store.checkpoint_stopped_session_upload(name, prepared)
            except Exception as e:
                logger.debug(
                    f'[Emby:{name}] 停止播放流量刷入失败 sid={sid}: {e}',
                )
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
                _delete_persisted_upload_keys(name, [key])
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
    if key:
        _delete_persisted_upload_keys(name, [key])
    if value is None:
        return None
    raw = max(0, int(value))
    if raw <= 0:
        return None
    return raw


def _assign_lucky_ip_upload(instance_name: str, sessions: list, delta_up: int) -> dict:
    """Lucky 回退路径：同 IP 多会话时按码率权重分摊。"""
    name = (instance_name or '').strip()
    delta_up = max(0, int(delta_up or 0))
    active = _active_upload_sessions(sessions)
    if not name or delta_up <= 0 or not active:
        return _allocation_debug_payload(
            total_upload_bytes=delta_up,
            remainder_bytes=delta_up,
        )

    infos = [
        {'key': _persist_key_for_session(s), 'bps': session_stream_bps(s)}
        for s in active
    ]
    infos = [i for i in infos if i.get('key')]
    shares = _distribute_weighted(delta_up, infos)
    assigned = _apply_conn_deltas_to_accumulator(name, shares)
    return _allocation_debug_payload(
        total_upload_bytes=delta_up,
        wan_upload_bytes=assigned,
        assigned_bytes=assigned,
        remainder_bytes=max(0, delta_up - assigned),
        target_session_count=len(active),
        wan_session_count=len(active),
    )


def accumulate_wan_upload_by_ip(
    instance_name: str,
    sessions: list,
    ip_deltas: Dict[str, int],
    *,
    tick_seconds: float = None,
) -> dict:
    """Lucky 模式：按客户端 IP 增量分摊到对应外网会话。"""
    name = (instance_name or '').strip()
    deltas = {
        str(ip).strip(): max(0, int(v or 0))
        for ip, v in (ip_deltas or {}).items()
        if str(ip).strip() and int(v or 0) > 0
    }
    if not name or not deltas:
        return _allocation_debug_payload()
    active_remote = [
        s for s in (sessions or [])
        if isinstance(s, dict) and is_wan_playback_session(s)
    ]
    if not active_remote:
        return _allocation_debug_payload(
            total_upload_bytes=sum(deltas.values()),
            remainder_bytes=sum(deltas.values()),
        )

    assigned_total = 0
    remainder_total = 0
    merged_debug = _allocation_debug_payload()
    matched_ips = set()
    for ip, delta in deltas.items():
        ip_sessions = [
            s for s in active_remote
            if parse_endpoint_ip(s.get('remote_endpoint') or '') == ip
        ]
        if not ip_sessions:
            continue
        matched_ips.add(ip)
        part = _assign_lucky_ip_upload(name, ip_sessions, delta)
        assigned = int(
            part.get('wan_upload_bytes') or part.get('assigned_bytes') or 0,
        )
        assigned_total += assigned
        remainder_total += max(0, int(part.get('remainder_bytes') or 0))
        merged_debug['wan_upload_bytes'] = (
            int(merged_debug.get('wan_upload_bytes') or 0) + assigned
        )
        merged_debug['assigned_bytes'] = (
            int(merged_debug.get('assigned_bytes') or 0) + assigned
        )

    unmatched_delta = sum(
        v for ip, v in deltas.items() if ip not in matched_ips
    )
    if unmatched_delta > 0 and active_remote:
        fallback_sessions = active_remote
        if len(deltas) == 1 and len(active_remote) == 1:
            fallback_sessions = active_remote
        elif not matched_ips:
            fallback_sessions = active_remote
        else:
            fallback_sessions = [
                s for s in active_remote
                if parse_endpoint_ip(s.get('remote_endpoint') or '') not in matched_ips
            ] or active_remote
        part = _assign_lucky_ip_upload(name, fallback_sessions, unmatched_delta)
        assigned = int(
            part.get('wan_upload_bytes') or part.get('assigned_bytes') or 0,
        )
        assigned_total += assigned
        remainder_total += max(0, int(part.get('remainder_bytes') or 0))
        merged_debug['wan_upload_bytes'] = (
            int(merged_debug.get('wan_upload_bytes') or 0) + assigned
        )
        merged_debug['assigned_bytes'] = (
            int(merged_debug.get('assigned_bytes') or 0) + assigned
        )

    total = sum(deltas.values())
    merged_debug['total_upload_bytes'] = total
    merged_debug['wan_pool_bytes'] = assigned_total
    merged_debug['remainder_bytes'] = max(0, total - assigned_total)
    merged_debug['program_remainder_bytes'] = remainder_total
    return merged_debug
