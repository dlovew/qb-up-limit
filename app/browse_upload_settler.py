"""选片流量结算：跟随 Lucky 选片入账状态，连接结束或离开选片时写入 SQLite。"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from emby_client import EmbyClient
from emby_lucky_verdict import browse_persist_key_for_session
from emby_traffic_filter import parse_endpoint_ip

logger = logging.getLogger(__name__)

BROWSE_CONN_END_GRACE_SECONDS = 5
ORPHAN_BUCKET_MIN_AGE_SECONDS = 30
DEFAULT_MIN_BROWSE_UPLOAD_BYTES = 1024 * 1024
OFFLINE_TIMEOUT_SECONDS = 5 * 60
_BROWSE_KEY_PREFIX = 'browse:sid:'
SETTLER_VERSION = 'emby-primary-v3'

_lock = threading.RLock()
_active_browse_keys: Dict[str, Set[str]] = {}
_browse_key_gap_since: Dict[str, Dict[str, float]] = {}
_browse_key_meta: Dict[str, Dict[str, dict]] = {}
_last_session_modes: Dict[str, Dict[str, str]] = {}
_orphan_bucket_since: Dict[str, Dict[str, float]] = {}
_offline_since: Dict[str, Optional[float]] = {}
_next_segment_id: Dict[str, int] = {}
_tick_logged: Set[str] = set()
_tick_min_upload_bytes: int = DEFAULT_MIN_BROWSE_UPLOAD_BYTES


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '0000Z'


def _remember_key_meta(instance_name: str, key: str, meta: dict) -> None:
    name = (instance_name or '').strip()
    key = str(key or '').strip()
    user_name = str((meta or {}).get('user_name') or '').strip()
    if not name or not key or not user_name:
        return
    with _lock:
        bucket = _browse_key_meta.setdefault(name, {})
        bucket[key] = {
            'user_name': user_name,
            'user_id': str(meta.get('user_id') or ''),
            'viewing_title': str(meta.get('viewing_title') or ''),
            'series_name': str(meta.get('series_name') or ''),
            'episode_label': str(meta.get('episode_label') or ''),
            'episode_title': str(meta.get('episode_title') or ''),
            'device_name': str(meta.get('device_name') or ''),
            'client': str(meta.get('client') or ''),
            'client_ip': str(meta.get('client_ip') or ''),
            'production_year': meta.get('production_year'),
            'emby_session_id': str(
                meta.get('emby_session_id') or meta.get('session_id') or meta.get('id') or '',
            ),
        }


def _pop_key_meta(instance_name: str, key: str) -> None:
    name = (instance_name or '').strip()
    key = str(key or '').strip()
    if not name or not key:
        return
    with _lock:
        bucket = _browse_key_meta.get(name)
        if bucket:
            bucket.pop(key, None)
            if not bucket:
                _browse_key_meta.pop(name, None)


def _meta_for_browse_key(instance_name: str, key: str) -> dict:
    name = (instance_name or '').strip()
    key = str(key or '').strip()
    if not name or not key:
        return {}
    with _lock:
        return dict((_browse_key_meta.get(name) or {}).get(key) or {})


def _cache_meta_from_analysis(instance_name: str, analysis: dict) -> None:
    name = (instance_name or '').strip()
    if not name:
        return
    import emby_playback_traffic

    for row in (analysis or {}).get('rows') or []:
        if str(row.get('billing_state') or '') != 'browse_credited':
            continue
        key = str(row.get('browse_persist_key') or '').strip()
        if not key:
            continue
        sid = _sid_from_browse_key(key)
        meta = _meta_from_lucky_row(row)
        if meta.get('user_name'):
            _remember_key_meta(name, key, meta)
            if sid:
                emby_playback_traffic.remember_browse_session_meta(name, sid, meta)


def _prepare_session(session: dict) -> dict:
    if session.get('NowPlayingItem') or session.get('PlayState') or session.get('NowViewingItem'):
        return EmbyClient.normalize_session(session)
    return session


def _session_sid(session: dict) -> str:
    return str(
        session.get('emby_session_id')
        or session.get('session_id')
        or session.get('id')
        or session.get('Id')
        or '',
    ).strip()


def _sid_from_browse_key(key: str) -> str:
    text = str(key or '').strip()
    if text.startswith(_BROWSE_KEY_PREFIX):
        return text[len(_BROWSE_KEY_PREFIX):]
    return ''


def _has_browse_bytes(instance_name: str, sid: str, meta: Optional[dict] = None) -> bool:
    import emby_playback_traffic
    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    if not name or not sid:
        return False
    if emby_playback_traffic.peek_browse_upload_bytes_for_sid(name, sid) > 0:
        return True
    for key in _browse_keys_for_sid(sid, meta):
        bucket = emby_playback_traffic.peek_browse_upload_bucket(name)
        if int(bucket.get(key) or 0) > 0:
            return True
    return False


def _clear_browse_pending_key(instance_name: str, sid: str) -> None:
    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    if not name or not sid:
        return
    key = browse_persist_key_for_session({'id': sid})
    with _lock:
        gaps = _browse_key_gap_since.get(name)
        if gaps and key:
            gaps.pop(key, None)
        orphan = _orphan_bucket_since.get(name)
        if orphan and key:
            orphan.pop(key, None)


def _sync_emby_browse_transitions(
    instance_name: str,
    sessions: list,
    *,
    now_mono: float,
) -> None:
    """Emby 会话边沿：开播立即结选片、断会话立即结案。"""
    name = (instance_name or '').strip()
    if not name:
        return
    by_sid = _sessions_by_sid(sessions)
    with _lock:
        prev_modes = dict(_last_session_modes.get(name) or {})
    current_sids = set(by_sid.keys())
    prev_sids = set(prev_modes.keys())

    for sid in prev_sids - current_sids:
        if not _has_browse_bytes(name, sid):
            continue
        meta = _resolve_meta(name, sid, sessions, {})
        _clear_browse_pending_key(name, sid)
        _settle_browse_session(name, sid, meta, settle_reason='disconnect')

    for sid, session in by_sid.items():
        mode = str(session.get('session_mode') or '').strip()
        prev_mode = str(prev_modes.get(sid) or '').strip()
        if (
            prev_mode in ('viewing', 'connected')
            and mode in ('playing', 'paused')
            and _has_browse_bytes(name, sid, _meta_from_session(session))
        ):
            meta = _meta_from_session(session)
            if meta.get('user_name'):
                import emby_playback_traffic
                emby_playback_traffic.remember_browse_session_meta(name, sid, meta)
            _clear_browse_pending_key(name, sid)
            _settle_browse_session(
                name, sid, meta, settle_reason='playback_started',
            )

    with _lock:
        _last_session_modes[name] = {
            sid: str(sess.get('session_mode') or '').strip()
            for sid, sess in by_sid.items()
        }


def _browse_keys_for_sid(sid: str, meta: Optional[dict] = None) -> List[str]:
    sid = str(sid or '').strip()
    if not sid:
        return []
    meta = meta or {}
    candidates = [
        meta.get('emby_session_id'),
        meta.get('session_id'),
        sid,
        meta.get('id'),
    ]
    keys: List[str] = []
    for raw in candidates:
        text = str(raw or '').strip()
        if not text:
            continue
        key = browse_persist_key_for_session({'id': text})
        if key and key not in keys:
            keys.append(key)
    return keys or [f'{_BROWSE_KEY_PREFIX}{sid}']


def _sessions_by_sid(sessions: list) -> Dict[str, dict]:
    by_sid: Dict[str, dict] = {}
    for raw in sessions or []:
        if not isinstance(raw, dict):
            continue
        prepared = _prepare_session(raw)
        sid = _session_sid(prepared)
        if sid:
            by_sid[sid] = prepared
    return by_sid


def _browse_media_from_session(session: Optional[dict]) -> dict:
    if not session or not isinstance(session, dict):
        return {}
    prepared = _prepare_session(session)
    mode = str(prepared.get('session_mode') or '').strip()
    if mode == 'viewing' or str(prepared.get('viewing_title') or '').strip():
        series = str(prepared.get('viewing_series_name') or '').strip()
        label = str(prepared.get('viewing_episode_label') or '').strip()
        main = str(prepared.get('viewing_title') or '').strip()
    else:
        series = str(prepared.get('series_name') or '').strip()
        label = str(prepared.get('episode_label') or '').strip()
        main = str(
            prepared.get('episode_title') or prepared.get('title') or '',
        ).strip()
    endpoint = str(prepared.get('remote_endpoint') or '').strip()
    return {
        'series_name': series,
        'episode_label': label,
        'episode_title': main,
        'viewing_title': main or series,
        'production_year': prepared.get('production_year'),
    }


def _meta_from_session(session: Optional[dict]) -> dict:
    if not session or not isinstance(session, dict):
        return {}
    prepared = _prepare_session(session)
    sid = _session_sid(prepared)
    media = _browse_media_from_session(prepared)
    endpoint = str(prepared.get('remote_endpoint') or '').strip()
    meta = {
        'user_name': str(prepared.get('user_name') or '').strip(),
        'user_id': str(prepared.get('user_id') or '').strip(),
        'device_name': str(prepared.get('device_name') or '').strip(),
        'client': str(prepared.get('client') or '').strip(),
        'client_ip': parse_endpoint_ip(endpoint),
        **media,
    }
    if sid:
        meta['emby_session_id'] = sid
        meta['session_id'] = sid
        meta['id'] = sid
    return meta


def _meta_from_lucky_row(row: dict) -> dict:
    emby_user = str(row.get('emby_user') or '').strip()
    user_name = emby_user.split('·', 1)[0].strip() if emby_user else ''
    return {
        'user_name': user_name,
        'user_id': '',
        'viewing_title': str(row.get('media_label') or '').strip(),
    }


def _resolve_meta(instance_name: str, sid: str, sessions: list, analysis: dict) -> dict:
    import emby_playback_traffic

    name = (instance_name or '').strip()
    sid = str(sid or '').strip()
    by_sid = {}
    for raw in sessions or []:
        if not isinstance(raw, dict):
            continue
        prepared = _prepare_session(raw)
        psid = _session_sid(prepared)
        if psid:
            by_sid[psid] = prepared
    if sid in by_sid:
        meta = _meta_from_session(by_sid[sid])
        if meta.get('user_name'):
            emby_playback_traffic.remember_browse_session_meta(name, sid, meta)
            return meta
    stored = emby_playback_traffic.get_browse_session_meta(name, sid)
    if stored.get('user_name'):
        return stored
    for key in _browse_keys_for_sid(sid):
        cached = _meta_for_browse_key(name, key)
        if cached.get('user_name'):
            return cached
    for row in (analysis or {}).get('rows') or []:
        key = str(row.get('browse_persist_key') or '').strip()
        if _sid_from_browse_key(key) != sid:
            continue
        meta = _meta_from_lucky_row(row)
        if meta.get('user_name'):
            emby_playback_traffic.remember_browse_session_meta(name, sid, meta)
            return meta
    return {}


def _alloc_browse_segment_id(instance_name: str) -> int:
    name = (instance_name or '').strip()
    with _lock:
        if name not in _next_segment_id:
            import emby_traffic_db
            emby_traffic_db._ensure_emby_schema()
            import traffic_db
            conn = traffic_db.get_conn()
            try:
                c = conn.cursor()
                c.execute(
                    'SELECT COALESCE(MAX(segment_id), 0) FROM emby_browse_upload_facts'
                    ' WHERE instance_name = ?',
                    (name,),
                )
                row = c.fetchone()
                _next_segment_id[name] = int(row[0] or 0) + 1
            finally:
                conn.close()
        seg_id = _next_segment_id[name]
        _next_segment_id[name] = seg_id + 1
        return seg_id


def _settle_browse_session(
    instance_name: str,
    sid: str,
    meta: dict,
    *,
    settle_reason: str,
) -> None:
    name = (instance_name or '').strip()
    if not name or not sid:
        return
    import emby_playback_traffic
    import emby_traffic_db

    upload = None
    used_key = ''
    for key in _browse_keys_for_sid(sid, meta):
        upload = emby_playback_traffic.take_accumulated_browse_upload_by_key(name, key)
        if upload is not None and int(upload) > 0:
            used_key = key
            break

    if upload is None or int(upload) <= 0:
        peek = emby_playback_traffic.peek_browse_upload_bytes_for_sid(name, sid)
        logger.debug(
            f'[Browse:{name}] 选片结算跳过 sid={sid} reason={settle_reason} '
            f'bytes=0 peek={peek} user={meta.get("user_name") or "?"}',
        )
        return

    user_name = str(meta.get('user_name') or '').strip()
    if not user_name:
        logger.debug(
            f'[Browse:{name}] 选片结算跳过 sid={sid} reason={settle_reason} '
            f'bytes={upload} user=空 key={used_key}',
        )
        return

    upload_int = int(upload)
    min_bytes = max(0, int(_tick_min_upload_bytes))
    if upload_int < min_bytes:
        emby_playback_traffic.pop_browse_session_meta(name, sid)
        if used_key:
            _pop_key_meta(name, used_key)
        logger.debug(
            f'[Browse:{name}] 选片未达入账阈值 sid={sid} reason={settle_reason} '
            f'bytes={upload_int} min={min_bytes} user={user_name}',
        )
        return

    stopped_at = emby_traffic_db._now().strftime('%Y-%m-%d %H:%M:%S')
    ok = emby_traffic_db.save_browse_upload_fact(
        name,
        _alloc_browse_segment_id(name),
        user_name,
        meta.get('user_id') or '',
        stopped_at,
        upload_int,
        meta.get('viewing_title') or meta.get('episode_title') or '',
        settle_reason,
        series_name=meta.get('series_name') or '',
        episode_label=meta.get('episode_label') or '',
        episode_title=meta.get('episode_title') or '',
        device_name=meta.get('device_name') or '',
        client=meta.get('client') or '',
        client_ip=meta.get('client_ip') or '',
        production_year=meta.get('production_year'),
        min_upload_bytes=min_bytes,
    )
    if ok:
        emby_playback_traffic.pop_browse_session_meta(name, sid)
        if used_key:
            _pop_key_meta(name, used_key)
        logger.info(
            f'[Browse:{name}] 选片入库 sid={sid} bytes={upload} '
            f'reason={settle_reason} key={used_key}',
        )
    else:
        logger.warning(
            f'[Browse:{name}] 选片入库失败 sid={sid} bytes={upload} '
            f'reason={settle_reason} key={used_key}',
        )


def _current_browse_credit_keys(analysis: dict) -> Set[str]:
    keys: Set[str] = set()
    for row in (analysis or {}).get('rows') or []:
        if str(row.get('billing_state') or '') != 'browse_credited':
            continue
        key = str(row.get('browse_persist_key') or '').strip()
        if key:
            keys.add(key)
    return keys


def sync_browse_credit_from_analysis(
    instance_name: str,
    analysis: dict,
    sessions: list,
    *,
    now_mono: Optional[float] = None,
) -> None:
    """Lucky 连接从「选片入账」消失时，按会话 sid 结算累计桶。"""
    name = (instance_name or '').strip()
    if not name:
        return
    now_mono = time.monotonic() if now_mono is None else float(now_mono)
    _cache_meta_from_analysis(name, analysis)
    current = _current_browse_credit_keys(analysis)
    by_sid = _sessions_by_sid(sessions)
    with _lock:
        prev = set(_active_browse_keys.get(name) or set())
        dropped = prev - current
        gaps = _browse_key_gap_since.setdefault(name, {})

        for key in current:
            gaps.pop(key, None)
            orphan = _orphan_bucket_since.setdefault(name, {})
            orphan.pop(key, None)

        for key in dropped:
            sid = _sid_from_browse_key(key)
            if not sid:
                gaps.pop(key, None)
                continue
            session = by_sid.get(sid)
            if session is None:
                gaps.pop(key, None)
                meta = _resolve_meta(name, sid, sessions, analysis)
                _settle_browse_session(name, sid, meta, settle_reason='disconnect')
                continue
            mode = str(session.get('session_mode') or '').strip()
            if mode in ('playing', 'paused'):
                gaps.pop(key, None)
                meta = _resolve_meta(name, sid, sessions, analysis)
                _settle_browse_session(
                    name, sid, meta, settle_reason='playback_started',
                )
                continue
            if key not in gaps:
                gaps[key] = now_mono
                logger.info(
                    f'[Browse:{name}] 选片连接结束待结算 key={key} sid={sid}',
                )

        for key, since in list(gaps.items()):
            sid = _sid_from_browse_key(key)
            if not sid:
                gaps.pop(key, None)
                continue
            session = by_sid.get(sid)
            if session is None:
                gaps.pop(key, None)
                meta = _resolve_meta(name, sid, sessions, analysis)
                _settle_browse_session(name, sid, meta, settle_reason='disconnect')
                continue
            mode = str(session.get('session_mode') or '').strip()
            if mode in ('playing', 'paused'):
                gaps.pop(key, None)
                meta = _resolve_meta(name, sid, sessions, analysis)
                _settle_browse_session(
                    name, sid, meta, settle_reason='playback_started',
                )
                continue
            if now_mono - float(since) < BROWSE_CONN_END_GRACE_SECONDS:
                continue
            gaps.pop(key, None)
            meta = _resolve_meta(name, sid, sessions, analysis)
            _settle_browse_session(name, sid, meta, settle_reason='browse_conn_end')

        _active_browse_keys[name] = current


def _settle_orphan_buckets(
    instance_name: str,
    sessions: list,
    analysis: dict,
    *,
    now_mono: float,
) -> None:
    """兜底：累计桶仍有字节、且已脱管超过阈值时入库。"""
    import emby_playback_traffic

    name = (instance_name or '').strip()
    if not name:
        return
    active = _current_browse_credit_keys(analysis)
    bucket = emby_playback_traffic.peek_browse_upload_bucket(name)
    with _lock:
        gaps = dict(_browse_key_gap_since.get(name) or {})
        orphan_since = _orphan_bucket_since.setdefault(name, {})

    for key, amount in bucket.items():
        if int(amount or 0) <= 0 or key in active:
            continue
        if key in gaps:
            continue
        sid = _sid_from_browse_key(key)
        if not sid:
            continue
        first_seen = orphan_since.get(key)
        if first_seen is None:
            orphan_since[key] = now_mono
            continue
        if now_mono - float(first_seen) < ORPHAN_BUCKET_MIN_AGE_SECONDS:
            continue
        meta = _resolve_meta(name, sid, sessions, analysis)
        if not meta.get('user_name'):
            continue
        orphan_since.pop(key, None)
        _settle_browse_session(name, sid, meta, settle_reason='orphan_bucket')


def _handle_offline(instance_name: str, now_mono: float) -> None:
    bucket = _offline_since
    if bucket.get(instance_name) is None:
        bucket[instance_name] = now_mono
    elapsed = now_mono - float(bucket[instance_name])
    if elapsed < OFFLINE_TIMEOUT_SECONDS:
        return
    import emby_playback_traffic

    name = (instance_name or '').strip()
    browse_bucket = emby_playback_traffic.peek_browse_upload_bucket(name)
    for key, amount in list(browse_bucket.items()):
        if int(amount or 0) <= 0:
            continue
        sid = _sid_from_browse_key(key)
        if not sid:
            continue
        meta = emby_playback_traffic.get_browse_session_meta(name, sid)
        if not meta.get('user_name'):
            continue
        _settle_browse_session(name, sid, meta, settle_reason='timeout_offline')
    with _lock:
        _active_browse_keys.pop(name, None)
        _browse_key_gap_since.pop(name, None)
    bucket[instance_name] = now_mono


def tick(
    instance_name: str,
    sessions: list,
    *,
    api_online: bool = True,
    credit_enabled: bool = True,
    analysis: Optional[dict] = None,
    min_upload_bytes: Optional[int] = None,
) -> None:
    """每个采集 tick：同步 Lucky 选片入账状态并兜底结算。"""
    global _tick_min_upload_bytes
    if min_upload_bytes is not None:
        _tick_min_upload_bytes = max(0, int(min_upload_bytes))
    name = (instance_name or '').strip()
    if not name or not credit_enabled:
        return

    now_mono = time.monotonic()
    with _lock:
        if name not in _tick_logged:
            _tick_logged.add(name)
            logger.info(
                f'[Browse:{name}] 选片结算器已加载 ({SETTLER_VERSION})',
            )
        if not api_online:
            _handle_offline(name, now_mono)
            return
        _offline_since[name] = None

    _sync_emby_browse_transitions(name, sessions, now_mono=now_mono)
    if isinstance(analysis, dict):
        sync_browse_credit_from_analysis(
            name, analysis, sessions, now_mono=now_mono,
        )
        _settle_orphan_buckets(
            name, sessions, analysis, now_mono=now_mono,
        )


def flush_instance(instance_name: str, *, settle_reason: str = 'instance_reset') -> None:
    name = (instance_name or '').strip()
    if not name:
        return
    import emby_playback_traffic

    browse_bucket = emby_playback_traffic.peek_browse_upload_bucket(name)
    for key, amount in list(browse_bucket.items()):
        if int(amount or 0) <= 0:
            continue
        sid = _sid_from_browse_key(key)
        if not sid:
            continue
        meta = _meta_for_browse_key(name, key)
        if not meta.get('user_name'):
            meta = emby_playback_traffic.get_browse_session_meta(name, sid)
        if not meta.get('user_name'):
            continue
        _settle_browse_session(name, sid, meta, settle_reason=settle_reason)


def clear_instance(instance_name: str, *, flush: bool = True) -> None:
    name = (instance_name or '').strip()
    if not name:
        return
    if flush:
        flush_instance(name, settle_reason='instance_reset')
    with _lock:
        _active_browse_keys.pop(name, None)
        _browse_key_gap_since.pop(name, None)
        _browse_key_meta.pop(name, None)
        _last_session_modes.pop(name, None)
        _orphan_bucket_since.pop(name, None)
        _offline_since.pop(name, None)
        _next_segment_id.pop(name, None)
