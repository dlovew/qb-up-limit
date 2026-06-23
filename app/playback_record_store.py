"""自写播放段记录：以 /Sessions 为真相源，热更新 JSON。"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import emby_playback_upload_sync
import emby_watch_progress
from emby_client import EmbyClient
from emby_storage_paths import (
    EMBY_EVENTS_DIR,
    legacy_playback_record_store_path,
    playback_record_store_path,
)
from secrets_store import _read_json, _write_json

logger = logging.getLogger(__name__)

MAX_STORED_RECORDS = 500
OFFLINE_TIMEOUT_SECONDS = 30 * 60

_lock = threading.RLock()
_runtime: Dict[str, dict] = {}

_PLAYBACK_STATUSES = frozenset({'playing', 'ended', 'incomplete'})


def _store_path(instance_name: str) -> str:
    return playback_record_store_path(instance_name)


def _is_activity_log_store(data: dict) -> bool:
    return isinstance(data, dict) and isinstance(data.get('events'), list) and not isinstance(
        data.get('records'), list,
    )


def _is_playback_record_store(data: dict) -> bool:
    return isinstance(data, dict) and isinstance(data.get('records'), list)


def _migrate_instance_store(instance_name: str) -> None:
    path = _store_path(instance_name)
    legacy = legacy_playback_record_store_path(instance_name)
    if os.path.isfile(path):
        data = _read_json(path, {})
        if _is_activity_log_store(data):
            try:
                os.remove(path)
                logger.info(f'[Playback:{instance_name}] 已删除旧活动日志 JSON: {path}')
            except OSError as e:
                logger.warning(f'[Playback:{instance_name}] 删除旧活动日志失败: {e}')
    if os.path.isfile(legacy):
        if not os.path.isfile(path):
            try:
                os.rename(legacy, path)
                logger.info(f'[Playback:{instance_name}] 已迁移播放记录: {legacy} -> {path}')
            except OSError as e:
                logger.warning(f'[Playback:{instance_name}] 迁移 *_2.json 失败: {e}')
        else:
            legacy_data = _read_json(legacy, {})
            if _is_playback_record_store(legacy_data):
                try:
                    os.remove(legacy)
                    logger.info(f'[Playback:{instance_name}] 已删除重复 *_2.json: {legacy}')
                except OSError as e:
                    logger.warning(f'[Playback:{instance_name}] 删除 *_2.json 失败: {e}')


_migrated_instances: Set[str] = set()


def _ensure_migrated(instance_name: str) -> None:
    if not instance_name:
        return
    with _lock:
        if instance_name in _migrated_instances:
            return
        _migrate_instance_store(instance_name)
        _migrated_instances.add(instance_name)


def _migrate_all_stores_once() -> None:
    if not os.path.isdir(EMBY_EVENTS_DIR):
        return
    names = set()
    for fname in os.listdir(EMBY_EVENTS_DIR):
        if fname.endswith('_2.json'):
            data = _read_json(os.path.join(EMBY_EVENTS_DIR, fname), {})
            inst = (data.get('instance_name') or '').strip()
            if inst:
                names.add(inst)
        elif fname.endswith('.json'):
            data = _read_json(os.path.join(EMBY_EVENTS_DIR, fname), {})
            inst = (data.get('instance_name') or '').strip()
            if inst and _is_playback_record_store(data):
                names.add(inst)
            elif inst and _is_activity_log_store(data):
                names.add(inst)
    for inst in names:
        _ensure_migrated(inst)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '0000Z'


def _parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        text = str(s).strip()
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


def _load_store(instance_name: str) -> dict:
    _ensure_migrated(instance_name)
    path = _store_path(instance_name)
    default = {
        'instance_name': instance_name,
        'next_id': 1,
        'records': [],
    }
    if not os.path.isfile(path):
        return default
    data = _read_json(path, default)
    if not isinstance(data, dict):
        return default
    data.setdefault('instance_name', instance_name)
    data.setdefault('next_id', 1)
    if not isinstance(data.get('records'), list):
        data['records'] = []
    return data


def _save_store(store: dict) -> None:
    path = _store_path(store.get('instance_name') or '')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    records = store.get('records') or []
    store['records'] = records[:MAX_STORED_RECORDS]
    _write_json(path, store)


def _runtime_bucket(instance_name: str) -> dict:
    with _lock:
        bucket = _runtime.setdefault(instance_name, {})
        bucket.setdefault('offline_since', None)
        bucket.setdefault('was_api_online', True)
        return bucket


def _alloc_id(store: dict) -> int:
    rid = int(store.get('next_id') or 1)
    store['next_id'] = rid + 1
    return rid


def _segment_key(rec: dict) -> tuple:
    return (
        str(rec.get('user_id') or '').strip(),
        EmbyClient._normalize_client_key(rec.get('client') or ''),
        str(rec.get('item_id') or '').strip(),
    )


def _ended_record_sort_key(rec: dict) -> str:
    """已结束段列表排序：墙钟结束时间优先。"""
    return (
        rec.get('stopped_at') or rec.get('last_tick_at')
        or rec.get('started_at') or ''
    )


def _session_track_key(session: dict) -> tuple:
    return (
        str(session.get('user_id') or '').strip(),
        EmbyClient._normalize_client_key(session.get('client') or ''),
        str(session.get('item_id') or '').strip(),
    )


def _prepare_session(session: dict) -> dict:
    if session.get('NowPlayingItem') or session.get('PlayState'):
        return EmbyClient.normalize_session(session)
    return session


def _apply_session_meta(record: dict, session: dict) -> None:
    prepared = _prepare_session(session)
    EmbyClient.apply_playback_meta(record, prepared)
    endpoint = record.get('remote_endpoint') or prepared.get('remote_endpoint') or ''
    if endpoint and not record.get('client_ip'):
        from emby_traffic_filter import parse_endpoint_ip
        record['client_ip'] = parse_endpoint_ip(endpoint)
    for key in ('user_id', 'user_name', 'client', 'device_name'):
        val = prepared.get(key)
        if val not in (None, ''):
            record[key] = val
    runtime = int(prepared.get('runtime_seconds') or 0)
    if runtime > 0:
        record['runtime_seconds'] = runtime
    if not record.get('item_title'):
        record['item_title'] = (
            prepared.get('episode_title') or prepared.get('title') or ''
        )
    if not record.get('episode_title'):
        record['episode_title'] = prepared.get('episode_title') or record.get('item_title') or ''
    if not record.get('series_name'):
        record['series_name'] = prepared.get('series_name') or ''
    if not record.get('episode_label') and prepared.get('episode_label'):
        record['episode_label'] = prepared.get('episode_label')
    meta = EmbyClient.extract_playback_meta(prepared)
    for key in ('play_method', 'is_video_direct', 'is_audio_direct', 'transcode_kind'):
        if key in meta:
            record[key] = meta[key]
    if 'is_paused' in prepared:
        record['is_paused'] = bool(prepared.get('is_paused'))
    pos = prepared.get('position_seconds')
    if pos is not None and record.get('status') == 'playing':
        record['end_position_seconds'] = max(0, int(pos))
    if record.get('status') == 'playing' and not record.get('upload_accumulator_key'):
        from emby_traffic_filter import playback_accumulator_key
        acc_key = playback_accumulator_key(record)
        if acc_key:
            record['upload_accumulator_key'] = acc_key


def _apply_watch_snapshot(record: dict, snapshot: dict) -> None:
    if not snapshot:
        return
    overwrite = (
        bool(record.get('watch_fields_frozen'))
        or record.get('status') != 'playing'
    )
    emby_watch_progress.merge_watch_snapshot(record, snapshot, overwrite=overwrite)


def _resolve_upload_bytes(instance_name: str, record: dict) -> None:
    if not record.get('is_remote'):
        return
    if record.get('estimated_upload_bytes') is not None:
        return
    emby_playback_upload_sync.resolve_upload_bytes(
        instance_name,
        playback_record=record,
    )


def _take_upload(instance_name: str, record: dict) -> None:
    _resolve_upload_bytes(instance_name, record)


def _persist_upload_fact(instance_name: str, record: dict) -> None:
    if not record.get('is_remote'):
        return
    upload = record.get('estimated_upload_bytes')
    if upload is None or int(upload) <= 0:
        return
    try:
        import emby_traffic_db
        emby_traffic_db.save_playback_upload_fact(
            instance_name,
            int(record.get('id') or 0),
            record.get('user_name') or '',
            record.get('user_id') or '',
            record.get('stopped_at') or record.get('last_tick_at') or '',
            int(upload),
            record.get('series_name') or '',
            record.get('episode_label') or '',
        )
    except Exception as e:
        logger.debug(f'[Playback:{instance_name}] 用户上行入库失败: {e}')


def _estimate_played_wall_seconds(record: dict) -> Optional[int]:
    start_dt = _parse_iso(record.get('started_at') or '')
    stop_dt = _parse_iso(record.get('stopped_at') or '')
    if not start_dt or not stop_dt or stop_dt <= start_dt:
        return None
    return max(1, int((stop_dt - start_dt).total_seconds()))


def _finalize_record(instance_name: str, record: dict, *,
                     status: str, stopped_at: str = None,
                     interrupt_reason: str = None) -> None:
    if status not in _PLAYBACK_STATUSES or status == 'playing':
        return
    if record.get('status') != 'playing':
        return
    record['status'] = status
    if status == 'ended' and not interrupt_reason:
        record['stopped_at'] = stopped_at or _utc_now_iso()
    else:
        record['stopped_at'] = stopped_at or record.get('last_tick_at') or _utc_now_iso()
    if interrupt_reason:
        record['interrupt_reason'] = interrupt_reason
    snap = emby_watch_progress.snapshot_for_record(instance_name, record)
    if snap:
        _apply_watch_snapshot(record, snap)
    emby_watch_progress.finalize_watch_to_event(instance_name, record)
    if not record.get('played_seconds'):
        wall = _estimate_played_wall_seconds(record)
        if wall:
            record['played_seconds'] = wall
    start_pos = record.get('start_position_seconds')
    end_pos = record.get('end_position_seconds')
    if start_pos is not None and (end_pos is None or int(end_pos or 0) <= 0):
        played = int(record.get('played_seconds') or 0)
        if played > 0:
            record['end_position_seconds'] = max(0, int(start_pos)) + played
    _take_upload(instance_name, record)
    if status == 'ended' or (status == 'incomplete' and record.get('estimated_upload_bytes')):
        _persist_upload_fact(instance_name, record)
    emby_watch_progress.reset_tracker_for_record(instance_name, record)
    if record.get('emby_session_id'):
        bucket = _runtime_bucket(instance_name)
        bucket.pop(f'sid:{record["emby_session_id"]}', None)


def _new_record(instance_name: str, store: dict, session: dict) -> dict:
    prepared = _prepare_session(session)
    rid = _alloc_id(store)
    now = _utc_now_iso()
    record = {
        'id': rid,
        'instance_name': instance_name,
        'status': 'playing',
        'source': 'native',
        'emby_session_id': str(prepared.get('id') or ''),
        'started_at': now,
        'stopped_at': None,
        'last_tick_at': now,
        'interrupt_reason': None,
    }
    _apply_session_meta(record, prepared)
    emby_watch_progress.begin_pair_watch(instance_name, record)
    snap = emby_watch_progress.update_for_record(instance_name, record, prepared)
    _apply_watch_snapshot(record, snap)
    bucket = _runtime_bucket(instance_name)
    sid = record.get('emby_session_id')
    if sid:
        bucket[f'sid:{sid}'] = rid
    bucket[f'track:{_segment_key(record)}'] = rid
    return record


def _find_open_record(store: dict, session: dict,
                      instance_name: str) -> Optional[dict]:
    records = store.get('records') or []
    sid = str(session.get('id') or '')
    bucket = _runtime_bucket(instance_name)
    if sid:
        rid = bucket.get(f'sid:{sid}')
        if rid:
            for rec in records:
                if rec.get('id') == rid and rec.get('status') == 'playing':
                    return rec
    track = _session_track_key(session)
    rid = bucket.get(f'track:{track}')
    if rid:
        for rec in records:
            if rec.get('id') == rid and rec.get('status') == 'playing':
                return rec
    for rec in records:
        if rec.get('status') != 'playing':
            continue
        if _segment_key(rec) == track:
            return rec
    return None


def enrich_sessions_playback_started_at(instance_name: str,
                                        sessions: list) -> list:
    """为 Sessions 附加播放段 started_at，供前端按开始时间稳定排序。"""
    name = (instance_name or '').strip()
    if not name or not sessions:
        return list(sessions or [])
    store = _load_store(name)
    playing = [
        rec for rec in (store.get('records') or [])
        if rec.get('status') == 'playing'
    ]
    by_sid: Dict[str, str] = {}
    by_track: Dict[tuple, str] = {}
    for rec in playing:
        started = str(rec.get('started_at') or '').strip()
        if not started:
            continue
        sid = str(rec.get('emby_session_id') or '').strip()
        if sid:
            by_sid[sid] = started
        by_track[_segment_key(rec)] = started

    enriched = []
    for raw in sessions:
        session = dict(raw)
        prepared = _prepare_session(session)
        sid = str(prepared.get('id') or '').strip()
        started = by_sid.get(sid) or by_track.get(_session_track_key(prepared), '')
        if started:
            session['playback_started_at'] = started
        enriched.append(session)
    return enriched


def _active_playing_sessions(sessions: list) -> List[dict]:
    result = []
    for raw in sessions or []:
        session = _prepare_session(raw)
        if not session.get('is_playing') and not session.get('item_id'):
            continue
        if not session.get('title') and not session.get('series_name'):
            continue
        result.append(session)
    return result


def _handle_offline(instance_name: str, store: dict, now_mono: float) -> bool:
    bucket = _runtime_bucket(instance_name)
    if bucket.get('offline_since') is None:
        bucket['offline_since'] = now_mono
    elapsed = now_mono - float(bucket['offline_since'])
    changed = False
    if elapsed < OFFLINE_TIMEOUT_SECONDS:
        return False
    for record in store.get('records') or []:
        if record.get('status') != 'playing':
            continue
        _finalize_record(
            instance_name, record,
            status='incomplete',
            interrupt_reason='timeout_offline',
        )
        changed = True
    bucket['offline_since'] = now_mono
    return changed


def tick_from_sessions(instance_name: str, sessions: list, *,
                       api_online: bool = True) -> bool:
    """Sessions 轮询入口：热更新 open 段，检测开始/结束/超时。"""
    if not instance_name:
        return False
    now_mono = time.monotonic()
    with _lock:
        store = _load_store(instance_name)
        changed = False
        bucket = _runtime_bucket(instance_name)

        if not api_online:
            bucket['was_api_online'] = False
            if _handle_offline(instance_name, store, now_mono):
                changed = True
                _save_store(store)
            return changed

        if not bucket.get('was_api_online', True):
            bucket['offline_since'] = None
        bucket['was_api_online'] = True
        bucket['offline_since'] = None

        playing_sessions = _active_playing_sessions(sessions)
        seen_rids: Set[int] = set()
        seen_sids: Set[str] = set()

        for session in playing_sessions:
            sid = str(session.get('id') or '')
            if sid:
                seen_sids.add(sid)
            prepared = _prepare_session(session)
            record = _find_open_record(store, prepared, instance_name)
            if record and sid and record.get('emby_session_id') and record.get('emby_session_id') != sid:
                old_item = str(record.get('item_id') or '')
                new_item = str(session.get('item_id') or '')
                if old_item and new_item and old_item != new_item:
                    _finalize_record(instance_name, record, status='ended')
                    changed = True
                    record = None
            if record is None:
                record = _new_record(instance_name, store, prepared)
                store.setdefault('records', []).insert(0, record)
                changed = True
            else:
                if sid and not record.get('emby_session_id'):
                    record['emby_session_id'] = sid
                old_item = str(record.get('item_id') or '')
                new_item = str(prepared.get('item_id') or '')
                if old_item and new_item and old_item != new_item:
                    _finalize_record(instance_name, record, status='ended')
                    record = _new_record(instance_name, store, prepared)
                    store.setdefault('records', []).insert(0, record)
                    changed = True
                else:
                    _apply_session_meta(record, prepared)
                    snap = emby_watch_progress.update_for_record(
                        instance_name, record, prepared,
                    )
                    _apply_watch_snapshot(record, snap)
                    changed = True
            record['last_tick_at'] = _utc_now_iso()
            seen_rids.add(int(record.get('id') or 0))
            if sid:
                bucket[f'sid:{sid}'] = record.get('id')
            bucket[f'track:{_segment_key(record)}'] = record.get('id')

        for record in list(store.get('records') or []):
            if record.get('status') != 'playing':
                continue
            rid = int(record.get('id') or 0)
            if rid in seen_rids:
                continue
            sid = str(record.get('emby_session_id') or '')
            if sid and sid in seen_sids:
                continue
            _finalize_record(instance_name, record, status='ended')
            changed = True

        if changed:
            records = store.get('records') or []
            playing = [r for r in records if r.get('status') == 'playing']
            others = [r for r in records if r.get('status') != 'playing']
            others.sort(key=_ended_record_sort_key, reverse=True)
            store['records'] = (playing + others)[:MAX_STORED_RECORDS]
            _save_store(store)
        return changed


def list_records(instance_name: str = None, limit: int = 200) -> List[dict]:
    limit = max(1, min(int(limit or 200), MAX_STORED_RECORDS))
    if instance_name:
        store = _load_store(instance_name)
        records = list(store.get('records') or [])
        for rec in records:
            rec.setdefault('instance_name', instance_name)
        playing = [r for r in records if r.get('status') == 'playing']
        others = [r for r in records if r.get('status') != 'playing']
        others.sort(key=_ended_record_sort_key, reverse=True)
        return (playing + others)[:limit]

    result = []
    if not os.path.isdir(EMBY_EVENTS_DIR):
        return result
    _migrate_all_stores_once()
    for fname in os.listdir(EMBY_EVENTS_DIR):
        if not fname.endswith('.json'):
            continue
        data = _read_json(os.path.join(EMBY_EVENTS_DIR, fname), {})
        if not isinstance(data.get('records'), list):
            continue
        inst = data.get('instance_name') or ''
        for rec in data.get('records') or []:
            item = dict(rec)
            item.setdefault('instance_name', inst)
            result.append(item)
    playing = [r for r in result if r.get('status') == 'playing']
    others = [r for r in result if r.get('status') != 'playing']
    others.sort(key=_ended_record_sort_key, reverse=True)
    return (playing + others)[:limit]



def delete_instance_records(instance_name: str) -> bool:
    path = _store_path(instance_name)
    with _lock:
        if not os.path.isfile(path):
            return False
        try:
            os.remove(path)
            return True
        except OSError as e:
            logger.warning(f'删除播放记录缓存失败 {path}: {e}')
            return False


def rename_instance_records(old_name: str, new_name: str) -> None:
    if not old_name or not new_name or old_name == new_name:
        return
    with _lock:
        old_path = _store_path(old_name)
        if not os.path.isfile(old_path):
            return
        store = _load_store(old_name)
        store['instance_name'] = new_name
        for record in store.get('records') or []:
            record['instance_name'] = new_name
        _save_store(store)
        if old_path != _store_path(new_name):
            try:
                os.remove(old_path)
            except OSError as e:
                logger.warning(f'删除旧播放记录文件失败 {old_path}: {e}')
