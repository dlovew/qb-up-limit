"""Emby 播放观看进度：按每次 start/stop 独立累计在播时长与片内起止位置。"""

import threading
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from emby_client import EmbyClient

WATCH_LOCK_SECONDS = 30
WATCH_EFFECTIVE_SECONDS = 300
SEEK_TOLERANCE_SECONDS = 5
STALL_GRACE_SECONDS = 8
MAX_POLL_GAP_SECONDS = 15
WATCH_COMPLETE_RATIO = 0.85

WATCH_FIELD_KEYS = (
    'runtime_seconds',
    'start_position_seconds',
    'end_position_seconds',
    'played_seconds',
    'watch_start_locked',
    'watch_fields_frozen',
    'seek_count',
    'seek_forward_count',
    'seek_backward_count',
    'last_seek_at',
)

_lock = threading.RLock()
_trackers: Dict[str, Dict[str, 'SessionWatchState']] = {}


class SessionWatchState:
    __slots__ = (
        'runtime_seconds', 'segment_start_position', 'continuous_seconds',
        'last_position', 'last_monotonic', 'stall_seconds', 'start_locked',
        'first_position_seconds', 'start_position_seconds', 'end_position_seconds',
        'played_seconds', 'seek_count', 'seek_forward_count', 'seek_backward_count',
        'last_seek_at', 'user_id', 'client', 'item_id', 'series_name',
        'item_title', 'episode_label', '_bound_media_id',
    )

    def __init__(self):
        self.runtime_seconds = 0
        self.segment_start_position = None
        self.continuous_seconds = 0.0
        self.last_position = None
        self.last_monotonic = None
        self.stall_seconds = 0.0
        self.start_locked = False
        self.first_position_seconds = None
        self.start_position_seconds = None
        self.end_position_seconds = None
        self.played_seconds = 0
        self.seek_count = 0
        self.seek_forward_count = 0
        self.seek_backward_count = 0
        self.last_seek_at = ''
        self.user_id = ''
        self.client = ''
        self.item_id = ''
        self.series_name = ''
        self.item_title = ''
        self.episode_label = ''
        self._bound_media_id = ''

    @staticmethod
    def _media_identity(item_id: str, series_name: str, episode_label: str,
                        item_title: str) -> str:
        if item_id:
            return f'id:{item_id}'
        if series_name and episode_label:
            return f'l:{series_name}|{episode_label}'
        if series_name and item_title:
            return f't:{series_name}|{item_title}'
        return ''

    def reset_pair(self) -> None:
        self.runtime_seconds = 0
        self.segment_start_position = None
        self.continuous_seconds = 0.0
        self.last_position = None
        self.last_monotonic = None
        self.stall_seconds = 0.0
        self.start_locked = False
        self.first_position_seconds = None
        self.start_position_seconds = None
        self.end_position_seconds = None
        self.played_seconds = 0
        self.seek_count = 0
        self.seek_forward_count = 0
        self.seek_backward_count = 0
        self.last_seek_at = ''

    def bind_session(self, record: dict) -> None:
        self.user_id = str(record.get('user_id') or record.get('UserId') or '').strip()
        self.client = EmbyClient._normalize_client_key(
            record.get('client') or record.get('Client')
            or record.get('device_name') or '',
        )
        item_id = str(record.get('item_id') or record.get('ItemId') or '').strip()
        series_name = (record.get('series_name') or '').casefold()
        item_title = (
            record.get('episode_title') or record.get('title')
            or record.get('item_title') or ''
        ).casefold()
        episode_label = (record.get('episode_label') or '').strip().casefold()
        media_id = self._media_identity(
            item_id, series_name, episode_label, item_title,
        )
        if self._bound_media_id and media_id and media_id != self._bound_media_id:
            self.reset_pair()
        if media_id:
            self._bound_media_id = media_id
        self.item_id = item_id
        self.series_name = series_name
        self.item_title = item_title
        self.episode_label = episode_label

    def hydrate_from_record(self, record: dict) -> None:
        """服务重启后：从 JSON playing 记录恢复 tracker，避免起点/时长被当前进度覆盖。"""
        if not isinstance(record, dict) or record.get('status') != 'playing':
            return

        runtime = int(record.get('runtime_seconds') or 0)
        if runtime > 0:
            self.runtime_seconds = runtime

        start_raw = record.get('start_position_seconds')
        if start_raw is not None:
            start_int = max(0, int(start_raw))
            self.first_position_seconds = start_int
            self.start_position_seconds = start_int
            self.segment_start_position = start_int

        end_raw = record.get('end_position_seconds')
        if end_raw is not None:
            end_int = max(0, int(end_raw))
            self.end_position_seconds = end_int
            self.last_position = end_int

        played = int(record.get('played_seconds') or 0)
        if played > 0:
            self.played_seconds = played

        if record.get('watch_start_locked'):
            self.start_locked = True
            self.continuous_seconds = float(WATCH_LOCK_SECONDS)

        for key in ('seek_count', 'seek_forward_count', 'seek_backward_count'):
            val = int(record.get(key) or 0)
            if val > 0:
                setattr(self, key, val)

        last_seek = str(record.get('last_seek_at') or '').strip()
        if last_seek:
            self.last_seek_at = last_seek

    def snapshot(self) -> dict:
        result = {}
        if self.runtime_seconds > 0:
            result['runtime_seconds'] = self.runtime_seconds
        if self.end_position_seconds is not None:
            result['end_position_seconds'] = self.end_position_seconds
        if self.played_seconds > 0:
            result['played_seconds'] = max(0, int(self.played_seconds))
        if self.first_position_seconds is not None:
            result['start_position_seconds'] = self.first_position_seconds
        elif self.start_position_seconds is not None:
            result['start_position_seconds'] = self.start_position_seconds
        if self.start_locked:
            result['watch_start_locked'] = True
        if self.seek_count > 0:
            result['seek_count'] = self.seek_count
        if self.seek_forward_count > 0:
            result['seek_forward_count'] = self.seek_forward_count
        if self.seek_backward_count > 0:
            result['seek_backward_count'] = self.seek_backward_count
        if self.last_seek_at:
            result['last_seek_at'] = self.last_seek_at
        return result

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '0000Z'

    def _record_seek(self, last_pos: int, pos: int) -> None:
        self.seek_count += 1
        if pos < last_pos:
            self.seek_backward_count += 1
        else:
            self.seek_forward_count += 1
        self.last_seek_at = self._utc_now_iso()

    def _reset_segment(self, position: int) -> None:
        self.segment_start_position = position
        self.continuous_seconds = 0.0
        self.stall_seconds = 0.0

    @staticmethod
    def _is_seek(last_pos: int, pos: int, elapsed: float) -> bool:
        delta = pos - last_pos
        if delta < -SEEK_TOLERANCE_SECONDS:
            return True
        if delta > elapsed + SEEK_TOLERANCE_SECONDS:
            return True
        return False

    def tick(self, session: dict) -> None:
        self.bind_session(session)
        runtime = int(session.get('runtime_seconds') or 0)
        position = max(0, int(session.get('position_seconds') or 0))
        is_paused = bool(session.get('is_paused'))
        now = time.monotonic()

        if runtime > 0:
            self.runtime_seconds = runtime
        if self.first_position_seconds is None:
            self.first_position_seconds = position
        self.end_position_seconds = position

        if self.last_monotonic is None:
            self.last_position = position
            self.last_monotonic = now
            if not is_paused:
                self.segment_start_position = position
            return

        elapsed = min(max(0.0, now - self.last_monotonic), MAX_POLL_GAP_SECONDS)
        self.last_monotonic = now
        last_pos = self.last_position if self.last_position is not None else position

        if is_paused:
            self._reset_segment(position)
            self.last_position = position
            return

        if self._is_seek(last_pos, position, elapsed):
            self._record_seek(last_pos, position)
            self._reset_segment(position)
            self.last_position = position
            return

        if abs(position - last_pos) <= 1:
            self.stall_seconds += elapsed
        else:
            self.stall_seconds = 0.0

        if self.segment_start_position is None:
            self.segment_start_position = last_pos

        self.continuous_seconds += elapsed

        if not self.start_locked and self.continuous_seconds >= WATCH_LOCK_SECONDS:
            self.start_locked = True
            self.start_position_seconds = int(self.segment_start_position or position)

        self.played_seconds += int(elapsed)
        self.last_position = position


def _media_key(record: dict) -> str:
    item_id = str(record.get('item_id') or record.get('ItemId') or '').strip()
    if not item_id:
        now_playing = record.get('NowPlayingItem') or {}
        if isinstance(now_playing, dict):
            item_id = str(now_playing.get('Id') or '').strip()
    if item_id:
        return item_id
    series = (record.get('series_name') or '').casefold()
    label = (record.get('episode_label') or '').strip().casefold()
    if series and label:
        return f'{series}|{label}'
    title = (
        record.get('episode_title') or record.get('title')
        or record.get('item_title') or ''
    ).casefold()
    if series and title:
        return f'{series}|{title}'
    return 'unknown'


def _session_track_key(record: dict) -> str:
    user_id = str(record.get('user_id') or record.get('UserId') or '').strip()
    client = EmbyClient._normalize_client_key(
        record.get('client') or record.get('Client')
        or record.get('device_name') or '',
    )
    return f'{user_id}|{client}|{_media_key(record)}'


def begin_pair_watch(instance_name: str, record: dict) -> None:
    """新一次开始播放：重置该用户/客户端/媒体的观看累计。"""
    name = (instance_name or '').strip()
    if not name or not record:
        return
    key = _session_track_key(record)
    with _lock:
        bucket = _trackers.setdefault(name, {})
        state = bucket.get(key)
        if state is None:
            state = SessionWatchState()
            bucket[key] = state
        else:
            state.reset_pair()
        state.bind_session(record)


def _get_state(instance_name: str, record: dict) -> Optional[SessionWatchState]:
    name = (instance_name or '').strip()
    if not name or not record:
        return None
    key = _session_track_key(record)
    with _lock:
        return (_trackers.get(name) or {}).get(key)


def snapshot_for_record(instance_name: str, record: dict) -> dict:
    state = _get_state(instance_name, record)
    if state is None:
        return {}
    return state.snapshot()


def reset_tracker_for_record(instance_name: str, record: dict) -> None:
    name = (instance_name or '').strip()
    if not name or not record:
        return
    key = _session_track_key(record)
    with _lock:
        state = (_trackers.get(name) or {}).get(key)
        if state is not None:
            state.reset_pair()


def finalize_watch_to_event(instance_name: str, event: dict) -> bool:
    """停止播放：写入本段观看快照并冻结，随后重置 tracker。"""
    if not isinstance(event, dict) or event.get('watch_fields_frozen'):
        return False
    snap = snapshot_for_record(instance_name, event)
    if not snap:
        return False
    if not snap.get('played_seconds') and snap.get('end_position_seconds') is None:
        return False
    merge_watch_snapshot(event, snap, overwrite=True)
    event['watch_fields_frozen'] = True
    reset_tracker_for_record(instance_name, event)
    return True


def update_from_session(instance_name: str, session: dict) -> dict:
    name = (instance_name or '').strip()
    if not name or not session:
        return {}
    if session.get('NowPlayingItem') or session.get('PlayState'):
        session = EmbyClient.normalize_session(session)
    key = _session_track_key(session)
    with _lock:
        bucket = _trackers.setdefault(name, {})
        state = bucket.get(key)
        if state is None:
            state = SessionWatchState()
            bucket[key] = state
        state.tick(session)
        return state.snapshot()


def update_for_record(instance_name: str, record: dict, session: dict) -> dict:
    """按播放段 record 绑定的 tracker 累计，避免身份字段不一致导致 key 错位。"""
    name = (instance_name or '').strip()
    if not name or not record or not session:
        return {}
    if session.get('NowPlayingItem') or session.get('PlayState'):
        session = EmbyClient.normalize_session(session)
    key = _session_track_key(record)
    with _lock:
        bucket = _trackers.setdefault(name, {})
        state = bucket.get(key)
        if state is None:
            state = SessionWatchState()
            bucket[key] = state
            state.bind_session(record)
            if record.get('status') == 'playing':
                state.hydrate_from_record(record)
        state.tick(session)
        return state.snapshot()


def get_snapshot_for_session(instance_name: str, session: dict) -> dict:
    return snapshot_for_record(instance_name, session)


def find_snapshot_for_event(instance_name: str, event: dict) -> dict:
    snap = snapshot_for_record(instance_name, event)
    if snap:
        return snap
    name = (instance_name or '').strip()
    if not name or not event:
        return {}
    user_id = str(event.get('user_id') or '').strip()
    client = EmbyClient._normalize_client_key(event.get('client') or '')
    series = (event.get('series_name') or '').casefold()
    label = (event.get('episode_label') or '').strip().casefold()
    title = (
        event.get('episode_title') or event.get('item_title') or ''
    ).casefold()

    with _lock:
        for state in (_trackers.get(name) or {}).values():
            if state.user_id != user_id or state.client != client:
                continue
            if label and state.episode_label and label == state.episode_label:
                return state.snapshot()
            if series and state.series_name and series == state.series_name:
                if label and state.episode_label and label != state.episode_label:
                    continue
                if title and state.item_title and title != state.item_title:
                    if title not in state.item_title and state.item_title not in title:
                        continue
                return state.snapshot()
    return {}


def apply_watch_fields(event: dict, snapshot: dict, *, exact: bool = False) -> bool:
    if not isinstance(event, dict) or not snapshot:
        return False
    if event.get('watch_fields_frozen'):
        return False
    changed = False

    runtime = snapshot.get('runtime_seconds')
    if runtime is not None and int(runtime) > 0:
        new_runtime = int(runtime)
        if event.get('runtime_seconds') != new_runtime:
            event['runtime_seconds'] = new_runtime
            changed = True

    end_pos = snapshot.get('end_position_seconds')
    if end_pos is not None:
        new_end = max(0, int(end_pos))
        if exact or event.get('end_position_seconds') is None:
            if event.get('end_position_seconds') != new_end:
                event['end_position_seconds'] = new_end
                changed = True
        elif new_end > int(event.get('end_position_seconds') or 0):
            event['end_position_seconds'] = new_end
            changed = True

    played = snapshot.get('played_seconds')
    if played is not None and int(played) > 0:
        new_played = int(played)
        if exact:
            if event.get('played_seconds') != new_played:
                event['played_seconds'] = new_played
                changed = True
        else:
            merged = max(int(event.get('played_seconds') or 0), new_played)
            if event.get('played_seconds') != merged:
                event['played_seconds'] = merged
                changed = True

    start_pos = snapshot.get('start_position_seconds')
    if start_pos is not None:
        new_start = max(0, int(start_pos))
        if event.get('start_position_seconds') is None or exact:
            if event.get('start_position_seconds') != new_start:
                event['start_position_seconds'] = new_start
                changed = True

    if snapshot.get('watch_start_locked'):
        if not event.get('watch_start_locked'):
            event['watch_start_locked'] = True
            changed = True

    for key in ('seek_count', 'seek_forward_count', 'seek_backward_count'):
        val = snapshot.get(key)
        if val is None:
            continue
        new_val = max(0, int(val))
        if exact:
            if event.get(key) != new_val:
                event[key] = new_val
                changed = True
        else:
            merged = max(int(event.get(key) or 0), new_val)
            if event.get(key) != merged:
                event[key] = merged
                changed = True

    last_seek = snapshot.get('last_seek_at')
    if last_seek and event.get('last_seek_at') != last_seek:
        event['last_seek_at'] = last_seek
        changed = True

    return changed


def merge_watch_snapshot(event: dict, snapshot: dict,
                         overwrite: bool = False) -> bool:
    if not isinstance(event, dict) or not snapshot:
        return False
    if event.get('watch_fields_frozen') and not overwrite:
        return False
    before = {k: event.get(k) for k in WATCH_FIELD_KEYS if k in event}
    apply_watch_fields(event, snapshot, exact=overwrite)
    if overwrite:
        for key in (
            'start_position_seconds', 'end_position_seconds', 'played_seconds',
            'seek_count', 'seek_forward_count', 'seek_backward_count',
        ):
            value = snapshot.get(key)
            if value is None:
                continue
            event[key] = max(0, int(value))
    after = {k: event.get(k) for k in WATCH_FIELD_KEYS if k in event}
    return before != after
