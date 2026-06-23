"""外网播放「估算上行」封顶。"""

from __future__ import annotations

from typing import Optional

MAX_WATCH_RATIO = 3.0

_TRANSCODE_KINDS = frozenset({
    'full_transcode', 'audio_transcode', 'video_transcode',
})


def _seek_count(event: dict) -> int:
    try:
        return max(0, int(event.get('seek_count') or 0))
    except (TypeError, ValueError):
        return 0


def _effective_played_seconds(event: dict) -> int:
    played = int(event.get('played_seconds') or 0)
    if played > 0:
        return played
    end = int(event.get('end_position_seconds') or 0)
    start = int(event.get('start_position_seconds') or 0)
    if end > start:
        return end - start
    return 0


def is_transcode_record(event: dict) -> bool:
    if not event:
        return False
    kind = (event.get('transcode_kind') or '').strip()
    if kind in _TRANSCODE_KINDS:
        return True
    return (event.get('play_method') or '').strip() == 'Transcode'


def _watch_ratio(event: dict) -> Optional[float]:
    """无跳转：片内跨度/片长（≤1）；有跳转：played_seconds/片长（可>1，重看场景）。"""
    runtime = int(event.get('runtime_seconds') or 0)
    if runtime <= 0:
        return None

    if _seek_count(event) > 0:
        played = _effective_played_seconds(event)
        if played <= 0:
            return None
        return min(MAX_WATCH_RATIO, played / runtime)

    start = event.get('start_position_seconds')
    end = event.get('end_position_seconds')
    if start is None or end is None:
        return None
    span = max(0, int(end) - int(start))
    if span <= 0:
        return None
    return min(1.0, span / runtime)


def _bitrate_cap_bytes(event: dict) -> Optional[int]:
    bitrate = int(event.get('bitrate') or 0)
    played = _effective_played_seconds(event)
    if bitrate <= 0 or played <= 0:
        return None
    return max(1, int(bitrate * played / 8))


def _upload_cap_bytes(event: dict) -> Optional[int]:
    """公式兜底封顶：转码按码率×时长，直传/串流按文件体积比例。"""
    if is_transcode_record(event):
        return _bitrate_cap_bytes(event)

    file_size = int(event.get('file_size_bytes') or 0)
    ratio = _watch_ratio(event)
    if file_size > 0 and ratio is not None and ratio > 0:
        return max(1, int(file_size * ratio))
    return _bitrate_cap_bytes(event)


def estimate_upload_from_playback(record: dict) -> Optional[int]:
    """累加器不可用时，按观看进度/码率估算外网上行（公式兜底）。"""
    if not record or not record.get('is_remote'):
        return None
    cap = _upload_cap_bytes(record)
    if cap is None or cap <= 0:
        return None
    return int(cap)


def cap_estimated_upload_bytes(event: dict, raw_bytes: int,
                             *, from_accumulator: bool = False) -> int:
    """累加器结果：信任 Docker 分摊；公式路径才按片长/码率封顶。"""
    raw_bytes = max(0, int(raw_bytes or 0))
    if raw_bytes <= 0 or not event:
        return 0
    if from_accumulator:
        return raw_bytes

    cap = _upload_cap_bytes(event)
    if cap is None or cap <= 0:
        return raw_bytes
    return min(raw_bytes, cap)
