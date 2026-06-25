"""外网播放估算上行：累加器 take 一次，写入播放记录。"""

from typing import List, Optional

import emby_playback_traffic
from emby_client import EmbyClient


def _upload_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        v = int(val)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def upload_subject_candidates(subject: dict) -> List[dict]:
    seen = set()
    result: List[dict] = []
    for client in (subject.get('client'), subject.get('device_name')):
        if not client:
            continue
        key = EmbyClient._normalize_client_key(str(client))
        if not key or key in seen:
            continue
        seen.add(key)
        if key == EmbyClient._normalize_client_key(subject.get('client') or ''):
            result.append(subject)
        else:
            result.append({**subject, 'client': client})
    return result or [subject]


def try_take_upload(instance_name: str, subject: dict) -> Optional[int]:
    if not subject or not subject.get('is_remote'):
        return None
    for cand in upload_subject_candidates(subject):
        taken = emby_playback_traffic.take_accumulated_upload(instance_name, cand)
        if taken is not None and taken > 0:
            return taken
    return None


def resolve_upload_bytes(instance_name: str, *, playback_record: dict) -> Optional[int]:
    """解析估算上行并写入播放记录。返回写入的字节数。"""
    if not playback_record or not playback_record.get('is_remote'):
        return None

    existing = _upload_int(playback_record.get('estimated_upload_bytes'))
    if existing:
        return existing

    taken = try_take_upload(instance_name, playback_record)
    if taken:
        playback_record['estimated_upload_bytes'] = taken
        return taken

    return None
