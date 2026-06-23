"""Emby 单次外网播放会话的上行流量估算累计。"""

import threading
from typing import Dict, List, Optional

from emby_traffic_filter import (
    allocate_wan_upload_per_session,
    legacy_playback_accumulator_key,
    playback_accumulator_key,
)
from emby_upload_estimate import cap_estimated_upload_bytes

_lock = threading.RLock()
_upload_accumulators: Dict[str, Dict[str, int]] = {}


def accumulate_wan_upload(instance_name: str, sessions: list, delta_up: int,
                          wan_pool_only: bool = False) -> None:
    """将本采集周期的外网估算上行增量累计到各外网播放会话。"""
    name = (instance_name or '').strip()
    if not name or delta_up <= 0 or not sessions:
        return
    shares = allocate_wan_upload_per_session(
        delta_up, sessions, wan_pool_only=wan_pool_only,
    )
    if not shares:
        return
    with _lock:
        bucket = _upload_accumulators.setdefault(name, {})
        for key, amount in shares.items():
            if amount <= 0:
                continue
            bucket[key] = bucket.get(key, 0) + amount


def peek_accumulated_upload(instance_name: str, event: dict) -> Optional[int]:
    name = (instance_name or '').strip()
    if not name or not event:
        return None
    key = _match_accumulator_key((_upload_accumulators.get(name) or {}), event)
    if not key:
        return None
    value = (_upload_accumulators.get(name) or {}).get(key)
    if value is None:
        return None
    return max(0, int(value))


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


def take_accumulated_upload(instance_name: str, event: dict) -> Optional[int]:
    """读取并清除与停止播放事件匹配的外网累计上行（累加器路径信任 Docker 实测）。"""
    name = (instance_name or '').strip()
    if not name or not event:
        return None
    with _lock:
        bucket = _upload_accumulators.get(name) or {}
        key = _match_accumulator_key(bucket, event)
        if not key:
            return None
        value = bucket.pop(key, None)
        if not bucket:
            _upload_accumulators.pop(name, None)
    if value is None:
        return None
    raw = max(0, int(value))
    if raw <= 0:
        return None
    return cap_estimated_upload_bytes(event, raw, from_accumulator=True)
