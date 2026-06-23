"""Emby 外网流量估算：Docker 总量无法按 IP 拆分，需结合播放会话客户端 IP 与码率分配。"""

from __future__ import annotations

import ipaddress
import logging
from typing import Iterable, List, Tuple

logger = logging.getLogger(__name__)

# Emby 偶发上报占位码率（如 1000001）；转码分摊时改用保守默认值。
SUSPICIOUS_PLACEHOLDER_BPS = 1_500_000
DEFAULT_STREAM_BPS = 8_000_000

_TRANSCODE_KINDS = frozenset({
    'full_transcode', 'audio_transcode', 'video_transcode',
})


def parse_endpoint_ip(remote_endpoint: str) -> str:
    ep = (remote_endpoint or '').strip()
    if not ep:
        return ''
    if ep.startswith('['):
        end = ep.find(']')
        if end > 0:
            return ep[1:end]
    if ep.count('.') == 3 and ':' in ep:
        return ep.rsplit(':', 1)[0]
    if ':' in ep and '.' not in ep:
        return ep
    return ep


def is_lan_ip(ip_str: str) -> bool:
    if not ip_str:
        return True
    try:
        addr = ipaddress.ip_address(ip_str.strip('[]'))
    except ValueError:
        return False
    return bool(
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_reserved
    )


def is_wan_endpoint(remote_endpoint: str) -> bool:
    ip = parse_endpoint_ip(remote_endpoint)
    if not ip:
        return False
    return not is_lan_ip(ip)


def is_wan_playback_session(session: dict) -> bool:
    if not session:
        return False
    if not (session.get('is_playing') or session.get('item_id')):
        return False
    return is_wan_endpoint(session.get('remote_endpoint') or '')


def _is_transcode_session(session: dict) -> bool:
    if not session:
        return False
    if (session.get('play_method') or '').strip() == 'Transcode':
        return True
    return (session.get('transcode_kind') or '').strip() in _TRANSCODE_KINDS


def session_stream_bps(session: dict) -> int:
    """有效码率：优先 video+audio 分量，过滤占位低码率，转码缺省 8Mbps。"""
    video = int(session.get('video_bitrate') or 0)
    audio = int(session.get('audio_bitrate') or 0)
    component_sum = video + audio
    if component_sum > 0:
        return component_sum

    bps = int(session.get('bitrate') or 0)
    if bps > SUSPICIOUS_PLACEHOLDER_BPS:
        return bps
    if bps > 0 and _is_transcode_session(session):
        return DEFAULT_STREAM_BPS
    if bps > 0:
        return bps
    if _is_transcode_session(session):
        return DEFAULT_STREAM_BPS
    if session.get('is_playing') or session.get('item_id'):
        return DEFAULT_STREAM_BPS
    return 0


def _active_playback_sessions(sessions: Iterable[dict]) -> List[dict]:
    return [
        s for s in (sessions or [])
        if s.get('is_playing') or s.get('item_id')
    ]


def _active_upload_sessions(sessions: Iterable[dict]) -> List[dict]:
    """参与外网上行分摊的会话：播放中且未暂停。"""
    return [
        s for s in _active_playback_sessions(sessions)
        if not bool(s.get('is_paused'))
    ]


def allocate_wan_deltas(delta_up: int, delta_dl: int,
                        sessions: Iterable[dict]) -> Tuple[int, int]:
    """
    将 Docker 容器增量按外网会话码率占比分配。
    无播放、仅局域网播放、或无法识别外网会话时返回 (0, 0)。
    """
    delta_up = max(0, int(delta_up or 0))
    delta_dl = max(0, int(delta_dl or 0))
    if delta_up == 0 and delta_dl == 0:
        return 0, 0

    playing = _active_playback_sessions(sessions)
    if not playing:
        return 0, 0

    wan = [s for s in playing if is_wan_playback_session(s)]
    if not wan:
        return 0, 0

    lan = [s for s in playing if not is_wan_playback_session(s)]
    if not lan:
        return delta_up, delta_dl

    wan_bps = sum(session_stream_bps(s) for s in wan)
    total_bps = sum(session_stream_bps(s) for s in playing)
    if total_bps <= 0:
        ratio = len(wan) / len(playing)
    else:
        ratio = wan_bps / total_bps
    ratio = max(0.0, min(1.0, ratio))
    return int(delta_up * ratio), int(delta_dl * ratio)


def apply_wan_traffic_filter(delta_up: int, delta_dl: int,
                             sessions: Iterable[dict],
                             enabled: bool) -> Tuple[int, int]:
    if not enabled:
        return max(0, int(delta_up or 0)), max(0, int(delta_dl or 0))
    return allocate_wan_deltas(delta_up, delta_dl, sessions)


def playback_accumulator_key(record: dict) -> str:
    """外网播放流量累加键：优先 user+client+item_id，其次集数标签，最后剧名标题。"""
    user = (record.get('user_name') or record.get('UserName') or '').strip().casefold()
    client = (record.get('client') or record.get('Client') or '').strip().casefold()
    item_id = str(record.get('item_id') or record.get('ItemId') or '').strip()
    series = (record.get('series_name') or '').casefold()
    episode_label = (record.get('episode_label') or '').strip().casefold()
    title = (
        record.get('item_title') or record.get('title')
        or record.get('episode_title') or ''
    ).casefold()

    if user and client and item_id:
        return f'{user}|{client}|{item_id}'
    if user and client and episode_label:
        return f'{user}|{client}|{series}|{episode_label}'
    if user and client and (series or title):
        return f'{user}|{client}|{series}|{title}'
    return f'{user}|{series}|{title}'


def legacy_playback_accumulator_key(record: dict) -> str:
    """旧版累加键，用于停止事件与历史累计桶匹配。"""
    user = (record.get('user_name') or record.get('UserName') or '').strip().casefold()
    series = (record.get('series_name') or '').casefold()
    title = (
        record.get('item_title') or record.get('title')
        or record.get('episode_title') or ''
    ).casefold()
    return f'{user}|{series}|{title}'


def allocate_wan_upload_per_session(delta_up: int,
                                    sessions: Iterable[dict],
                                    wan_pool_only: bool = False) -> dict:
    """将上传增量中的外网部分，按外网会话码率占比分摊。"""
    delta_up = max(0, int(delta_up or 0))
    if delta_up == 0:
        return {}

    playing = _active_upload_sessions(sessions)
    wan = [s for s in playing if is_wan_playback_session(s)]
    if not wan:
        return {}

    if wan_pool_only:
        wan_pool = delta_up
    else:
        lan = [s for s in playing if not is_wan_playback_session(s)]
        if lan:
            wan_bps = sum(session_stream_bps(s) for s in wan)
            total_bps = sum(session_stream_bps(s) for s in playing)
            if total_bps <= 0:
                ratio = len(wan) / len(playing)
            else:
                ratio = wan_bps / total_bps
            wan_pool = int(delta_up * max(0.0, min(1.0, ratio)))
        else:
            wan_pool = delta_up

    if wan_pool <= 0:
        return {}

    wan_bps_total = sum(session_stream_bps(s) for s in wan)
    result: dict = {}
    if wan_bps_total <= 0:
        share = wan_pool // len(wan)
        remainder = wan_pool % len(wan)
        for idx, session in enumerate(wan):
            key = playback_accumulator_key(session)
            result[key] = result.get(key, 0) + share + (1 if idx < remainder else 0)
        return result

    assigned = 0
    for idx, session in enumerate(wan):
        key = playback_accumulator_key(session)
        if idx == len(wan) - 1:
            part = wan_pool - assigned
        else:
            part = int(wan_pool * session_stream_bps(session) / wan_bps_total)
            assigned += part
        result[key] = result.get(key, 0) + part
    return result
