"""播放上行估算修复：合理上限、历史纠偏、异常告警。"""

import logging
import math
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# 直串/转码上传合理上限：码率 × 时长 × 系数（略高于理论值留余量）
_UPLOAD_CEILING_BITRATE_FACTOR = 1.35
# 无码率时按 25 Mbps 估算（1080p 直串偏保守上限）
_FALLBACK_BITRATE_BPS = 25_000_000
# 超过上限此倍数时打 warning
_INFLATED_UPLOAD_WARN_RATIO = 3.0


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


def _played_seconds(record: dict) -> int:
    runtime = int(record.get('runtime_seconds') or 0)
    played = int(record.get('played_seconds') or 0)
    if runtime > 0:
        return runtime
    if played > 0:
        return played
    start_dt = _parse_iso(record.get('started_at') or '')
    stop_dt = _parse_iso(record.get('stopped_at') or '')
    if start_dt and stop_dt and stop_dt > start_dt:
        return max(1, int((stop_dt - start_dt).total_seconds()))
    return 0


def estimate_upload_ceiling_bytes(record: dict) -> Optional[int]:
    """按片长与码率估算外网播放上传合理上限（字节）。"""
    if not record or not record.get('is_remote'):
        return None
    seconds = _played_seconds(record)
    if seconds <= 0:
        return None
    bitrate = int(record.get('bitrate') or 0)
    if bitrate <= 0:
        bitrate = _FALLBACK_BITRATE_BPS
    # bitrate 通常为 bps；直串上传约等于下载量
    theoretical = (bitrate * seconds) / 8.0
    ceiling = int(math.ceil(theoretical * _UPLOAD_CEILING_BITRATE_FACTOR))
    return max(1, ceiling)


def warn_if_inflated_playback_upload(
    instance_name: str,
    record: dict,
    *,
    upload_bytes: int = None,
) -> None:
    """单条播放记录上传明显偏离合理上限时告警。"""
    if not record or not record.get('is_remote'):
        return
    val = upload_bytes
    if val is None:
        val = int(record.get('estimated_upload_bytes') or 0)
    val = max(0, int(val or 0))
    if val <= 0:
        return
    ceiling = estimate_upload_ceiling_bytes(record)
    if not ceiling or val <= ceiling * _INFLATED_UPLOAD_WARN_RATIO:
        return
    inst = (instance_name or record.get('instance_name') or '').strip()
    user = (record.get('user_name') or '').strip()
    title = (record.get('series_name') or record.get('item_title') or '').strip()
    ep = (record.get('episode_label') or '').strip()
    logger.warning(
        '[Playback:%s] 外网播放上传异常偏高 rid=%s user=%s title=%s%s '
        'upload=%s ceiling=%s ratio=%.1fx',
        inst or '?',
        record.get('id'),
        user or '?',
        title or '?',
        f' {ep}' if ep else '',
        val,
        ceiling,
        val / max(1, ceiling),
    )


def repair_inflated_playback_upload_estimates(
    instance_name: str = None,
    *,
    rebuild_stats: bool = True,
) -> Dict[str, int]:
    """纠偏播放 JSON 中明显偏高的 estimated_upload_bytes，并可选重建聚合。"""
    import emby.records.store as playback_record_store
    from emby.storage_paths import EMBY_EVENTS_DIR
    import os

    playback_record_store._migrate_all_stores_once()

    targets = []
    if instance_name:
        targets.append((instance_name, playback_record_store._load_store(instance_name)))
    elif os.path.isdir(EMBY_EVENTS_DIR):
        from core.secrets_store import _read_json
        for fname in os.listdir(EMBY_EVENTS_DIR):
            if not fname.endswith('.json'):
                continue
            path = os.path.join(EMBY_EVENTS_DIR, fname)
            data = _read_json(path, {})
            if not isinstance(data.get('records'), list):
                continue
            inst = (data.get('instance_name') or '').strip()
            if inst:
                targets.append((inst, data))

    stats = {
        'stores': 0,
        'scanned': 0,
        'repaired': 0,
        'synced_checkpoint': 0,
        'skipped': 0,
    }

    for inst, store in targets:
        if not inst or not isinstance(store, dict):
            continue
        changed = False
        stats['stores'] += 1
        for rec in store.get('records') or []:
            if rec.get('status') == 'playing':
                booked = max(0, int(rec.get('estimated_upload_bytes') or 0))
                chk = max(0, int(rec.get('live_upload_checkpoint_bytes') or 0))
                if booked > 0 and chk > 0 and chk < booked:
                    rec['live_upload_checkpoint_bytes'] = booked
                    stats['synced_checkpoint'] += 1
                    changed = True
                continue
            if rec.get('status') not in ('ended', 'incomplete'):
                continue
            if not rec.get('is_remote'):
                continue
            stats['scanned'] += 1
            upload = max(0, int(rec.get('estimated_upload_bytes') or 0))
            if upload <= 0:
                stats['skipped'] += 1
                continue
            # 单段流量不设上限：反复重看/跳转重缓冲会真实叠加，允许超出码率×时长的理论值。
            # 这里不再把偏高值削回 ceiling，仅在明显异常时告警便于排查归属漂移。
            ceiling = estimate_upload_ceiling_bytes(rec)
            if ceiling and upload > ceiling * _INFLATED_UPLOAD_WARN_RATIO:
                logger.warning(
                    '[Playback:%s] 外网播放上传明显偏高 rid=%s upload=%s ceiling=%s '
                    'ratio=%.1fx（保留真实值，不设上限）',
                    inst,
                    rec.get('id'),
                    upload,
                    ceiling,
                    upload / max(1, ceiling),
                )
            stats['skipped'] += 1
        if changed:
            playback_record_store._save_store(store)

    if rebuild_stats and stats['repaired'] > 0:
        try:
            import emby.traffic.db as emby_traffic_db
            rebuild = emby_traffic_db.rebuild_playback_upload_stats(instance_name)
            stats['rebuilt_facts'] = rebuild.get('facts', 0)
        except Exception as e:
            logger.error('重建外网播放上行聚合失败: %s', e, exc_info=True)
            stats['rebuilt_facts'] = 0

    return stats
