"""Emby 用户名单同步：下拉选项与已删用户数据清理。"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

import emby.traffic.db as emby_traffic_db
import emby.records.store as playback_record_store
from emby.client import EmbyClient

logger = logging.getLogger(__name__)

USER_SYNC_INTERVAL_SECONDS = 300

_lock = threading.RLock()
_last_sync_mono: Dict[str, float] = {}


def _user_fold(name: str) -> str:
    return str(name or '').strip().casefold()


def fetch_server_users(client: EmbyClient) -> Tuple[List[dict], bool]:
    users = client.get_users() if client else []
    return users, bool(users)


def list_instance_user_names(
    client: EmbyClient,
    instance_name: str,
) -> Tuple[List[str], str]:
    """返回 (用户名列表, 来源 emby|local)。"""
    users, ok = fetch_server_users(client)
    if ok:
        return [row['name'] for row in users if row.get('name')], 'emby'
    local = emby_traffic_db.list_distinct_user_names(instance_name)
    return local, 'local'


def _should_sync(instance_name: str, *, force: bool = False) -> bool:
    if force:
        return True
    now = time.monotonic()
    with _lock:
        last = _last_sync_mono.get(instance_name, 0.0)
        if now - last < USER_SYNC_INTERVAL_SECONDS:
            return False
        _last_sync_mono[instance_name] = now
        return True


def _server_name_folds(users: List[dict]) -> Set[str]:
    return {
        _user_fold(row.get('name') or '')
        for row in (users or [])
        if _user_fold(row.get('name') or '')
    }


def _local_names_to_purge(
    instance_name: str,
    server_users: List[dict],
) -> List[str]:
    server_folds = _server_name_folds(server_users)
    if not server_folds:
        return []
    local_names = emby_traffic_db.list_distinct_user_names(instance_name)
    return [
        name for name in local_names
        if _user_fold(name) and _user_fold(name) not in server_folds
    ]


def purge_deleted_user(
    instance_name: str,
    user_name: str,
    *,
    user_ids: Optional[List[str]] = None,
) -> None:
    name = (instance_name or '').strip()
    user = (user_name or '').strip()
    if not name or not user:
        return
    ids = list(user_ids or [])
    if not ids:
        ids = emby_traffic_db.collect_user_ids_for_name(name, user)
    emby_traffic_db.delete_user_data(name, user, user_ids=ids)
    try:
        import emby.traffic.playback as emby_playback_traffic
        emby_playback_traffic.purge_user_state(name, user, user_ids=ids)
    except Exception as e:
        logger.debug(f'[Emby:{name}] 清理用户运行时状态失败 {user}: {e}')
    try:
        import emby.browse.settler as browse_upload_settler
        browse_upload_settler.purge_user(name, user, user_ids=ids)
    except Exception as e:
        logger.debug(f'[Emby:{name}] 清理选片结算状态失败 {user}: {e}')
    removed = playback_record_store.delete_user_records(name, user)
    logger.info(
        f'[Emby:{name}] 已同步删除 Emby 用户数据: {user}'
        f'（播放记录 {removed} 条）',
    )


def sync_deleted_users(
    instance_name: str,
    client: EmbyClient,
    *,
    force: bool = False,
) -> List[str]:
    """对比 Emby 用户名单，清理本地已不存在用户的数据。"""
    name = (instance_name or '').strip()
    if not name or not client:
        return []
    if not _should_sync(name, force=force):
        return []
    server_users, ok = fetch_server_users(client)
    if not ok:
        return []
    purged = []
    for user_name in _local_names_to_purge(name, server_users):
        purge_deleted_user(name, user_name)
        purged.append(user_name)
    return purged
