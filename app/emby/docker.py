"""通过 Docker Unix Socket 读取容器网络统计"""

import http.client
import json
import logging
import os
import socket
from typing import Optional, Dict
from urllib.parse import quote

logger = logging.getLogger(__name__)

DEFAULT_SOCKET = '/var/run/docker.sock'


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, unix_socket: str, timeout: float = 5):
        super().__init__('localhost', timeout=timeout)
        self.unix_socket = unix_socket

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.unix_socket)


class DockerStatsClient:
    """只读 Docker API 客户端，用于采集容器网络字节数"""

    def __init__(self, socket_path: str = DEFAULT_SOCKET):
        self.socket_path = socket_path or DEFAULT_SOCKET

    def is_available(self) -> bool:
        return os.path.exists(self.socket_path)

    def _request(self, method: str, path: str) -> Optional[dict]:
        if not self.is_available():
            return None
        conn = None
        try:
            conn = UnixHTTPConnection(self.socket_path, timeout=5)
            conn.request(method, path)
            resp = conn.getresponse()
            body = resp.read()
            if resp.status >= 400:
                logger.debug(
                    f'Docker API {method} {path} -> {resp.status}: {body[:200]!r}'
                )
                return None
            if not body:
                return {}
            return json.loads(body.decode('utf-8'))
        except Exception as e:
            logger.debug(f'Docker API 请求失败 {path}: {e}')
            return None
        finally:
            if conn:
                conn.close()

    @staticmethod
    def _sum_network_bytes(stats: dict) -> tuple:
        tx = rx = 0
        for net in (stats.get('networks') or {}).values():
            tx += int(net.get('tx_bytes') or 0)
            rx += int(net.get('rx_bytes') or 0)
        return tx, rx

    def _container_path(self, container_ref: str) -> str:
        return quote(container_ref, safe='')

    def inspect_container(self, container_ref: str) -> Optional[dict]:
        if not container_ref:
            return None
        return self._request('GET', f'/containers/{self._container_path(container_ref)}/json')

    def get_container_stats(self, container_ref: str) -> Optional[Dict]:
        if not container_ref:
            return None
        data = self._request(
            'GET',
            f'/containers/{self._container_path(container_ref)}/stats?stream=0',
        )
        if not data:
            return None
        tx, rx = self._sum_network_bytes(data)
        return {
            'tx_bytes': tx,
            'rx_bytes': rx,
            'read': data.get('read'),
        }

    @staticmethod
    def resolve_container_ref(container_name: str = '',
                              container_id: str = '') -> str:
        ref = (container_id or '').strip() or (container_name or '').strip()
        if ref and not container_id and container_name:
            return container_name.lstrip('/')
        return ref.lstrip('/')

    def test_container(self, container_name: str = '',
                       container_id: str = '') -> dict:
        ref = DockerStatsClient.resolve_container_ref(container_name, container_id)
        if not ref:
            return {'ok': False, 'error': '请填写容器名或容器 ID'}
        if not self.is_available():
            return {
                'ok': False,
                'error': 'Docker socket 不可用，请挂载 /var/run/docker.sock',
            }
        info = self.inspect_container(ref)
        if not info:
            return {'ok': False, 'error': f'找不到容器: {ref}'}
        stats = self.get_container_stats(ref)
        state = (info.get('State') or {})
        full_id = info.get('Id') or ''
        return {
            'ok': True,
            'container_id': full_id[:12] if full_id else '',
            'container_name': (info.get('Name') or '').lstrip('/'),
            'state': state.get('Status', 'unknown'),
            'tx_bytes': stats['tx_bytes'] if stats else 0,
            'rx_bytes': stats['rx_bytes'] if stats else 0,
        }
