"""Emby 外网流量分摊验算：离线模拟 + 在线轮询。

离线（在 app 目录下执行）:
  python -m tools.emby_traffic_verify
  python -m tools.emby_traffic_verify --offline

在线（需 Web 服务运行，默认 http://127.0.0.1:8765）:
  python -m tools.emby_traffic_verify --online --instance "MyEmby" --seconds 60
  python -m tools.emby_traffic_verify --online --url http://127.0.0.1:8765

环境变量（推荐，避免在命令行写密码）:
  QBUPLIMIT_VERIFY_URL / QBUPLIMIT_HOST / QBUPLIMIT_WEB_PORT
  QBUPLIMIT_WEB_USER / QBUPLIMIT_WEB_PASSWORD

一键自动验算:
  python -m tools.emby_traffic_verify --auto -u <user> -p <pass>

可选对比路由侧 WAN（MB）:
  --router-wan-start-mb 100 --router-wan-end-mb 205
  --router-wan-mb 123.4
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

_APP = Path(__file__).resolve().parent.parent.parent
_WORKSPACE = _APP.parent
if str(_APP) not in sys.path:
    sys.path.insert(0, str(_APP))

_DEFAULT_WEB_PORT = 8765


def _web_port_from_local_config() -> int | None:
    """读取本地 data/config.yaml 的 web_port（该目录已在 .gitignore 中）。"""
    workspace_cfg = _WORKSPACE / 'data' / 'config.yaml'
    if not workspace_cfg.is_file():
        return None
    try:
        import yaml
        with open(workspace_cfg, encoding='utf-8') as f:
            raw = yaml.safe_load(f) or {}
        port = int((raw.get('global') or {}).get('web_port') or 0)
        if 1024 <= port <= 65535:
            return port
    except Exception:
        pass
    return None


def _default_verify_base_url() -> str:
    """在线验算默认地址：环境变量 > 127.0.0.1，端口默认 8765。"""
    for key in ('QBUPLIMIT_VERIFY_URL', 'EMBY_VERIFY_URL', 'QBUPLIMIT_BASE_URL'):
        val = (os.environ.get(key) or '').strip().rstrip('/')
        if val:
            return val

    host = (os.environ.get('QBUPLIMIT_HOST') or '127.0.0.1').strip() or '127.0.0.1'

    port = _DEFAULT_WEB_PORT
    port_env = (os.environ.get('QBUPLIMIT_WEB_PORT') or '').strip()
    if port_env.isdigit():
        port = int(port_env)
    else:
        local_port = _web_port_from_local_config()
        if local_port:
            port = local_port

    port = max(1024, min(65535, int(port or _DEFAULT_WEB_PORT)))
    return f'http://{host}:{port}'


def _parse_router_mb(value: str) -> float:
    """解析路由读数：支持 149.08、0MB、149.08MB、1.2GB 等。"""
    text = str(value or '').strip().replace(',', '')
    if not text:
        raise argparse.ArgumentTypeError('路由读数不能为空')
    m = re.match(
        r'^([+-]?\d+(?:\.\d+)?)\s*(mb|m|gb|g|kb|k|b)?$',
        text,
        re.IGNORECASE,
    )
    if not m:
        raise argparse.ArgumentTypeError(
            f'无效路由读数 "{value}"，示例: 0、149.08、0MB、149.08MB',
        )
    num = float(m.group(1))
    unit = (m.group(2) or 'mb').casefold()
    if unit in ('gb', 'g'):
        num *= 1024.0
    elif unit in ('kb', 'k'):
        num /= 1024.0
    elif unit == 'b':
        num /= 1024.0 * 1024.0
    return num


def _fmt_bytes(n, *, signed: bool = False) -> str:
    val = int(n or 0)
    if not signed:
        val = max(0, val)
    sign = ''
    if signed and val < 0:
        sign = '-'
        val = abs(val)
    if val >= 1024 * 1024:
        return f'{sign}{val / 1024 / 1024:.2f} MB'
    if val >= 1024:
        return f'{sign}{val / 1024:.1f} KB'
    return f'{sign}{val} B'


def _norm_instance_key(name: str) -> str:
    return (name or '').strip().replace('_', '').casefold()


def _instances_from_config_path(path: str) -> list:
    try:
        import core.config_manager as config_manager
        cfg = config_manager.enrich_config(config_manager._read_config_file(path))
        return config_manager.get_emby_instances(cfg) or []
    except Exception:
        return []


def _default_instance_from_config() -> str:
    """CLI 优先读工作区 data/config.yaml，避免误用本机 /data 里的旧配置。"""
    paths = []
    workspace_cfg = _WORKSPACE / 'data' / 'config.yaml'
    if workspace_cfg.is_file():
        paths.append(str(workspace_cfg))
    try:
        import core.config_manager as config_manager
        if os.path.exists(config_manager.CONFIG_PATH):
            paths.append(config_manager.CONFIG_PATH)
    except Exception:
        pass
    for path in paths:
        instances = _instances_from_config_path(path)
        if not instances:
            continue
        for row in instances:
            if row.get('wan_traffic_only'):
                return str(row.get('name') or '').strip()
        if len(instances) == 1:
            return str(instances[0].get('name') or '').strip()
        return str(instances[0].get('name') or '').strip()
    return ''


def _resolve_credentials(username: str, password: str) -> tuple:
    user = (username or os.environ.get('QBUPLIMIT_WEB_USER') or '').strip()
    pwd = password or os.environ.get('QBUPLIMIT_WEB_PASSWORD') or ''
    return user, pwd


def _resolve_instance_name(requested: str, live_rows: list) -> tuple:
    """以 Web live 接口为准解析实例名；返回 (name, note)。"""
    rows = list(live_rows or [])
    if not rows:
        return (requested or '').strip(), ''

    names = {r.get('name') for r in rows if r.get('name')}
    by_container = {
        (r.get('container_name') or '').strip(): r.get('name')
        for r in rows
        if (r.get('container_name') or '').strip()
    }
    by_norm = {_norm_instance_key(n): n for n in names}
    for cname, iname in by_container.items():
        by_norm.setdefault(_norm_instance_key(cname), iname)

    req = (requested or '').strip()
    if req:
        if req in names:
            return req, ''
        if req in by_container:
            resolved = by_container[req]
            return resolved, f'已将 "{req}" 解析为实例名 "{resolved}"（容器名）'
        norm = _norm_instance_key(req)
        if norm in by_norm:
            resolved = by_norm[norm]
            if resolved != req:
                return resolved, f'已将 "{req}" 解析为实例名 "{resolved}"'
            return resolved, ''

    playing = [r for r in rows if int(r.get('session_count') or 0) > 0]
    if len(playing) == 1:
        name = playing[0].get('name') or ''
        return name, f'自动选用正在播放的实例: {name}'

    if len(rows) == 1:
        name = rows[0].get('name') or ''
        return name, f'自动选用唯一实例: {name}'

    cfg_name = _default_instance_from_config()
    if cfg_name:
        if cfg_name in names:
            return cfg_name, f'自动选用配置实例: {cfg_name}'
        norm = _norm_instance_key(cfg_name)
        if norm in by_norm:
            resolved = by_norm[norm]
            return resolved, f'自动选用配置实例: {resolved}'

    return '', '未能唯一确定实例，请传 --instance "实例名"'


def _wait_for_playback(client: _WebClient, instance: str, wait_seconds: float) -> bool:
    """reset 后等待外网播放出现，避免「脚本在跑但还没开始播」。"""
    if wait_seconds <= 0:
        return True
    deadline = time.time() + wait_seconds
    print(f'等待播放开始（最多 {wait_seconds:.0f}s）… 请现在启动 LAN/外网播放')
    while time.time() < deadline:
        rows = _list_live_instances(client)
        row = next((r for r in rows if r.get('name') == instance), None)
        if row and int(row.get('session_count') or 0) > 0:
            print(f'检测到播放: {instance}  会话数={row.get("session_count")}')
            return True
        time.sleep(1.0)
    print(f'超时: {wait_seconds:.0f}s 内未检测到 {instance} 的播放会话')
    return False


class _WebClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip('/')
        self._jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._jar),
        )

    def login(self, username: str, password: str) -> None:
        payload = {
            'username': username,
            'password': password,
            'remember': True,
        }
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            f'{self.base}/api/auth/login',
            data=data,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            },
            method='POST',
        )
        with self._opener.open(req, timeout=10) as resp:
            body = json.loads(resp.read().decode('utf-8'))
        if not body.get('success'):
            raise RuntimeError(body.get('error') or '登录失败')

    def fetch_json(self, url: str, *, method: str = 'GET', body: dict = None) -> dict:
        data = None
        headers = {'Accept': 'application/json'}
        if body is not None:
            data = json.dumps(body).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with self._opener.open(req, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))


def _fetch_json(url: str, *, method: str = 'GET', body: dict = None,
                client: Optional[_WebClient] = None) -> dict:
    if client is not None:
        return client.fetch_json(url, method=method, body=body)
    data = None
    headers = {'Accept': 'application/json'}
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _list_live_instances(client: _WebClient) -> list:
    try:
        payload = client.fetch_json(f'{client.base}/api/emby/status/live')
        rows = payload.get('data') or []
        return [
            {
                'name': row.get('name') or '',
                'container_name': row.get('container_name') or '',
                'is_online': bool(row.get('is_online')),
                'api_online': bool(row.get('api_online')),
                'session_count': int(row.get('session_count') or 0),
            }
            for row in rows
            if row.get('name')
        ]
    except Exception:
        return []


def _format_instance_hint(instances: list) -> str:
    if not instances:
        return '（未能读取实例列表，请登录 Web 查看 Emby 实例名）'
    parts = []
    for row in instances:
        name = row.get('name') or '?'
        container = row.get('container_name') or ''
        online = '在线' if row.get('api_online') else '离线'
        sessions = int(row.get('session_count') or 0)
        extra = f', 容器={container}' if container else ''
        parts.append(f'{name}({online}, 会话={sessions}{extra})')
    return '可用实例: ' + '; '.join(parts)


def run_offline():
    from tools.emby_traffic_verify.offline import run_all
    run_all()


def _print_tick_row(row: dict, *, router_wan_mb: float = None) -> None:
    name = row.get('instance_name') or '?'
    mode = row.get('mode_label') or row.get('mode_code') or '?'
    passed = row.get('tick_passed')
    status = 'PASS' if passed else 'FAIL'
    failed = int(row.get('tick_failed_count') or 0)
    inp = row.get('tick_inputs') or {}
    out = row.get('tick_outputs') or {}
    cum = row.get('cumulative') or {}
    print(
        f'[{status}] {name} | {mode} | '
        f'raw={_fmt_bytes(inp.get("live_raw_up"))} '
        f'wan_pool={_fmt_bytes(inp.get("live_delta_up_wan_pool"))} '
        f'assigned={_fmt_bytes(out.get("wan_assigned_tick"))} '
        f'live={_fmt_bytes(out.get("wan_session_live_total"))} '
        f'fail={failed}',
    )
    if cum:
        print(
            f'       累计 pool={_fmt_bytes(cum.get("wan_pool_bytes"))} '
            f'assigned={_fmt_bytes(cum.get("wan_assigned_bytes"))} '
            f'session_live={_fmt_bytes(cum.get("wan_session_live_total"))} '
            f'gap(assigned-live)={_fmt_bytes(cum.get("assigned_vs_session_gap"), signed=True)} '
            f'gap(pool-live)={_fmt_bytes(cum.get("pool_vs_session_gap"), signed=True)} '
            f'ticks={cum.get("ticks")} failed_ticks={cum.get("failed_ticks")}',
        )
    if router_wan_mb is not None and cum.get('wan_session_live_total') is not None:
        router_b = int(float(router_wan_mb) * 1024 * 1024)
        live = int(cum.get('wan_session_live_total') or 0)
        diff = live - router_b
        print(
            f'       路由WAN≈{_fmt_bytes(router_b)} '
            f'会话累计={_fmt_bytes(live)} 差={diff / 1024 / 1024:+.2f} MB',
        )
    for check in row.get('tick_failed_checks') or []:
        label = check.get('label') or check.get('id')
        got = check.get('got')
        expect = check.get('expect')
        detail = check.get('detail') or ''
        extra = f' got={got} expect={expect}' if got is not None else ''
        if detail:
            extra += f' ({detail})'
        print(f'       ! {label}{extra}')


def _print_auto_report(
    *,
    instance: str,
    last_row: dict,
    sample_total: int,
    fail_total: int,
    seconds: float,
    router_wan_mb: float = None,
    router_wan_delta_mb: float = None,
) -> None:
    cum = (last_row or {}).get('cumulative') or {}
    mode = last_row.get('mode_label') or last_row.get('mode_code') or '?'
    ticks = int(cum.get('ticks') or 0)
    failed_ticks = int(cum.get('failed_ticks') or 0)
    session_live = int(cum.get('wan_session_live_total') or 0)
    assigned_gap = int(cum.get('assigned_vs_session_gap') or 0)
    pool_gap = int(cum.get('pool_vs_session_gap') or 0)
    wan_pool = int(cum.get('wan_pool_bytes') or 0)

    print('\n' + '=' * 60)
    print('自动验算结论')
    print('=' * 60)
    print(f'实例: {instance or "?"}  模式: {mode}  观测: {seconds:.0f}s  采样: {sample_total}')

    if ticks <= 0:
        print('\n【未验算】未采集到 M2/M3 外网 tick。')
        print('  → 请先开始外网播放（M2 仅外网 / M3 局域网+外网），再重新运行 --auto。')
        return

    internal_ok = fail_total == 0 and failed_ticks == 0 and assigned_gap == 0
    print(f'\n【内部分摊】{"通过" if internal_ok else "未通过"}')
    print(f'  M2/M3 tick 数: {ticks}，失败 tick: {failed_ticks}')
    print(f'  外网会话累计: {_fmt_bytes(session_live)}')
    print(f'  分摊→会话 gap: {_fmt_bytes(assigned_gap, signed=True)}'
          + ('（分摊字节已全部进入会话）' if assigned_gap == 0 else '（有字节未入账）'))
    if pool_gap < 0:
        print(
            f'  WAN 池累计 {_fmt_bytes(wan_pool)} < 会话 {_fmt_bytes(session_live)} 属正常：'
            '切换期 backlog / M1 捕获等会在 M3 稳态灌入会话，不计入 wan_pool 累计。',
        )
    elif pool_gap > 1024:
        print(f'  WAN 池 vs 会话 gap: {_fmt_bytes(pool_gap, signed=True)}（池侧偏多，需关注）')

    if router_wan_delta_mb is not None:
        router_b = int(float(router_wan_delta_mb) * 1024 * 1024)
        diff = session_live - router_b
        pct = (diff / router_b * 100) if router_b > 0 else 0.0
        ok_router = abs(diff) <= max(2 * 1024 * 1024, router_b * 0.05)
        print(f'\n【对比路由 WAN 增量】{"接近" if ok_router else "有偏差"}')
        print(f'  路由 WAN 增量: {_fmt_bytes(router_b)}')
        print(f'  外网会话累计: {_fmt_bytes(session_live)}')
        print(f'  差值: {diff / 1024 / 1024:+.2f} MB ({pct:+.1f}%)')
        if not ok_router and internal_ok:
            print('  → 内部分摊无 bug；偏差来自 Docker 比例 vs 真实 WAN 口径（M3 常见）。')
    elif router_wan_mb is not None:
        router_b = int(float(router_wan_mb) * 1024 * 1024)
        diff = session_live - router_b
        print(f'\n【对比路由 WAN 绝对读数】')
        print(f'  路由 WAN≈{_fmt_bytes(router_b)}  会话累计={_fmt_bytes(session_live)}'
              f'  差={diff / 1024 / 1024:+.2f} MB')
        print('  提示: 更准请用 --router-wan-start-mb / --router-wan-end-mb 算播放期间增量。')
    else:
        print('\n【对比路由】未提供路由读数。')
        print('  播放前: python -m tools.emby_traffic_verify --auto ... --router-wan-start-mb <读数>')
        print('  播放后: 同上并加 --router-wan-end-mb <读数>')

    print('=' * 60)


def run_online(*, base_url: str, instance: str, seconds: float, interval: float,
               router_wan_mb: float = None, router_wan_start_mb: float = None,
               router_wan_end_mb: float = None, reset: bool = False,
               username: str = '', password: str = '', quiet: bool = False,
               auto_report: bool = False, client: Optional[_WebClient] = None,
               wait_playback: float = 0) -> int:
    import urllib.parse

    base = base_url.rstrip('/')
    own_client = client is None
    if client is None:
        client = _WebClient(base)
        if username:
            try:
                client.login(username, password)
                print(f'已登录 Web: {username} @ {base}')
            except urllib.error.HTTPError as e:
                print(f'登录失败: HTTP {e.code} {e.reason}')
                return 2
            except Exception as e:
                print(f'登录失败: {e}')
                return 2
    else:
        base = client.base

    live_instances = _list_live_instances(client) if username or not own_client else []
    if live_instances:
        resolved, note = _resolve_instance_name(instance, live_instances)
        if note:
            print(note)
        if resolved:
            instance = resolved
        elif instance:
            print(_format_instance_hint(live_instances))
            return 2

    verify_url = f'{base}/api/emby/traffic-verify'
    if instance:
        verify_url += f'?instance={urllib.parse.quote(instance)}'

    if reset and instance:
        try:
            _fetch_json(
                f'{base}/api/emby/traffic-verify/reset',
                method='POST',
                body={'instance': instance},
                client=client if username else None,
            )
            print(f'已重置 {instance} 在线验算累计')
        except urllib.error.HTTPError as e:
            if e.code == 401:
                print('重置失败: 未登录（请加 -u / -p）')
                return 2
            print(f'重置失败: HTTP {e.code} {e.reason}')
        except Exception as e:
            print(f'重置失败: {e}')

    if wait_playback > 0 and instance:
        if not _wait_for_playback(client, instance, wait_playback):
            print('将继续计时，但可能无 M2/M3 tick；建议重新运行并在一分钟内开始播放')

    router_wan_delta_mb = None
    if router_wan_start_mb is not None and router_wan_end_mb is not None:
        router_wan_delta_mb = float(router_wan_end_mb) - float(router_wan_start_mb)

    if not quiet:
        print(f'在线验算: {verify_url}  时长={seconds}s  间隔={interval}s')
    elif auto_report:
        print(f'▶ 开始计时 {seconds:.0f}s  实例={instance or "全部"}  （静默，仅 tick 变化时输出）')
    deadline = time.time() + max(1.0, float(seconds))
    last_cum_ticks = -1
    fail_total = 0
    sample_total = 0
    empty_hint_shown = False
    last_row = None

    while time.time() < deadline:
        sample_total += 1
        try:
            payload = _fetch_json(
                verify_url,
                client=client if username else None,
            )
            rows = payload.get('data') or []
            if not rows:
                if not empty_hint_shown:
                    empty_hint_shown = True
                    if not username:
                        print('(无实例数据；Web API 需登录，请加 -u / -p)')
                    elif instance:
                        print(
                            f'(无实例 "{instance}" 的验算数据；'
                            '请确认实例名正确且 Emby 调度已在运行)',
                        )
                    else:
                        print('(无验算数据；Emby 调度是否已产生 tick?)')
                    if live_instances:
                        print(_format_instance_hint(live_instances))
                    elif username:
                        live_instances = _list_live_instances(client)
                        if live_instances:
                            print(_format_instance_hint(live_instances))
            for row in rows:
                last_row = row
                if not row.get('tick_passed'):
                    fail_total += 1
                cum = row.get('cumulative') or {}
                tick_changed = int(cum.get('ticks') or 0) != last_cum_ticks
                show_row = (not quiet) or (not row.get('tick_passed')) or tick_changed
                if show_row:
                    _print_tick_row(row, router_wan_mb=router_wan_mb)
                if tick_changed:
                    last_cum_ticks = int(cum.get('ticks') or 0)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                print('请求失败: 未登录（请加 -u / -p）')
            else:
                print(f'请求失败: HTTP {e.code} {e.reason}')
            return 2
        except urllib.error.URLError as e:
            print(f'请求失败: {e}  (服务是否在 {base} 运行?)')
            return 2
        except Exception as e:
            print(f'解析失败: {e}')
            return 2
        time.sleep(max(0.5, float(interval)))

    if auto_report:
        _print_auto_report(
            instance=instance,
            last_row=last_row or {},
            sample_total=sample_total,
            fail_total=fail_total,
            seconds=seconds,
            router_wan_mb=router_wan_mb,
            router_wan_delta_mb=router_wan_delta_mb,
        )
    elif not quiet:
        print(
            f'\n在线验算结束: 采样={sample_total} tick失败样本={fail_total} '
            f'{"(有失败项，见上方)" if fail_total else "(全部通过)"}',
        )
    return 1 if fail_total else 0


def run_auto(*, base_url: str, instance: str, seconds: float, interval: float,
             username: str, password: str,
             router_wan_mb: float = None, router_wan_start_mb: float = None,
             router_wan_end_mb: float = None, wait_playback: float = 90) -> int:
    user, pwd = _resolve_credentials(username, password)
    if not user or not pwd:
        print('自动验算需要 Web 登录：请传 -u / -p，或设置环境变量 QBUPLIMIT_WEB_USER / QBUPLIMIT_WEB_PASSWORD')
        return 2

    base = base_url.rstrip('/')
    client = _WebClient(base)
    try:
        client.login(user, pwd)
    except urllib.error.HTTPError as e:
        print(f'登录失败: HTTP {e.code} {e.reason}')
        return 2
    except Exception as e:
        print(f'登录失败: {e}')
        return 2

    live_rows = _list_live_instances(client)
    resolved, note = _resolve_instance_name(instance.strip(), live_rows)
    if not resolved:
        print(note or '未能确定 Emby 实例名')
        if live_rows:
            print(_format_instance_hint(live_rows))
        return 2
    instance = resolved

    print(f'已登录: {user} @ {base}')
    if note:
        print(note)
    print(f'目标实例: {instance}')
    print('操作顺序: ① 手抄路由起点 → ② 运行本脚本 → ③ 看到「等待播放」后立即播放 → ④ 保持 {0:.0f}s → ⑤ 手抄路由终点'.format(seconds))
    if router_wan_start_mb is not None and router_wan_end_mb is None:
        print(f'路由 WAN 起点: {router_wan_start_mb:.2f} MB（结束后用 --router-wan-end-mb 填入终点）')

    return run_online(
        base_url=base_url,
        instance=instance,
        seconds=seconds,
        interval=interval,
        router_wan_mb=router_wan_mb,
        router_wan_start_mb=router_wan_start_mb,
        router_wan_end_mb=router_wan_end_mb,
        reset=True,
        username=user,
        password=pwd,
        quiet=True,
        auto_report=True,
        client=client,
        wait_playback=wait_playback,
    )


def main():
    parser = argparse.ArgumentParser(description='Emby 外网流量分摊验算')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--offline', action='store_true', help='离线模拟（默认）')
    mode.add_argument('--online', action='store_true', help='在线轮询 API')
    mode.add_argument('--auto', action='store_true',
                        help='一键自动验算（自动实例+重置+静默+中文结论）')
    parser.add_argument('--url', default=None,
                        help='Web 服务根地址（默认 http://127.0.0.1:8765 或环境变量 QBUPLIMIT_VERIFY_URL）')
    parser.add_argument('--instance', default='', help='Emby 实例名')
    parser.add_argument('--seconds', type=float, default=60.0, help='在线轮询时长')
    parser.add_argument('--interval', type=float, default=2.0, help='轮询间隔秒')
    parser.add_argument('--router-wan-mb', type=_parse_router_mb, default=None,
                        help='路由侧 WAN 累计 MB（结束时绝对读数，可写 149.08MB）')
    parser.add_argument('--router-wan-start-mb', type=_parse_router_mb, default=None,
                        help='播放开始前路由 WAN 读数（可写 0MB 或纯数字）')
    parser.add_argument('--router-wan-end-mb', type=_parse_router_mb, default=None,
                        help='播放结束后路由 WAN 读数 MB')
    parser.add_argument('--quiet', action='store_true', help='静默轮询，仅 tick 变化或失败时输出')
    parser.add_argument('--wait-playback', type=float, default=None,
                        help='reset 后等待播放出现的秒数（--auto 默认 90，0=不等待）')
    parser.add_argument('--reset', action='store_true', help='开始前重置累计验算')
    parser.add_argument('-u', '--username', default='', help='Web 登录账号')
    parser.add_argument('-p', '--password', default='', help='Web 登录密码')
    args = parser.parse_args()

    base_url = (args.url or '').strip() or _default_verify_base_url()
    user, pwd = _resolve_credentials(args.username.strip(), args.password)

    wait_playback = 0.0 if args.wait_playback is None else max(0.0, float(args.wait_playback))

    if args.auto:
        auto_wait = 90.0 if args.wait_playback is None else wait_playback
        code = run_auto(
            base_url=base_url,
            instance=args.instance.strip(),
            seconds=args.seconds if args.seconds != 60.0 else 120.0,
            interval=args.interval,
            username=user,
            password=pwd,
            router_wan_mb=args.router_wan_mb,
            router_wan_start_mb=args.router_wan_start_mb,
            router_wan_end_mb=args.router_wan_end_mb,
            wait_playback=auto_wait,
        )
        raise SystemExit(code)

    if args.online:
        code = run_online(
            base_url=base_url,
            instance=args.instance.strip(),
            seconds=args.seconds,
            interval=args.interval,
            router_wan_mb=args.router_wan_mb,
            router_wan_start_mb=args.router_wan_start_mb,
            router_wan_end_mb=args.router_wan_end_mb,
            reset=args.reset,
            username=user,
            password=pwd,
            quiet=args.quiet,
            auto_report=args.quiet,
            wait_playback=wait_playback,
        )
        raise SystemExit(code)

    run_offline()


if __name__ == '__main__':
    main()
