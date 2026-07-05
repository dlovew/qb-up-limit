"""Emby 外网流量分摊 — 离线验算场景。"""

from __future__ import annotations

import sys
from pathlib import Path

_APP = Path(__file__).resolve().parent.parent.parent
if str(_APP) not in sys.path:
    sys.path.insert(0, str(_APP))

import emby.traffic.playback as emby_playback_traffic
from emby.traffic.filter import (
    allocate_wan_deltas,
    resolve_transcode_kind,
    session_container_egress_bps,
    session_stream_bps,
)


# RFC 5737 / RFC 1918 文档用地址，仅用于离线模拟
_OFFLINE_WAN_ENDPOINT = '8.8.8.8:12345'
_OFFLINE_LAN_ENDPOINT = '10.0.0.50:12345'


def _session(*, remote: bool, transcode: bool = True, bps: int = 8_000_000,
             sid: str = 's1', video_bps: int = None, audio_bps: int = None,
             play_method: str = None, transcode_kind: str = None,
             is_video_direct: bool = None, is_audio_direct: bool = None) -> dict:
    ep = _OFFLINE_WAN_ENDPOINT if remote else _OFFLINE_LAN_ENDPOINT
    if play_method is None:
        play_method = 'Transcode' if transcode else 'DirectPlay'
    if transcode_kind is None:
        if play_method == 'DirectPlay':
            transcode_kind = 'direct_play'
        elif play_method == 'DirectStream':
            transcode_kind = 'direct_stream'
        elif transcode:
            transcode_kind = 'full_transcode'
        else:
            transcode_kind = 'direct_play'
    vb = video_bps if video_bps is not None else bps // 2
    ab = audio_bps if audio_bps is not None else bps // 2
    row = {
        'is_playing': True,
        'is_paused': False,
        'is_remote': remote,
        'remote_endpoint': ep,
        'play_method': play_method,
        'transcode_kind': transcode_kind,
        'video_bitrate': vb,
        'audio_bitrate': ab,
        'emby_session_id': sid,
        'user_name': 'u',
        'client': 'c',
        'item_id': f'item-{sid}',
    }
    if is_video_direct is not None:
        row['is_video_direct'] = is_video_direct
    if is_audio_direct is not None:
        row['is_audio_direct'] = is_audio_direct
    return row


def _tick_wan_assigned(raw_up: int, sessions: list, *, backlog: int = 0) -> dict:
    wan_up, _ = allocate_wan_deltas(raw_up, 0, sessions)
    effective = wan_up + max(0, backlog)
    return {
        'raw_up': raw_up,
        'wan_pool': wan_up,
        'effective': effective,
        'wan_assigned': effective,
        'remainder': max(0, raw_up - effective),
    }


def _assert_close(label: str, got: int, expect: int, tol: int = 1) -> None:
    ok = abs(got - expect) <= tol
    status = 'OK' if ok else 'FAIL'
    print(f'  [{status}] {label}: got={got} expect={expect} (tol={tol})')
    if not ok:
        raise AssertionError(f'{label}: {got} != {expect}')


def run_all() -> None:
    print('Emby 外网流量分摊 — 离线验算')

    print('\n=== M1 仅局域网: WAN池应为 0 ===')
    s = [_session(remote=False, sid='lan1')]
    wan_up, _ = allocate_wan_deltas(1024 * 1024, 0, s)
    _assert_close('WAN池', wan_up, 0)

    print('\n=== M2 稳态单外网转码 8Mbps, 1s tick, 1MB raw ===')
    s = [_session(remote=True, sid='wan1')]
    r = _tick_wan_assigned(1024 * 1024, s)
    _assert_close('WAN池', r['wan_pool'], 1024 * 1024)
    _assert_close('WAN分摊', r['wan_assigned'], 1024 * 1024)
    _assert_close('余量', r['remainder'], 0)

    print('\n=== M3 双转码同码率 50/50, 1s 1MB raw ===')
    sessions = [_session(remote=False, sid='lan1'), _session(remote=True, sid='wan1')]
    r = _tick_wan_assigned(1024 * 1024, sessions)
    _assert_close('WAN池(50%)', r['wan_pool'], 512 * 1024, tol=1024)
    _assert_close('WAN分摊', r['wan_assigned'], r['wan_pool'], tol=1024)

    print('\n=== M3 LAN 仅音频转码 + 外网全转码（分量码率自动配比）===')
    lan_audio = _session(
        remote=False, sid='lan-a',
        play_method='Transcode', transcode_kind='audio_transcode',
        is_video_direct=True, is_audio_direct=False,
        video_bps=6_000_000, audio_bps=192_000,
    )
    wan_full = _session(
        remote=True, sid='wan-f',
        play_method='Transcode', transcode_kind='full_transcode',
        is_video_direct=False, is_audio_direct=False,
        video_bps=4_000_000, audio_bps=128_000,
    )
    mix = [lan_audio, wan_full]
    raw = 1024 * 1024
    pool, _ = allocate_wan_deltas(raw, 0, mix)
    lan_e = session_container_egress_bps(lan_audio)
    wan_e = session_container_egress_bps(wan_full)
    expect = int(raw * wan_e / (lan_e + wan_e))
    print(f'  LAN egress={lan_e} WAN egress={wan_e} ratio={wan_e/(lan_e+wan_e):.3f}')
    _assert_close('WAN池(分量比)', pool, expect, tol=1024)

    print('\n=== M3 LAN 直串流 + 外网视频转码 ===')
    lan_ds = _session(
        remote=False, sid='lds', transcode=False,
        play_method='DirectStream', transcode_kind='direct_stream',
        video_bps=8_000_000, audio_bps=384_000,
    )
    wan_vt = _session(
        remote=True, sid='wvt',
        play_method='Transcode', transcode_kind='video_transcode',
        is_video_direct=False, is_audio_direct=True,
        video_bps=3_000_000, audio_bps=128_000,
    )
    mix2 = [lan_ds, wan_vt]
    pool2, _ = allocate_wan_deltas(raw, 0, mix2)
    expect2 = int(raw * session_container_egress_bps(wan_vt)
                  / (session_container_egress_bps(lan_ds) + session_container_egress_bps(wan_vt)))
    _assert_close('DirectStream+video_transcode WAN池', pool2, expect2, tol=1024)

    print('\n=== M3 外网新会话突发 2MB WAN池 ===')
    emby_playback_traffic.clear_instance_live_upload_state('sim')
    r = _tick_wan_assigned(2 * 1024 * 1024, sessions)
    _assert_close('WAN分摊=池', r['wan_assigned'], r['wan_pool'], tol=2048)

    print('\n=== 权重: DirectPlay / DirectStream / 双转码 ===')
    lan_direct = _session(remote=False, transcode=False, sid='ld')
    lan_stream = _session(
        remote=False, transcode=False, sid='ls',
        play_method='DirectStream', transcode_kind='direct_stream',
    )
    lan_tx = _session(remote=False, transcode=True, sid='lt')
    wan = _session(remote=True, sid='w')
    raw10 = 10 * 1024 * 1024
    mix_direct, _ = allocate_wan_deltas(raw10, 0, [lan_direct, wan])
    mix_stream, _ = allocate_wan_deltas(raw10, 0, [lan_stream, wan])
    mix_tx, _ = allocate_wan_deltas(raw10, 0, [lan_tx, wan])
    print(f'  DirectPlay混合 WAN池={mix_direct} ({mix_direct/raw10:.1%})')
    print(f'  DirectStream混合 WAN池={mix_stream} ({mix_stream/raw10:.1%})')
    print(f'  双转码混合 WAN池={mix_tx} ({mix_tx/raw10:.1%})')

    print('\n=== transcode_kind 解析 ===')
    for label, sess in (
        ('audio_transcode', lan_audio),
        ('video_transcode', wan_vt),
        ('direct_stream', lan_ds),
    ):
        got = resolve_transcode_kind(sess)
        print(f'  {label}: {got}')

    print('\n=== filter 码率权重一致性 ===')
    filter_bps = sum(session_container_egress_bps(s) for s in sessions)
    stream_bps = sum(session_stream_bps(s) for s in sessions)
    print(f'  egress={filter_bps} stream={stream_bps} 一致={filter_bps == stream_bps}')

    print('\n全部离线验算通过。')
