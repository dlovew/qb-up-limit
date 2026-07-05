"""系统日志展示层中文化（不影响原始字段，供分页游标使用）。"""

import re
from typing import Dict

_LEVEL_ZH = {
    'DEBUG': '调试',
    'INFO': '信息',
    'WARNING': '警告',
    'ERROR': '错误',
    'CRITICAL': '严重',
}

_LOGGER_ZH = {
    '__main__': '主程序',
    'main': '主程序',
    'scheduler': '调度器',
    'speed_limiter': '限速器',
    'qb_monitor': 'qB 监控',
    'config_manager': '配置管理',
    'traffic_db': '流量数据库',
    'emby_scheduler': 'Emby 调度器',
    'emby_traffic_db': 'Emby 流量数据库',
    'emby_client': 'Emby 客户端',
    'emby_playback_traffic': 'Emby 播放流量',
    'emby_lucky': 'Lucky 客户端',
    'emby_user_sync': 'Emby 用户同步',
    'emby_continuous_playback': 'Emby 连播',
    'emby_traffic_filter': 'Emby 流量过滤',
    'emby_playback_upload_sync': 'Emby 播放上行同步',
    'emby_lucky_verdict': 'Lucky 裁决',
    'playback_record_store': '播放记录',
    'playback_upload_repair': '播放上行修复',
    'browse_upload_settler': '选片结算',
    'secrets_store': '密钥存储',
    'user_prefs_store': '用户偏好',
    'web.server': 'Web 服务',
    'web.auth': 'Web 认证',
    'urllib3': '网络库',
    'urllib3.connectionpool': '网络连接池',
    'werkzeug': 'Web 框架',
}

_LOGGER_PREFIX_ZH = (
    ('urllib3', '网络库'),
    ('werkzeug', 'Web 框架'),
)

_SETTLE_REASON_ZH = {
    'account_superseded': '账号被取代',
    'user_switch': '用户切换',
    'playback_started': '开始播放',
    'disconnect': '断开连接',
    'browse_conn_end': '选片连接结束',
    'orphan_bucket': '孤儿桶',
    'timeout_offline': '超时离线',
    'instance_reset': '实例重置',
    'emby_confirmed_stop': 'Emby 确认停止',
    'emby_abnormal_disconnect': 'Emby侧异常断开',
    'grace_expired': '宽限期结束',
    'item_change': '切换条目',
}

_MESSAGE_RULES = [
    (re.compile(r'\[Playback:'), '[播放:'),
    (re.compile(r'\[Browse:'), '[选片:'),
    (re.compile(r'\brid='), '记录='),
    (re.compile(r'\bsid='), '会话='),
    (re.compile(r'\bbytes='), '字节='),
    (re.compile(r'\bkeys='), '键='),
    (re.compile(r'\bgrace='), '宽限='),
    (re.compile(r'\bkey='), '键='),
    (re.compile(r'\bSIGTERM\b'), '终止信号'),
    (re.compile(r'\bSIGINT\b'), '中断信号'),
    (re.compile(r'\bSIGHUP\b'), '挂起信号'),
    (re.compile(r'incremental_vacuum'), '增量整理'),
    (re.compile(r'保留 WAN'), '保留外网'),
    (re.compile(r'\bWAN\b'), '外网'),
    (re.compile(r'KB/s'), 'KB/秒'),
    (re.compile(r'config\.yaml'), '配置文件'),
    (re.compile(r'登录响应非 Ok\.'), '登录响应非常规'),
    (re.compile(r'无需认证 qB='), '无需认证 qB 版本='),
    (re.compile(r'Asia/Shanghai'), '亚洲/上海'),
    (re.compile(r'API /(\S+) 错误'), r'接口 /\1 错误'),
]

_URLLIB3_RETRY_RE = re.compile(
    r"Retrying \(Retry\((.+?)\)\) after connection broken by '(.+)': (.+)$"
)


def _localize_logger(logger: str) -> str:
    if logger in _LOGGER_ZH:
        return _LOGGER_ZH[logger]
    for prefix, label in _LOGGER_PREFIX_ZH:
        if logger == prefix or logger.startswith(f'{prefix}.'):
            return label
    return logger


def _localize_network_errors(text: str) -> str:
    text = re.sub(
        r"HTTPSConnectionPool\(host='([^']+)', port=(\d+)\)",
        r'HTTPS 连接池（主机 \1，端口 \2）',
        text,
    )
    text = re.sub(
        r"HTTPConnectionPool\(host='([^']+)', port=(\d+)\)",
        r'HTTP 连接池（主机 \1，端口 \2）',
        text,
    )
    text = re.sub(
        r'Read timed out\. \(read timeout=([\d.]+)\)',
        r'读取超时（读超时 \1 秒）',
        text,
    )
    text = re.sub(
        r'Connect timed out\.? \(connect timeout=([\d.]+)\)',
        r'连接超时（连接超时 \1 秒）',
        text,
    )
    replacements = (
        ('ReadTimeoutError', '读取超时'),
        ('ConnectTimeoutError', '连接超时'),
        ('NewConnectionError', '新建连接失败'),
        ('ConnectionError', '连接错误'),
        ('Read timed out', '读取超时'),
        ('Connect timed out', '连接超时'),
        ('Connection refused', '连接被拒绝'),
        ('Connection reset by peer', '连接被对方重置'),
        ('Name or service not known', '无法解析主机名'),
        ('Max retries exceeded', '超过最大重试次数'),
        ('after connection broken by', '连接中断于'),
        ('Caused by', '原因'),
    )
    for src, dst in replacements:
        text = text.replace(src, dst)
    text = re.sub(r'读取超时\("', '读取超时（', text)
    text = re.sub(r'连接超时\("', '连接超时（', text)
    text = re.sub(r'"\)', '）', text)
    text = re.sub(r'\btotal=', '总次数=', text)
    text = re.sub(r'\bconnect=', '连接=', text)
    text = re.sub(r'\bread=', '读取=', text)
    text = re.sub(r'\bredirect=', '重定向=', text)
    text = re.sub(r'\bstatus=', '状态=', text)
    text = re.sub(r'\bNone\b', '无', text)
    return text


def _localize_urllib3_message(message: str) -> str:
    text = str(message or '')
    match = _URLLIB3_RETRY_RE.match(text)
    if match:
        retry_cfg, err, path = match.groups()
        return (
            f'正在重试（{_localize_network_errors(retry_cfg)}），'
            f'连接中断：{_localize_network_errors(err)}，请求：{path}'
        )
    if 'ConnectionPool' in text or 'TimeoutError' in text or 'retries exceeded' in text:
        return _localize_network_errors(text)
    return text


def _localize_message(message: str) -> str:
    text = str(message or '')
    if not text:
        return ''
    text = _localize_urllib3_message(text)
    for reason, label in _SETTLE_REASON_ZH.items():
        text = text.replace(f'reason={reason}', f'原因={label}')
    text = re.sub(r'\breason=', '原因=', text)
    text = re.sub(r'\buser=', '用户=', text)
    for pattern, repl in _MESSAGE_RULES:
        text = pattern.sub(repl, text)
    return text


def localize_system_log_entry(entry: Dict) -> Dict:
    """为日志条目附加展示用中文字段，保留原始 level/logger/message。"""
    result = dict(entry)
    level = str(entry.get('level') or '')
    logger = str(entry.get('logger') or '').strip()
    message = str(entry.get('message') or '')
    result['level_display'] = _LEVEL_ZH.get(level, level)
    result['logger_display'] = _localize_logger(logger)
    result['message_display'] = _localize_message(message)
    return result
