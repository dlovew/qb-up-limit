import os
import sys
import logging
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

file_handler = RotatingFileHandler(
    '/data/app.log', encoding='utf-8',
    maxBytes=10 * 1024 * 1024, backupCount=3
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logging.getLogger().addHandler(file_handler)

logging.getLogger('urllib3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import traffic_db
import emby_traffic_db
from scheduler import TrafficMonitor
from emby_scheduler import EmbyMonitor
from web.server import init_web_server, run_web_server
import config_manager


def main():
    logger.info("=" * 60)
    logger.info("qB-达量限速管理 qb-up-limit 启动中...")
    logger.info("=" * 60)

    config = config_manager.ensure_config()
    runtime_config = config_manager.enrich_config(config)

    global_cfg = config_manager.get_global_config(config)
    traffic_db.set_retention_years(global_cfg.get('data_retention_years', 5))
    traffic_db.init_db()
    emby_traffic_db.init_db()
    logger.info("数据库初始化完成")

    monitor = TrafficMonitor(runtime_config, config_path=config_manager.CONFIG_PATH)
    monitor.start()

    emby_monitor = EmbyMonitor(runtime_config, config_path=config_manager.CONFIG_PATH)
    emby_monitor.start()

    init_web_server(monitor, emby_monitor)

    web_port = config.get('global', {}).get('web_port', 8765)
    logger.info(f"Web 管理界面启动: http://0.0.0.0:{web_port}")
    logger.info(f"配置文件路径: {config_manager.CONFIG_PATH}")
    run_web_server(port=web_port)


if __name__ == '__main__':
    main()
