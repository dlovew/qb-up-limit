# qb-up-limit

qBittorrent **达量自动限速**管理工具，带 Web 仪表盘。支持多实例、按周期（月/周/自定义）统计上行流量，达到阈值后自动调整上传限速；可选集成 **Emby**，统计外网播放相关上行流量。

当前版本：`v1.61`（见 `app/web/static/js/version.js`）

## 功能概览

- **多 qB 实例管理**：统一监控多台 qBittorrent 的上传/下载流量与在线状态
- **达量限速规则**：按周期累计上行 GB，触发多级限速（如 500GB → 128KB/s，800GB → 512KB/s）
- **周期重置**：支持按月/按周等周期自动重置统计与限速策略
- **Web 管理界面**：设备卡片、流量图表、规则配置、连通性测试、手动解除限速
- **凭据安全存储**：qB 密码、Emby API Key、Web 登录密码与配置文件分离加密保存
- **Emby 集成（可选）**：播放会话记录、外网上行估算、与 qB 设备视图合并展示
- **Docker 部署**：单容器运行，数据目录持久化

## 快速开始（Docker Compose）

### 1. 准备目录与配置

```bash
git clone https://github.com/luowenfu/qb-up-limit.git
cd qb-up-limit
mkdir -p data config
cp config/config.yaml.example config/config.yaml
# 编辑 config/config.yaml，填入你的 qB 地址与限速规则
```

### 2. 启动

```bash
cp docker-compose.yml.example docker-compose.yml
docker compose up -d --build
```

### 3. 访问

浏览器打开：`http://<主机IP>:8765`

**默认 Web 账号**（首次启动自动生成，请及时修改）：

| 用户名 | 密码 |
|--------|------|
| `admin` | `adminadmin` |

> 密码保存在数据卷 `data/.web_auth`，可在 Web 界面「全局设置」中修改。

## 手动 Docker 运行

```bash
docker build -t qb-up-limit .
docker run -d \
  --name qb-up-limit \
  --restart unless-stopped \
  -p 8765:8765 \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/config/config.yaml:/config/config.yaml:ro" \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  qb-up-limit
```

- `/data`：运行时配置、SQLite 数据库、加密密钥、日志（**务必备份**）
- `/config/config.yaml`：可选，仅**首次**无 `/data/config.yaml` 时导入
- Docker Socket：仅 Emby 容器流量统计需要；不需要 Emby 时可去掉该挂载

## 配置说明

主配置文件路径（容器内）：`/data/config.yaml`

关键字段：

```yaml
global:
  web_port: 8765
  refresh_interval: 1      # 界面刷新间隔（秒）
  emby_enabled: false      # 是否启用 Emby 模块

qbittorrent_instances:
  - name: 我的qB
    host: 192.168.1.100    # 支持 host:port 合并写法
    port: 8080
    speed_rules:
      - cycle_upload_limit_gb: 500
        speed_limit_kbps: 128
    cycle:
      type: month
      reset_anchor: 1      # 每月 1 日重置
```

完整示例见 [`config/config.yaml.example`](config/config.yaml.example)。

qB 密码、Emby API Key 通过 Web 界面保存，写入 `data/.qb_secrets`、`data/.emby_secrets`，**不会**明文写入 `config.yaml`。

## 本地开发

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

mkdir -p data
cp config/config.yaml.example data/config.yaml
cd app && python main.py
```

> 本地运行时配置路径为 `/data/config.yaml`；Windows 下需自行映射或修改 `config_manager.CONFIG_PATH` 做调试。

## 测试数据

`test_data/` 提供模拟流量生成脚本，用于界面测试，不影响生产库：

```bash
python test_data/generate_test_traffic.py
```

详见 [`test_data/README.md`](test_data/README.md)。

## 项目结构

```
app/                  # Python 后端与 Web 静态资源
  main.py             # 入口
  scheduler.py        # qB 流量采集与限速调度
  emby_scheduler.py   # Emby 会话与流量（可选）
  web/                # Flask API 与前端页面
config/               # 种子配置示例
data/                 # 运行时数据（git 忽略，勿提交）
test_data/            # 测试脚本与模拟数据库
Dockerfile
requirements.txt
```

## 安全提示（开源 / 部署前必读）

以下内容**切勿**提交到公开仓库或分享给他人：

- `data/config.yaml`（真实主机地址）
- `data/.qb_secrets`、`data/.emby_secrets`（API 密钥与密码）
- `data/.web_auth`、`data/.web_secret`、`data/.data_key`
- `data/*.db`、`data/app.log*`、`data/emby_events/`

首次 `git push` 前请执行：

```bash
git status   # 确认 data/ 未被跟踪
```

## 文档

- [Emby 自写 JSON 播放记录方案说明](自写Emby_json日志+获取信息的方案方法总结.md)（实现细节，供维护参考）

## 许可证

[MIT](LICENSE)

## 致谢

- [qbittorrent-api](https://github.com/rmartin16/qbittorrent-api)
- [Flask](https://flask.palletsprojects.com/)
